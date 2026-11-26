use crate::{model::Manifest, Error, Limits, Result};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
    time::{timeout_at, Duration, Instant},
};
use verify_protocol::{canonical_json, parse_json, validate_response, Request, Response};

pub struct Session {
    child: Child,
    pid: u32,
    stdin: ChildStdin,
    receiver: mpsc::Receiver<Message>,
    readers: Vec<JoinHandle<()>>,
    _temporary: tempfile::TempDir,
    limits: Limits,
    deadline: Instant,
    closed: bool,
}
enum Message {
    Response(Vec<u8>),
    Error(Error),
    Eof,
}
impl Session {
    pub fn start(manifest: &Manifest, limits: &Limits, deadline: Instant) -> Result<Self> {
        #[cfg(not(unix))]
        return Err(Error::harness(
            "subprocess backend currently requires POSIX process groups",
        ));
        let temporary = tempfile::tempdir()?;
        let mut command = Command::new(&manifest.argv[0]);
        command
            .args(&manifest.argv[1..])
            .current_dir(&manifest.root)
            .env_clear()
            .envs(&manifest.environment)
            .env("TMPDIR", temporary.path())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }
        let mut child = command
            .spawn()
            .map_err(|e| Error::harness(format!("adapter spawn failed: {e}")))?;
        let pid = child
            .id()
            .ok_or_else(|| Error::harness("adapter PID unavailable"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::harness("adapter stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::harness("adapter stdout unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::harness("adapter stderr unavailable"))?;
        let (tx, receiver) = mpsc::channel(2);
        let consumed = Arc::new(AtomicUsize::new(0));
        let readers = vec![
            tokio::spawn(read_stdout(
                stdout,
                tx.clone(),
                consumed.clone(),
                limits.max_response_bytes,
                limits.max_output_bytes,
            )),
            tokio::spawn(read_stderr(stderr, tx, consumed, limits.max_output_bytes)),
        ];
        Ok(Self {
            child,
            pid,
            stdin,
            receiver,
            readers,
            _temporary: temporary,
            limits: limits.clone(),
            deadline,
            closed: false,
        })
    }
    pub async fn request(&mut self, request: &Request) -> Result<(Response, Value)> {
        if Instant::now() >= self.deadline {
            return Err(Error::inconclusive("campaign wall-clock budget exhausted"));
        }
        let mut bytes = canonical_json(request)?;
        bytes.push(b'\n');
        if bytes.len() > self.limits.max_response_bytes {
            return Err(Error::harness("request byte limit exceeded"));
        }
        let deadline = self
            .deadline
            .min(Instant::now() + Duration::from_secs_f64(self.limits.timeout));
        let operation = async {
            self.stdin
                .write_all(&bytes)
                .await
                .map_err(|_| Error::harness("adapter closed stdin"))?;
            self.stdin.flush().await?;
            let message = self
                .receiver
                .recv()
                .await
                .ok_or_else(|| Error::harness("adapter closed output without a response"))?;
            let bytes = match message {
                Message::Response(bytes) => bytes,
                Message::Error(error) => return Err(error),
                Message::Eof => return Err(Error::harness("adapter exited or closed stdout")),
            };
            let raw = parse_json(&bytes)?;
            let response: Response = serde_json::from_value(raw.clone())?;
            validate_response(&response, request.request_id)?;
            Ok((response, raw))
        };
        match timeout_at(deadline, operation).await {
            Ok(result) => result,
            Err(_) => Err(Error::inconclusive(if Instant::now() >= self.deadline {
                "campaign wall-clock budget exhausted"
            } else {
                "adapter request timeout; no virtual liveness failure inferred"
            })),
        }
    }
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        kill_group(self.pid, false);
        if tokio::time::timeout(Duration::from_millis(100), self.child.wait())
            .await
            .is_err()
        {
            kill_group(self.pid, true);
            let _ = tokio::time::timeout(Duration::from_secs(1), self.child.wait()).await;
        }
        // A leader may exit while descendants still hold pipes.
        kill_group(self.pid, true);
        // Drain the pipe tail before accepting a completed trace. This catches extra
        // responses and stderr/output overflow even after the last requested action.
        let drain = async {
            while let Some(message) = self.receiver.recv().await {
                match message {
                    Message::Response(_) => {
                        return Err(Error::harness("adapter emitted an unsolicited response"));
                    }
                    Message::Error(error) => return Err(error),
                    Message::Eof => {}
                }
            }
            Ok(())
        };
        let result = tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .unwrap_or_else(|_| Err(Error::inconclusive("adapter output pipes did not close")));
        for reader in &self.readers {
            reader.abort();
        }
        self.closed = true;
        result
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        if !self.closed {
            kill_group(self.pid, true);
        }
        for reader in &self.readers {
            reader.abort();
        }
    }
}
fn kill_group(pid: u32, force: bool) {
    #[cfg(unix)]
    unsafe {
        // The child owns a fresh process group with pgid==pid. No user-selected PID.
        libc::kill(
            -(pid as i32),
            if force { libc::SIGKILL } else { libc::SIGTERM },
        );
    }
}
async fn read_stdout<R: AsyncRead + Unpin>(
    mut reader: R,
    tx: mpsc::Sender<Message>,
    total: Arc<AtomicUsize>,
    line_limit: usize,
    output_limit: usize,
) {
    let mut pending = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let n = match reader.read(&mut buffer).await {
            Ok(0) => {
                let message = if pending.is_empty() {
                    Message::Eof
                } else {
                    Message::Error(Error::harness("adapter emitted unterminated JSONL output"))
                };
                let _ = tx.send(message).await;
                return;
            }
            Ok(n) => n,
            Err(e) => {
                let _ = tx
                    .send(Message::Error(Error::harness(format!(
                        "adapter stdout read failed: {e}"
                    ))))
                    .await;
                return;
            }
        };
        if total.fetch_add(n, Ordering::Relaxed).saturating_add(n) > output_limit {
            let _ = tx
                .send(Message::Error(Error::inconclusive(
                    "adapter output budget exhausted",
                )))
                .await;
            return;
        }
        for &byte in &buffer[..n] {
            if byte == b'\n' {
                if tx
                    .send(Message::Response(std::mem::take(&mut pending)))
                    .await
                    .is_err()
                {
                    return;
                }
            } else {
                pending.push(byte);
                if pending.len() >= line_limit {
                    let _ = tx
                        .send(Message::Error(Error::harness(
                            "adapter response byte limit exceeded",
                        )))
                        .await;
                    return;
                }
            }
        }
    }
}
async fn read_stderr<R: AsyncRead + Unpin>(
    mut reader: R,
    tx: mpsc::Sender<Message>,
    total: Arc<AtomicUsize>,
    output_limit: usize,
) {
    let mut buffer = [0u8; 8192];
    loop {
        let n = match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if total.fetch_add(n, Ordering::Relaxed).saturating_add(n) > output_limit {
            let _ = tx
                .send(Message::Error(Error::inconclusive(
                    "adapter output budget exhausted",
                )))
                .await;
            return;
        }
    }
}
