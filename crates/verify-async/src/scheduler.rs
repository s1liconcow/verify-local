use std::{
    collections::BTreeMap,
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use tokio::runtime::{Builder, Runtime};

struct Ready(AtomicBool);

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct Task<T> {
    future: Pin<Box<dyn Future<Output = T>>>,
    ready: Arc<Ready>,
}

/// Drives top-level futures by task ID on a single thread with paused Tokio time.
/// Put external effects behind adapter-controlled futures that wake these tasks.
/// Ordinary Tokio spawning, real I/O, and external wakers are outside this scheduler.
pub struct Scheduler<T> {
    tasks: BTreeMap<u64, Task<T>>,
    runtime: Runtime,
}

impl<T> Scheduler<T> {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            tasks: BTreeMap::new(),
            runtime: Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()?,
        })
    }

    pub fn start(&mut self, id: u64, future: impl Future<Output = T> + 'static) {
        assert!(!self.tasks.contains_key(&id), "task ID already in use");
        self.tasks.insert(
            id,
            Task {
                future: Box::pin(future),
                ready: Arc::new(Ready(AtomicBool::new(true))),
            },
        );
    }

    pub fn runnable(&self) -> impl Iterator<Item = u64> + '_ {
        self.tasks
            .iter()
            .filter(|(_, task)| task.ready.0.load(Ordering::Relaxed))
            .map(|(&id, _)| id)
    }

    pub fn poll(&mut self, id: u64) -> Poll<T> {
        let task = self.tasks.get_mut(&id).expect("unknown task ID");
        assert!(
            task.ready.0.swap(false, Ordering::Relaxed),
            "task is asleep"
        );
        let waker = Waker::from(task.ready.clone());
        let _entered = self.runtime.enter();
        let result = task.future.as_mut().poll(&mut Context::from_waker(&waker));
        if result.is_ready() {
            self.tasks.remove(&id);
        }
        self.check_no_spawn();
        result
    }

    pub fn cancel(&mut self, id: u64) -> bool {
        let _entered = self.runtime.enter();
        let removed = self.tasks.remove(&id).is_some();
        self.check_no_spawn();
        removed
    }

    pub fn advance(&mut self, duration: Duration) {
        self.check_no_spawn();
        // `advance` yields once to drive the timer wheel. The main future stays
        // runnable, so the clock cannot also auto-advance to a later deadline.
        self.runtime.block_on(tokio::time::advance(duration));
    }

    fn check_no_spawn(&self) {
        assert_eq!(
            self.runtime.metrics().num_alive_tasks(),
            0,
            "spawned tasks must be registered with the verification scheduler"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{sync::oneshot, time::sleep};

    #[test]
    fn polling_and_time_are_independent_choices() {
        let mut scheduler = Scheduler::new().unwrap();
        let (send, receive) = oneshot::channel();
        scheduler.start(9, async move { receive.await.unwrap() });
        scheduler.start(4, async {
            sleep(Duration::from_millis(10)).await;
            4
        });
        assert_eq!(scheduler.runnable().collect::<Vec<_>>(), [4, 9]);
        assert!(scheduler.poll(9).is_pending());
        assert!(scheduler.poll(4).is_pending());
        scheduler.advance(Duration::from_millis(5));
        assert_eq!(scheduler.runnable().count(), 0);
        send.send(9).unwrap();
        assert_eq!(scheduler.runnable().collect::<Vec<_>>(), [9]);
        scheduler.advance(Duration::from_millis(5));
        assert_eq!(scheduler.runnable().collect::<Vec<_>>(), [4, 9]);
        assert_eq!(scheduler.poll(9), Poll::Ready(9));
        assert_eq!(scheduler.poll(4), Poll::Ready(4));
    }

    #[test]
    fn cancellation_drops_waiting_future() {
        let mut scheduler = Scheduler::new().unwrap();
        let (send, receive) = oneshot::channel::<()>();
        scheduler.start(0, receive);
        assert!(scheduler.poll(0).is_pending());
        assert!(scheduler.cancel(0));
        assert!(send.send(()).is_err());
        assert_eq!(scheduler.runnable().count(), 0);
    }
}
