#!/usr/bin/env python3
"""Exercise the actual HTTP proxy with an ephemeral in-memory backend."""
import concurrent.futures
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent


def main():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen([
            str(ROOT / 'target/release/append-proxy'), '--memory',
            '--listen', f'127.0.0.1:{port}'
        ], stdout=log, stderr=log)
        base = f'http://127.0.0.1:{port}/v1/objects/'
        def request(method, key='demo/log', data=None, headers=None):
            req = urllib.request.Request(base + key, data=data, method=method, headers=headers or {})
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.status, dict(response.headers), response.read()
            except urllib.error.HTTPError as response:
                return response.code, dict(response.headers), response.read()
        try:
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError('proxy exited during startup')
                try:
                    request('HEAD')
                    break
                except urllib.error.URLError:
                    time.sleep(0.05)
            else:
                raise RuntimeError('proxy startup timeout')
            first = request('POST', data=b'hello ', headers={'Idempotency-Key': 'hello', 'Append-Offset': '0'})
            assert first[0] == 201, first
            second = request('POST', data=b'world\n', headers={'Idempotency-Key': 'world', 'Append-Offset': '6'})
            assert second[0] == 201, second
            retry = request('POST', data=b'hello ', headers={'Idempotency-Key': 'hello', 'Append-Offset': '0'})
            assert retry[0] == 200, retry
            original = json.loads(first[2])
            repeated = json.loads(retry[2])
            assert original['receipt'] == repeated['receipt'], (original, repeated)
            assert request('GET')[2] == b'hello world\n'
            ranged = request('GET', headers={'Range': 'bytes=4-8'})
            assert ranged[0] == 206 and ranged[2] == b'o wor', ranged
            conflict = request('POST', data=b'different', headers={'Idempotency-Key': 'hello'})
            assert conflict[0] == 409, conflict
            def append(i):
                payload = f'{i:04d}\n'.encode()
                result = request('POST', 'concurrent', payload, {'Idempotency-Key': f'record-{i}'})
                assert result[0] == 201, result
                return payload
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                expected = list(pool.map(append, range(32)))
            actual = request('GET', 'concurrent')[2].splitlines(keepends=True)
            assert sorted(actual) == sorted(expected), 'lost or duplicated concurrent append'
            print(json.dumps({
                'outcome':'PASS', 'backend':'in-memory storage through actual HTTP server',
                'checks':['append offsets','original receipt on retry','cross-segment range',
                          'idempotency conflict','32 concurrent exactly-once appends'],
                'content':'hello world\\n', 'concurrent_records':len(actual)
            }, separators=(',', ':')))
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if process.returncode not in (0, -15):
                log.seek(0)
                print(log.read(8192).decode(errors='replace'))


if __name__ == '__main__':
    main()
