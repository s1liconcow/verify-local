"""Drive the ledger as a client and keep fault control on separate process pipes."""
import hashlib
from http.client import HTTPConnection, RemoteDisconnected
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

from oracle import checks


class Experiment:
    def __init__(self, seed, directory, service_flags):
        self.database = str(Path(directory) / "ledger.db")
        self.service_flags = service_flags
        self.operation = {"id": f"credit-{seed}", "amount": 1 + seed % 97}
        self.phase = "ready"
        self.killed_at = None
        self.receipt = None
        self.balance = None
        self.witnesses = []
        self.client = None
        self.start_service()

    def start_service(self):
        self.service = subprocess.Popen(
            [sys.executable, "service.py", self.database, "--checkpoints", *self.service_flags],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.port = int(self.service.stdout.readline())

    def stop_service(self):
        self.service.kill()
        self.service.wait()
        self.service.stdin.close()
        self.service.stdout.close()

    def barrier(self, expected):
        actual = self.service.stdout.readline().strip()
        if actual != expected:
            raise RuntimeError(f"expected {expected} at service checkpoint, got {actual}")

    def release(self):
        self.service.stdin.write("continue\n")
        self.service.stdin.flush()

    def read_balance(self):
        client = HTTPConnection("127.0.0.1", self.port)
        try:
            client.request("GET", "/balance")
            return read_json(client)
        finally:
            client.close()

    def actions(self):
        actions = {
            "ready": [("send", "workload")],
            "retry": [("retry", "workload")],
            "before_commit": [("commit", "schedule")],
            "after_commit": [("reply", "schedule")],
            "down": [("restart", "schedule")],
            "acknowledged": [("audit", "workload")],
            "done": [],
        }[self.phase]
        if self.phase in ("before_commit", "after_commit") and self.killed_at is None:
            actions = [*actions, ("kill", "fault")]
        return [{"id": name, "kind": kind} for name, kind in actions]

    def step(self, action):
        if action in ("send", "retry"):
            self.client = HTTPConnection("127.0.0.1", self.port)
            self.client.request("POST", "/credit", json.dumps(self.operation),
                                {"Content-Type": "application/json"})
            self.barrier("before_commit")
            self.phase = "before_commit"
        elif action == "commit":
            self.release()
            self.barrier("after_commit")
            self.phase = "after_commit"
        elif action == "kill":
            self.killed_at = self.phase
            self.stop_service()
            try:
                self.client.getresponse()
            except (RemoteDisconnected, ConnectionResetError):
                self.witnesses.append("disconnect_" + self.phase)
            else:
                raise RuntimeError("received HTTP reply after fault at blocked checkpoint")
            self.client.close()
            self.phase = "down"
        elif action == "restart":
            self.start_service()
            self.balance = self.read_balance()
            self.phase = "retry"
        elif action == "reply":
            self.release()
            self.receipt = read_json(self.client)
            self.client.close()
            self.phase = "acknowledged"
        elif action == "audit":
            self.balance = self.read_balance()
            self.phase = "done"

    def response(self, request_id):
        observation = {"phase": self.phase, "killed_at": self.killed_at, "operation": self.operation,
                       "receipt": self.receipt, "balance": self.balance, "sqlite": sqlite3.sqlite_version}
        # Equivalent control points share search feedback across workload seeds.
        state = [self.phase, self.killed_at, self.balance and self.balance["count"]]
        return {
            "protocol": 1, "request_id": request_id,
            "state": hashlib.sha256(json.dumps(state).encode()).hexdigest(),
            "enabled": self.actions(), "terminal": self.phase == "done",
            "observation": observation, "witnesses": self.witnesses,
            "checks": checks(self.phase, self.operation, self.receipt, self.balance, self.killed_at),
        }


def read_json(client):
    response = client.getresponse()
    if response.status != 200:
        raise RuntimeError(f"HTTP service returned {response.status}")
    return json.loads(response.read())


def main():
    with tempfile.TemporaryDirectory() as directory:
        experiment = None
        try:
            for line in sys.stdin:
                request = json.loads(line)
                if request["op"] == "reset":
                    experiment = Experiment(request["seed"], directory, sys.argv[1:])
                else:
                    experiment.step(request["action"]["id"])
                print(json.dumps(experiment.response(request["request_id"])), flush=True)
        finally:
            if experiment and experiment.service.poll() is None:
                experiment.stop_service()


if __name__ == "__main__":
    main()
