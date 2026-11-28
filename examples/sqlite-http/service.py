"""A small HTTP ledger with optional transaction checkpoints for verification."""
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import sqlite3
import sys


class Ledger:
    def __init__(self, path, volatile_receipts, checkpoint):
        self.db = sqlite3.connect(path)
        self.checkpoint = checkpoint
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS credits(amount INTEGER NOT NULL)")
        storage = "TEMP" if volatile_receipts else ""
        self.db.execute(f"CREATE {storage} TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY)")

    def credit(self, key, amount):
        with self.db:
            inserted = self.db.execute("INSERT OR IGNORE INTO receipts VALUES (?)", (key,))
            if inserted.rowcount == 1:
                self.db.execute("INSERT INTO credits VALUES (?)", (amount,))
            self.checkpoint("before_commit")
        self.checkpoint("after_commit")
        return {"id": key, "amount": amount}

    def balance(self):
        count, total = self.db.execute("SELECT count(*), coalesce(sum(amount), 0) FROM credits").fetchone()
        return {"count": count, "total": total}


class Handler(BaseHTTPRequestHandler):
    def respond(self, body):
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        if self.path != "/credit":
            return self.send_error(404)
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.respond(self.server.ledger.credit(payload["id"], payload["amount"]))

    def do_GET(self):
        if self.path != "/balance":
            return self.send_error(404)
        self.respond(self.server.ledger.balance())

    def log_message(self, *_args):
        pass


def checkpoint(name):
    print(name, flush=True)
    if sys.stdin.readline() != "continue\n":
        raise RuntimeError("checkpoint controller disconnected")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("database")
    parser.add_argument("--checkpoints", action="store_true")
    parser.add_argument("--volatile-receipts", action="store_true")
    args = parser.parse_args()
    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        server.ledger = Ledger(args.database, args.volatile_receipts,
                               checkpoint if args.checkpoints else lambda _: None)
        print(server.server_port, flush=True)
        server.serve_forever()
