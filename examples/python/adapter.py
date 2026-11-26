#!/usr/bin/env python3
"""Dependency-free JSONL v1 example: a synthetic durable idempotent counter.

Durability is modeled in memory. This demonstrates language interoperability;
it supplies no evidence about a real database or production application.
"""
import collections
import hashlib
import json
import sys

OPERATIONS = {"A": 1, "B": 3}
MAX_LINE = 256 * 1024


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class Counter:
    def __init__(self):
        self.value = 0
        self.receipts = {}
        self.pending = None
        self.online = True
        self.effect_history = []
        self.acknowledged = set()
        self.lost_replies = set()
        self.restarted = False
        self.witnesses = set()

    def enabled(self):
        if not self.online:
            return [{"id": "restart", "kind": "fault"}]
        if self.pending is not None:
            advance = "checkpoint" if self.pending["phase"] == "received" else "reply"
            return [{"id": advance, "kind": "schedule"}, {"id": "crash", "kind": "fault"}]
        return [{"id": "deliver:" + op, "kind": "workload", "operation": op}
                for op in sorted(OPERATIONS)] + [{"id": "crash", "kind": "fault"}]

    def step(self, action):
        if action not in self.enabled():
            raise ValueError("action is not currently enabled")
        action_id = action["id"]
        if action_id == "crash":
            if self.pending and self.pending["phase"] == "committed":
                self.lost_replies.add(self.pending["operation"])
                self.witnesses.add("crash_after_checkpoint")
            self.online = False
            self.pending = None
        elif action_id == "restart":
            self.online = True
            self.restarted = True
            self.witnesses.add("restarted")
        elif action_id.startswith("deliver:"):
            operation = action["operation"]
            if self.restarted and operation in self.lost_replies:
                self.witnesses.add("retry_after_restart")
            self.pending = {"operation": operation, "phase": "received"}
        elif action_id == "checkpoint":
            operation = self.pending["operation"]
            if operation in self.receipts:
                self.witnesses.add("deduplicated_retry")
            else:
                # The modeled durable transaction publishes both effects.
                self.value += OPERATIONS[operation]
                self.receipts[operation] = OPERATIONS[operation]
                # Independent test history observes this committed effect.
                self.effect_history.append({"operation": operation, "delta": OPERATIONS[operation]})
            self.pending["phase"] = "committed"
        elif action_id == "reply":
            self.acknowledged.add(self.pending["operation"])
            self.witnesses.add("acknowledged")
            self.pending = None

    def response(self, request_id):
        state = {
            "value": self.value,
            "receipts": self.receipts,
            "pending": self.pending,
            "online": self.online,
            "effect_history": self.effect_history,
            "acknowledged": sorted(self.acknowledged),
            "lost_replies": sorted(self.lost_replies),
            "restarted": self.restarted,
            "witnesses": sorted(self.witnesses),
        }
        counts = collections.Counter(event["operation"] for event in self.effect_history)
        expected = sum(OPERATIONS[operation] for operation in counts)
        checks = []
        if self.effect_history:
            checks.extend([
                {"id": "counter.effect_once", "ok": all(count <= 1 for count in counts.values()),
                 "signature": "duplicate_committed_increment"},
                {"id": "counter.matches_history", "ok": self.value == expected,
                 "signature": "counter_disagrees_with_independent_history"},
            ])
        if self.acknowledged:
            checks.append({"id": "counter.acknowledged_durable",
                           "ok": all(operation in self.receipts and counts[operation] == 1
                                     for operation in self.acknowledged),
                           "signature": "acknowledged_increment_lost"})
        return {
            "protocol": 1, "request_id": request_id,
            "state": hashlib.sha256(canonical(state).encode()).hexdigest(),
            "enabled": self.enabled(), "checks": checks,
            "witnesses": sorted(self.witnesses), "terminal": False,
            "observation": state,
        }


def main():
    counter = None
    next_id = 0
    while True:
        raw = sys.stdin.buffer.readline(MAX_LINE + 1)
        if not raw:
            break
        if len(raw) > MAX_LINE:
            raise ValueError("request exceeds 256 KiB")
        request = json.loads(raw)
        if (type(request.get("protocol")) is not int or request["protocol"] != 1 or
                type(request.get("request_id")) is not int or request["request_id"] != next_id):
            raise ValueError("protocol/request_id mismatch")
        if request["op"] == "reset" and counter is None and next_id == 0:
            counter = Counter()
        elif request["op"] == "step" and counter is not None:
            counter.step(request["action"])
        else:
            raise ValueError("expected initial reset or subsequent step")
        print(canonical(counter.response(next_id)), flush=True)
        next_id += 1


if __name__ == "__main__":
    try:
        main()
    except (ValueError, TypeError, KeyError) as error:
        print("adapter error: " + str(error), file=sys.stderr)
        raise SystemExit(2)
