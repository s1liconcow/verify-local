#!/usr/bin/env node
/** Dependency-free JSONL v1 example. Durability is modeled in memory. */
import { createHash } from 'node:crypto';
import { createInterface } from 'node:readline';

const OPERATIONS = { A: 1, B: 3 };
const MAX_LINE = 256 * 1024;

function sorted(value) {
  if (Array.isArray(value)) return value.map(sorted);
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, sorted(value[key])]));
  }
  return value;
}
function canonical(value) { return JSON.stringify(sorted(value)); }

class Counter {
  constructor() {
    this.value = 0;
    this.receipts = {};
    this.pending = null;
    this.online = true;
    this.effectHistory = [];
    this.acknowledged = new Set();
    this.lostReplies = new Set();
    this.restarted = false;
    this.witnesses = new Set();
  }
  enabled() {
    if (!this.online) return [{ id: 'restart', kind: 'fault' }];
    if (this.pending !== null) {
      const advance = this.pending.phase === 'received' ? 'checkpoint' : 'reply';
      return [{ id: advance, kind: 'schedule' }, { id: 'crash', kind: 'fault' }];
    }
    return Object.keys(OPERATIONS).sort().map(operation =>
      ({ id: 'deliver:' + operation, kind: 'workload', operation }))
      .concat([{ id: 'crash', kind: 'fault' }]);
  }
  step(action) {
    if (!this.enabled().some(candidate => canonical(candidate) === canonical(action))) {
      throw new Error('action is not currently enabled');
    }
    if (action.id === 'crash') {
      if (this.pending && this.pending.phase === 'committed') {
        this.lostReplies.add(this.pending.operation);
        this.witnesses.add('crash_after_checkpoint');
      }
      this.online = false;
      this.pending = null;
    } else if (action.id === 'restart') {
      this.online = true;
      this.restarted = true;
      this.witnesses.add('restarted');
    } else if (action.id.startsWith('deliver:')) {
      if (this.restarted && this.lostReplies.has(action.operation)) {
        this.witnesses.add('retry_after_restart');
      }
      this.pending = { operation: action.operation, phase: 'received' };
    } else if (action.id === 'checkpoint') {
      const operation = this.pending.operation;
      if (Object.hasOwn(this.receipts, operation)) {
        this.witnesses.add('deduplicated_retry');
      } else {
        // A single modeled durable transaction updates value and receipt.
        this.value += OPERATIONS[operation];
        this.receipts[operation] = OPERATIONS[operation];
        this.effectHistory.push({ operation, delta: OPERATIONS[operation] });
      }
      this.pending.phase = 'committed';
    } else if (action.id === 'reply') {
      this.acknowledged.add(this.pending.operation);
      this.witnesses.add('acknowledged');
      this.pending = null;
    }
  }
  response(requestId) {
    const state = {
      value: this.value, receipts: this.receipts, pending: this.pending, online: this.online,
      effect_history: this.effectHistory, acknowledged: [...this.acknowledged].sort(),
      lost_replies: [...this.lostReplies].sort(), restarted: this.restarted,
      witnesses: [...this.witnesses].sort(),
    };
    const counts = {};
    for (const event of this.effectHistory) counts[event.operation] = (counts[event.operation] ?? 0) + 1;
    const expected = Object.keys(counts).reduce((total, operation) => total + OPERATIONS[operation], 0);
    const checks = [];
    if (this.effectHistory.length) {
      checks.push(
        { id: 'counter.effect_once', ok: Object.values(counts).every(count => count <= 1),
          signature: 'duplicate_committed_increment' },
        { id: 'counter.matches_history', ok: this.value === expected,
          signature: 'counter_disagrees_with_independent_history' },
      );
    }
    if (this.acknowledged.size) {
      checks.push({ id: 'counter.acknowledged_durable',
        ok: [...this.acknowledged].every(operation => Object.hasOwn(this.receipts, operation) && counts[operation] === 1),
        signature: 'acknowledged_increment_lost' });
    }
    return {
      protocol: 1, request_id: requestId,
      state: createHash('sha256').update(canonical(state)).digest('hex'),
      enabled: this.enabled(), checks, witnesses: [...this.witnesses].sort(),
      terminal: false, observation: state,
    };
  }
}

async function main() {
  let counter = null;
  let nextId = 0;
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of lines) {
    if (Buffer.byteLength(line) + 1 > MAX_LINE) throw new Error('request exceeds 256 KiB');
    const request = JSON.parse(line);
    if (request.protocol !== 1 || !Number.isSafeInteger(request.request_id) || request.request_id !== nextId) {
      throw new Error('protocol/request_id mismatch');
    }
    if (request.op === 'reset' && counter === null && nextId === 0) counter = new Counter();
    else if (request.op === 'step' && counter !== null) counter.step(request.action);
    else throw new Error('expected initial reset or subsequent step');
    const output = canonical(counter.response(nextId)) + '\n';
    if (!process.stdout.write(output)) await new Promise(resolve => process.stdout.once('drain', resolve));
    nextId += 1;
  }
}
main().catch(error => { process.stderr.write('adapter error: ' + error.message + '\n'); process.exitCode = 2; });
