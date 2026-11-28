#!/usr/bin/env python3
"""Verify the language adapters and a crash/retry scenario against an HTTP/SQLite service."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--skip-build', action='store_true')
    parser.add_argument('--out', type=Path, help='New evidence directory (default: unique .verify/demo-* directory)')
    args = parser.parse_args()
    if not args.skip_build:
        subprocess.run(['cargo', 'build', '--release', '--workspace', '--locked'], cwd=ROOT, check=True)
    if args.out:
        out = args.out.resolve()
        out.mkdir(parents=True, exist_ok=False)
    else:
        (ROOT / '.verify').mkdir(exist_ok=True)
        out = Path(tempfile.mkdtemp(prefix='demo-', dir=ROOT / '.verify'))
    executable = str(ROOT / 'target/release/verify-local')
    results = {}

    def invoke(label, arguments, expected, code=0):
        completed = subprocess.run([executable, *map(str, arguments)], cwd=ROOT,
                                   capture_output=True, text=True, timeout=150)
        if len(completed.stdout) > 12 * 1024:
            raise AssertionError(f'{label}: exceeded compact-output budget')
        response = json.loads(completed.stdout)
        assert completed.returncode == code and response['outcome'] == expected, {
            'label':label, 'exit':completed.returncode, 'response':response,
            'stderr':completed.stderr[-2048:]
        }
        results[label] = response
        print(json.dumps({'stage':label, **response}, separators=(',', ':')), flush=True)
        return response

    for language in ['python', 'javascript']:
        invoke(f'{language}-doctor', ['doctor', f'examples/{language}/verify.json'], 'READY')
        invoke(language, ['run', f'examples/{language}/verify.json', '--out', out / language,
            '--runs', 100, '--steps', 80, '--seconds', 60, '--seed', 1], 'NO_FAILURE_WITHIN_BOUNDS')
    fixed = 'examples/sqlite-http/verify.json'
    mutant = 'examples/sqlite-http/verify-mutant.json'
    bounds = ['--runs', 8, '--steps', 12, '--seconds', 60, '--guided']
    # One execution cannot witness both a pre-commit kill and a post-commit kill.
    first = invoke('sqlite-seed', ['run', fixed, '--guided', '--steps', 12, '--runs', 1,
                   '--out', out / 'seed'], 'INCONCLUSIVE', 2)
    assert first['replays'] == 1 and first['guidance']['pending'] > 0
    baseline = invoke('sqlite-baseline', ['run', fixed, *bounds, '--out', out / 'baseline',
                      '--corpus-from', first['artifacts']['frontier']], 'NO_FAILURE_WITHIN_BOUNDS')
    assert baseline['replays'] == 8 and baseline['guidance']['branches_replayed'] > 0
    bug = invoke('sqlite-mutant', ['run', mutant, *bounds, '--out', out / 'mutant'], 'FOUND', 1)
    assert bug['failure']['property'] == 'ledger.once'
    failure = bug['artifacts']['failure']
    invoke('sqlite-replay', ['replay', mutant, failure], 'FOUND', 1)
    reduced = out / 'reduced.json'
    invoke('sqlite-shrink', ['shrink', mutant, failure, '--out', reduced], 'FOUND', 1)
    invoke('sqlite-regression', ['regression', fixed, reduced], 'NO_FAILURE_WITHIN_BOUNDS')

    # Reconstruct the same committed transaction and let the HTTP reply through.
    trace = json.loads(reduced.read_text())
    event = next(i for i, action in enumerate(trace['actions']) if action['id'] == 'kill')
    alternate = out / 'reply-instead.json'
    fork = invoke('sqlite-fork', ['fork', mutant, reduced, '--at', event, '--action', 'reply',
                  '--steps', 4, '--out', alternate], 'INCONCLUSIVE', 2)
    assert fork['replay_match'] and fork['stop'] == 'terminal' and fork['failure'] is None
    assert fork['missing_witnesses']['count'] == 2
    final = json.loads(alternate.read_text())['responses'][-1]
    assert final['observation']['balance']['count'] == 1
    assert any(c['id'] == 'ledger.once' and c['ok'] for c in final['checks'])
    (out / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps({'outcome':'PASS', 'summary':str(out / 'summary.json')}, separators=(',', ':')))


if __name__ == '__main__':
    main()
