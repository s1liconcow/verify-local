#!/usr/bin/env python3
"""Run the language-neutral Python and JavaScript examples with checked verdicts."""
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
    (out / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps({'outcome':'PASS', 'summary':str(out / 'summary.json')}, separators=(',', ':')))


if __name__ == '__main__':
    main()
