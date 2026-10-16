"""Read-only re-evaluation of a saved lifecycle JUnit; never starts tests."""
import argparse
import json
from pathlib import Path

from linux_phase4_lifecycle import read_junit


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('junit',type=Path)
    parser.add_argument('--backend',choices=('buffered','direct'),required=True)
    parser.add_argument('--expected-cases',type=int)
    args=parser.parse_args()
    result=read_junit(args.junit,backend=args.backend)
    result.update(junit=str(args.junit),backend=args.backend,recheck_only=True)
    ok=(not result['failed'] and not result['unexpected_skips'] and
        (args.expected_cases is None or result['cases']==args.expected_cases))
    result['status']='passed' if ok else 'failed'
    print(json.dumps(result,indent=2))
    return 0 if ok else 1


if __name__=='__main__':raise SystemExit(main())
