"""Explicit Linux liburing build/setup probe; importing this module is inert."""
import argparse
import json
import os
import platform
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', action='store_true',
                        help='explicitly build the adapter and initialize one io_uring')
    args = parser.parse_args(argv)
    report = {
        'platform': sys.platform,
        'kernel_release': platform.release(),
        'python': platform.python_version(),
        'native_requested': os.environ.get('DDQ_NATIVE_URING') == '1',
        'status': 'not-requested',
    }
    if not args.probe or not report['native_requested']:
        report['reason'] = 'pass --probe and set DDQ_NATIVE_URING=1 to initialize native io_uring'
        print(json.dumps(report, sort_keys=True))
        return 2
    if not sys.platform.startswith('linux'):
        report.update(status='unavailable', reason='io_uring requires Linux')
        print(json.dumps(report, sort_keys=True))
        return 2
    from svsim.uring_backend import NativeUringEngine
    try:
        engine = NativeUringEngine(max_requests=1)
    except NotImplementedError as exc:
        report.update(status='unavailable', reason=str(exc))
        print(json.dumps(report, sort_keys=True))
        return 2
    try:
        report.update(status='available', resolved_engine=engine.identity,
                      capacity=engine.capacity, metadata_bytes=engine.metadata_bytes,
                      native_overhead_bytes=engine.native_overhead_bytes,
                      adapter_build=engine.shim.build_metadata)
        engine.close()
    except BaseException as exc:
        report.update(status='probe-failed', reason=f'{type(exc).__name__}: {exc}')
        return_code = 1
    else:
        return_code = 0
    print(json.dumps(report, sort_keys=True))
    return return_code


if __name__ == '__main__':
    raise SystemExit(main())
