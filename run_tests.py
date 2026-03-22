#!/usr/bin/env python
"""
run_tests.py  –  Pipeline test runner
======================================
Runs the full pytest suite, captures results, and prints a summary.

Usage
-----
    python run_tests.py              # all tests
    python run_tests.py --unit       # unit tests only
    python run_tests.py --integration # integration tests only
    python run_tests.py --dq         # data quality tests only
    python run_tests.py --coverage   # with coverage report
    python run_tests.py -v           # verbose (pass extra flags)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


# ────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline test runner")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--unit",        action="store_true", help="Unit tests only")
    group.add_argument("--integration", action="store_true", help="Integration tests only")
    group.add_argument("--dq",          action="store_true", help="Data quality tests only")
    p.add_argument("--coverage", action="store_true", help="Generate coverage report (requires pytest-cov)")
    p.add_argument("-v", "--verbose",   action="store_true", help="Verbose pytest output")
    p.add_argument("-x", "--exitfirst", action="store_true", help="Stop after first failure")
    p.add_argument("extra", nargs="*", help="Extra arguments passed to pytest")
    return p.parse_args()


# ────────────────────────────────────────────────────────────────────────────
# Build pytest command
# ────────────────────────────────────────────────────────────────────────────

def build_cmd(args: argparse.Namespace) -> list[str]:
    cmd = [sys.executable, "-m", "pytest"]

    # Target selection
    if args.unit:
        cmd += ["tests/test_unit_utils.py",
                "tests/test_unit_jobreq.py",
                "tests/test_unit_hire.py",
                "tests/test_unit_app.py",
                "tests/test_unit_referral.py",
                "tests/test_unit_erp.py"]
    elif args.integration:
        cmd += ["tests/test_integration.py"]
    elif args.dq:
        cmd += ["tests/test_data_quality.py"]
    else:
        cmd += ["tests/"]

    # Flags
    if args.verbose:
        cmd.append("-v")
    if args.exitfirst:
        cmd.append("-x")
    if args.coverage:
        cmd += [
            "--cov=src",
            "--cov-report=term-missing",
            "--cov-report=html:htmlcov",
        ]

    # Always show short test summary
    cmd += ["-r", "N"]  # show only failures + errors in summary

    # Extra args
    cmd += args.extra
    return cmd


# ────────────────────────────────────────────────────────────────────────────
# Print helpers
# ────────────────────────────────────────────────────────────────────────────

def _banner(text: str, width: int = 60) -> str:
    return f"\n{'═' * width}\n  {text}\n{'═' * width}"


def _section(text: str) -> str:
    return f"\n{'─' * 50}\n  {text}\n{'─' * 50}"


# ────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_args()
    project_root = Path(__file__).parent
    cmd = build_cmd(args)

    # ── Pre-flight checks ────────────────────────────────────────────────
    print(_banner("Pipeline Test Suite"))

    required_dirs = ["src", "tests"]
    for d in required_dirs:
        if not (project_root / d).exists():
            print(f"  ERROR: Required directory '{d}' not found.")
            return 1

    # ── Run tests ────────────────────────────────────────────────────────
    suite = (
        "unit"        if args.unit        else
        "integration" if args.integration else
        "data quality" if args.dq        else
        "all"
    )
    print(f"  Suite   : {suite}")
    print(f"  Command : {' '.join(cmd)}\n")

    start = time.monotonic()
    result = subprocess.run(cmd, cwd=str(project_root))
    elapsed = time.monotonic() - start

    # ── Summary ──────────────────────────────────────────────────────────
    print(_section("Test Run Summary"))
    status = "PASSED" if result.returncode == 0 else "FAILED"
    icon   = "✓" if result.returncode == 0 else "✗"
    print(f"  {icon}  Result  : {status}")
    print(f"     Duration : {elapsed:.1f}s")

    if args.coverage:
        print(f"     Coverage : see htmlcov/index.html")

    if result.returncode != 0:
        print("\n  Tip: run with --verbose / -v for full failure details")

    print()
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
