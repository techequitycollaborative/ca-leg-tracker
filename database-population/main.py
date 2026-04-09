"""
Entry point for the annual session update.
Usage: python -m session.main [--force-update]
"""

import argparse
from pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(
        description="Run annual session update for legislator and contact data."
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Force update on snapshot schema without date filtering.",
    )
    args = parser.parse_args()
    run_pipeline(force_update=args.force_update)


if __name__ == "__main__":
    main()
