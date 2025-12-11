#!/usr/bin/env python3
"""
Quality Reporter API Client

A command-line tool to consume the Quality Reporter API and review DQA validation
results produced by the DQAValidator NiFi processor.

The DQAValidator publishes validation results to Kafka, which are aggregated by
the Quality Reporter service. This script provides a convenient way to query and
visualize those aggregated statistics.

Usage:
    python scripts/quality_report_client.py --help
    python scripts/quality_report_client.py report
    python scripts/quality_report_client.py report --format json
    python scripts/quality_report_client.py report --validator my-validator
    python scripts/quality_report_client.py clear
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print(
        "Error: 'requests' package is required. Install it with: pip install requests"
    )
    sys.exit(1)


@dataclass
class ReportEntry:
    """Represents a single validation report entry."""

    validator: str
    rule: str
    feature: str
    valid: int
    fail: int

    @property
    def total(self) -> int:
        return self.valid + self.fail

    @property
    def pass_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.valid / self.total) * 100

    @classmethod
    def from_dict(cls, data: dict) -> "ReportEntry":
        return cls(
            validator=data["validator"],
            rule=data["rule"],
            feature=data["feature"],
            valid=data["VALID"],
            fail=data["FAIL"],
        )


class QualityReporterClient:
    """Client for the Quality Reporter REST API."""

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_report(self) -> list[ReportEntry]:
        """
        Fetch the validation report from the API.

        Returns:
            List of ReportEntry objects containing validation statistics.

        Raises:
            requests.RequestException: If the API request fails.
        """
        url = urljoin(self.base_url + "/", "report")
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        return [ReportEntry.from_dict(entry) for entry in data]

    def clear_report(self) -> dict:
        """
        Clear all validation statistics from the report database.

        Returns:
            Response from the API containing deletion confirmation.

        Raises:
            requests.RequestException: If the API request fails.
        """
        url = urljoin(self.base_url + "/", "report")
        response = requests.delete(url, timeout=self.timeout)
        response.raise_for_status()
        return response.json()


def format_table(entries: list[ReportEntry]) -> str:
    """Format report entries as an ASCII table."""
    if not entries:
        return "No validation data available."

    # Column headers and widths
    headers = ["Validator", "Rule", "Feature", "Valid", "Fail", "Total", "Pass Rate"]

    # Calculate column widths
    widths = [
        max(len(headers[0]), max(len(e.validator) for e in entries)),
        max(len(headers[1]), max(len(e.rule) for e in entries)),
        max(len(headers[2]), max(len(e.feature) for e in entries)),
        max(len(headers[3]), max(len(str(e.valid)) for e in entries)),
        max(len(headers[4]), max(len(str(e.fail)) for e in entries)),
        max(len(headers[5]), max(len(str(e.total)) for e in entries)),
        max(len(headers[6]), 8),  # "100.00%"
    ]

    # Build table
    separator = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header_row = "|" + "|".join(f" {h:<{w}} " for h, w in zip(headers, widths)) + "|"

    lines = [separator, header_row, separator]

    for entry in entries:
        row = (
            "|"
            + "|".join(
                [
                    f" {entry.validator:<{widths[0]}} ",
                    f" {entry.rule:<{widths[1]}} ",
                    f" {entry.feature:<{widths[2]}} ",
                    f" {entry.valid:>{widths[3]}} ",
                    f" {entry.fail:>{widths[4]}} ",
                    f" {entry.total:>{widths[5]}} ",
                    f" {entry.pass_rate:>{widths[6]-1}.2f}% ",
                ]
            )
            + "|"
        )
        lines.append(row)

    lines.append(separator)

    # Summary statistics
    total_valid = sum(e.valid for e in entries)
    total_fail = sum(e.fail for e in entries)
    total_all = total_valid + total_fail
    overall_rate = (total_valid / total_all * 100) if total_all > 0 else 0

    lines.append("")
    lines.append(
        f"Summary: {len(entries)} rules, {total_valid} valid, {total_fail} failed, {overall_rate:.2f}% pass rate"
    )

    return "\n".join(lines)


def format_json(entries: list[ReportEntry]) -> str:
    """Format report entries as JSON."""
    data = [
        {
            "validator": e.validator,
            "rule": e.rule,
            "feature": e.feature,
            "valid": e.valid,
            "fail": e.fail,
            "total": e.total,
            "pass_rate": round(e.pass_rate, 2),
        }
        for e in entries
    ]
    return json.dumps(data, indent=2)


def format_summary(entries: list[ReportEntry]) -> str:
    """Format report entries as a summary grouped by validator."""
    if not entries:
        return "No validation data available."

    lines = []

    # Group by validator
    validators = {}
    for entry in entries:
        if entry.validator not in validators:
            validators[entry.validator] = []
        validators[entry.validator].append(entry)

    for validator, validator_entries in sorted(validators.items()):
        total_valid = sum(e.valid for e in validator_entries)
        total_fail = sum(e.fail for e in validator_entries)
        total_all = total_valid + total_fail
        pass_rate = (total_valid / total_all * 100) if total_all > 0 else 0

        lines.append(f"\n{'='*60}")
        lines.append(f"Validator: {validator}")
        lines.append(f"{'='*60}")
        lines.append(f"  Rules checked: {len(validator_entries)}")
        lines.append(f"  Total validations: {total_all}")
        lines.append(f"  Passed: {total_valid} ({pass_rate:.2f}%)")
        lines.append(f"  Failed: {total_fail} ({100-pass_rate:.2f}%)")

        # Show failing rules
        failing_rules = [e for e in validator_entries if e.fail > 0]
        if failing_rules:
            lines.append(f"\n  Failing rules:")
            for entry in sorted(failing_rules, key=lambda x: x.pass_rate):
                lines.append(
                    f"    - {entry.rule} on '{entry.feature}': {entry.fail} failures ({entry.pass_rate:.1f}% pass)"
                )

    return "\n".join(lines)


def cmd_report(args, client: QualityReporterClient):
    """Handle the 'report' command."""
    try:
        entries = client.get_report()

        # Filter by validator if specified
        if args.validator:
            entries = [e for e in entries if e.validator == args.validator]

        # Filter by rule if specified
        if args.rule:
            entries = [e for e in entries if e.rule == args.rule]

        # Format output
        if args.format == "json":
            print(format_json(entries))
        elif args.format == "summary":
            print(format_summary(entries))
        else:
            print(format_table(entries))

    except requests.RequestException as e:
        print(f"Error fetching report: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_clear(args, client: QualityReporterClient):
    """Handle the 'clear' command."""
    if not args.force:
        confirm = input("Are you sure you want to clear all validation data? [y/N]: ")
        if confirm.lower() != "y":
            print("Aborted.")
            return

    try:
        result = client.clear_report()
        print(f"Report cleared: {result.get('deleted_records', 0)} records deleted.")
    except requests.RequestException as e:
        print(f"Error clearing report: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_watch(args, client: QualityReporterClient):
    """Handle the 'watch' command - continuously poll and display updates."""
    import time

    last_data = None

    print(
        f"Watching for validation updates (refresh every {args.interval}s, Ctrl+C to stop)..."
    )
    print()

    try:
        while True:
            try:
                entries = client.get_report()

                # Filter if specified
                if args.validator:
                    entries = [e for e in entries if e.validator == args.validator]

                # Check if data changed
                current_data = [
                    (e.validator, e.rule, e.feature, e.valid, e.fail) for e in entries
                ]

                if current_data != last_data:
                    # Clear screen (cross-platform)
                    os.system("cls" if os.name == "nt" else "clear")
                    print(f"Quality Report (auto-refresh every {args.interval}s)")
                    print(f"Press Ctrl+C to stop\n")
                    print(format_table(entries))
                    last_data = current_data

                time.sleep(args.interval)

            except requests.RequestException as e:
                print(f"Error fetching report: {e}", file=sys.stderr)
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopped watching.")


def main():
    parser = argparse.ArgumentParser(
        description="Quality Reporter API Client - Review DQA validation results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s report                         # Show validation report as table
  %(prog)s report --format json           # Output as JSON
  %(prog)s report --format summary        # Show summary grouped by validator
  %(prog)s report --validator my-validator # Filter by validator ID
  %(prog)s watch                          # Continuously watch for updates
  %(prog)s clear                          # Clear all validation data

Environment Variables:
  QUALITY_REPORTER_URL   Base URL of the Quality Reporter API (default: http://localhost:8000)
""",
    )

    parser.add_argument(
        "--url",
        default=os.environ.get("QUALITY_REPORTER_URL", "http://localhost:8000"),
        help="Quality Reporter API base URL (default: http://localhost:8000 or QUALITY_REPORTER_URL env var)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds (default: 30)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Report command
    report_parser = subparsers.add_parser(
        "report", help="Fetch and display the validation report"
    )
    report_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json", "summary"],
        default="table",
        help="Output format (default: table)",
    )
    report_parser.add_argument("--validator", "-v", help="Filter by validator ID")
    report_parser.add_argument(
        "--rule",
        "-r",
        help="Filter by rule type (domain, strlen, datatype, categorical, exists, regex)",
    )

    # Clear command
    clear_parser = subparsers.add_parser(
        "clear", help="Clear all validation statistics"
    )
    clear_parser.add_argument(
        "--force", "-f", action="store_true", help="Skip confirmation prompt"
    )

    # Watch command
    watch_parser = subparsers.add_parser(
        "watch", help="Continuously watch for validation updates"
    )
    watch_parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=5,
        help="Refresh interval in seconds (default: 5)",
    )
    watch_parser.add_argument("--validator", "-v", help="Filter by validator ID")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    client = QualityReporterClient(args.url, timeout=args.timeout)

    if args.command == "report":
        cmd_report(args, client)
    elif args.command == "clear":
        cmd_clear(args, client)
    elif args.command == "watch":
        cmd_watch(args, client)


if __name__ == "__main__":
    main()
