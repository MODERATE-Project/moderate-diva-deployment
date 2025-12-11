#!/usr/bin/env python3
"""
Dataset Publisher for Kafka REST Gateway

A command-line tool to publish dataset ingestion requests to the NiFi flow
via the Kafka REST Proxy. This script sends messages in the format expected
by the NiFi ConsumeKafka processor for triggering data ingestion.

Message Format:
    {
        "s3_url": "https://s3.amazonaws.com/bucket/file.csv",
        "dataset_id": "my-dataset"
    }

Usage:
    python scripts/publish_dataset.py --help
    python scripts/publish_dataset.py --s3-url https://example.com/data.csv --dataset-id my-dataset
    python scripts/publish_dataset.py --s3-url https://example.com/data.csv  # auto-generates dataset_id
    python scripts/publish_dataset.py --from-file datasets.json
"""

import argparse
import base64
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

try:
    import requests
except ImportError:
    print(
        "Error: 'requests' package is required. Install it with: pip install requests"
    )
    sys.exit(1)


@dataclass
class DatasetMessage:
    """Represents a dataset ingestion request message."""

    s3_url: str
    dataset_id: str

    def to_dict(self) -> dict:
        return {
            "s3_url": self.s3_url,
            "dataset_id": self.dataset_id,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_url(
        cls, s3_url: str, dataset_id: Optional[str] = None
    ) -> "DatasetMessage":
        """Create a DatasetMessage from a URL, auto-generating dataset_id if not provided."""
        if dataset_id is None:
            # Extract filename from URL and use it as dataset_id
            parsed = urlparse(s3_url)
            path = parsed.path.rstrip("/")
            filename = path.split("/")[-1] if path else ""
            # Remove extension and sanitize
            base_name = filename.rsplit(".", 1)[0] if "." in filename else filename
            if base_name:
                dataset_id = base_name
            else:
                # Fallback to UUID if we can't extract a name
                dataset_id = f"dataset-{uuid.uuid4().hex[:8]}"
        return cls(s3_url=s3_url, dataset_id=dataset_id)


class KafkaRestClient:
    """Client for the Confluent Kafka REST Proxy API."""

    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.auth = None
        if username and password:
            self.auth = (username, password)

    def publish(
        self,
        topic: str,
        messages: list[DatasetMessage],
        key: Optional[str] = None,
    ) -> dict:
        """
        Publish messages to a Kafka topic via the REST Proxy.

        Args:
            topic: The Kafka topic to publish to.
            messages: List of DatasetMessage objects to publish.
            key: Optional key for all messages (for partitioning).

        Returns:
            Response from the Kafka REST Proxy.

        Raises:
            requests.RequestException: If the API request fails.
        """
        url = f"{self.base_url}/topics/{topic}"

        # Kafka REST Proxy v2 format
        # Using JSON embedded format (value_schema not required)
        records = []
        for msg in messages:
            record = {"value": msg.to_dict()}
            if key:
                record["key"] = key
            records.append(record)

        payload = {"records": records}

        headers = {
            "Content-Type": "application/vnd.kafka.json.v2+json",
            "Accept": "application/vnd.kafka.v2+json",
        }

        response = requests.post(
            url,
            json=payload,
            headers=headers,
            auth=self.auth,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def list_topics(self) -> list[str]:
        """List available Kafka topics."""
        url = f"{self.base_url}/topics"
        headers = {"Accept": "application/vnd.kafka.v2+json"}
        response = requests.get(
            url, headers=headers, auth=self.auth, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def get_topic_metadata(self, topic: str) -> dict:
        """Get metadata for a specific topic."""
        url = f"{self.base_url}/topics/{topic}"
        headers = {"Accept": "application/vnd.kafka.v2+json"}
        response = requests.get(
            url, headers=headers, auth=self.auth, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()


def get_env_config() -> dict:
    """Get configuration from environment variables."""
    machine_url = os.environ.get("MACHINE_URL", "localhost")

    # Build Kafka REST URL from MACHINE_URL
    kafka_rest_url = os.environ.get(
        "KAFKA_REST_URL", f"https://kafka-rest.{machine_url}"
    )

    return {
        "kafka_rest_url": kafka_rest_url,
        "username": os.environ.get("KAFKA_REST_BASIC_AUTH_USER"),
        "password": os.environ.get("KAFKA_REST_BASIC_AUTH_PASSWORD"),
        "default_topic": os.environ.get(
            "KAFKA_INGESTION_TOPIC", "data-ingestion-trigger"
        ),
    }


def cmd_publish(args, client: KafkaRestClient):
    """Handle the 'publish' command."""
    messages = []

    if args.from_file:
        # Load messages from JSON file
        try:
            with open(args.from_file, "r") as f:
                data = json.load(f)
            if isinstance(data, list):
                for item in data:
                    messages.append(
                        DatasetMessage(
                            s3_url=item["s3_url"],
                            dataset_id=item.get("dataset_id")
                            or DatasetMessage.from_url(item["s3_url"]).dataset_id,
                        )
                    )
            else:
                messages.append(
                    DatasetMessage(
                        s3_url=data["s3_url"],
                        dataset_id=data.get("dataset_id")
                        or DatasetMessage.from_url(data["s3_url"]).dataset_id,
                    )
                )
        except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # Single message from command line
        if not args.s3_url:
            print("Error: --s3-url is required", file=sys.stderr)
            sys.exit(1)
        messages.append(DatasetMessage.from_url(args.s3_url, args.dataset_id))

    topic = args.topic

    # Display what we're about to publish
    print(f"Publishing {len(messages)} message(s) to topic '{topic}':")
    for msg in messages:
        print(f"  - dataset_id: {msg.dataset_id}")
        print(f"    s3_url: {msg.s3_url}")

    if args.dry_run:
        print("\n[DRY RUN] No messages were actually published.")
        print("\nPayload that would be sent:")
        print(json.dumps([msg.to_dict() for msg in messages], indent=2))
        return

    try:
        result = client.publish(topic, messages, key=args.key)

        # Check for errors in response
        offsets = result.get("offsets", [])
        errors = [o for o in offsets if o.get("error")]

        if errors:
            print(f"\nErrors publishing some messages:", file=sys.stderr)
            for err in errors:
                print(f"  - {err.get('error')}: {err.get('message')}", file=sys.stderr)
            sys.exit(1)

        print(f"\nSuccessfully published {len(offsets)} message(s):")
        for i, offset in enumerate(offsets):
            print(
                f"  [{i}] partition={offset.get('partition')}, offset={offset.get('offset')}"
            )

    except requests.HTTPError as e:
        print(f"HTTP Error: {e}", file=sys.stderr)
        if e.response is not None:
            try:
                error_body = e.response.json()
                print(f"Response: {json.dumps(error_body, indent=2)}", file=sys.stderr)
            except Exception:
                print(f"Response: {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"Error publishing messages: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_list_topics(args, client: KafkaRestClient):
    """Handle the 'list-topics' command."""
    try:
        topics = client.list_topics()
        print(f"Available topics ({len(topics)}):")
        for topic in sorted(topics):
            print(f"  - {topic}")
    except requests.RequestException as e:
        print(f"Error listing topics: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_topic_info(args, client: KafkaRestClient):
    """Handle the 'topic-info' command."""
    try:
        metadata = client.get_topic_metadata(args.topic)
        print(f"Topic: {metadata.get('name')}")
        print(f"Partitions: {len(metadata.get('partitions', []))}")
        configs = metadata.get("configs", {})
        if configs:
            print("Configuration:")
            for key, value in sorted(configs.items()):
                print(f"  {key}: {value}")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Topic '{args.topic}' not found", file=sys.stderr)
        else:
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"Error getting topic info: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    config = get_env_config()

    parser = argparse.ArgumentParser(
        description="Publish dataset ingestion requests to Kafka via REST Proxy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Publish a single dataset
  %(prog)s publish --s3-url https://s3.amazonaws.com/bucket/data.csv --dataset-id my-dataset

  # Auto-generate dataset_id from URL filename
  %(prog)s publish --s3-url https://s3.amazonaws.com/bucket/sales-2024.csv

  # Publish multiple datasets from a JSON file
  %(prog)s publish --from-file datasets.json

  # Dry run (show what would be published)
  %(prog)s publish --s3-url https://example.com/data.csv --dry-run

  # List available topics
  %(prog)s list-topics

  # Get topic information
  %(prog)s topic-info data-ingestion

Environment Variables:
  MACHINE_URL                      Machine URL for building Kafka REST URL
  KAFKA_REST_URL                   Full Kafka REST Proxy URL (overrides MACHINE_URL)
  KAFKA_REST_BASIC_AUTH_USER       Basic auth username
  KAFKA_REST_BASIC_AUTH_PASSWORD   Basic auth password
  KAFKA_INGESTION_TOPIC            Default topic name (default: data-ingestion)

JSON File Format (for --from-file):
  [
    {"s3_url": "https://s3.example.com/data1.csv", "dataset_id": "dataset-1"},
    {"s3_url": "https://s3.example.com/data2.csv"}
  ]
""",
    )

    parser.add_argument(
        "--url",
        default=config["kafka_rest_url"],
        help=f"Kafka REST Proxy URL (default: {config['kafka_rest_url']})",
    )
    parser.add_argument(
        "--username",
        "-u",
        default=config["username"],
        help="Basic auth username (default: from KAFKA_REST_BASIC_AUTH_USER)",
    )
    parser.add_argument(
        "--password",
        "-p",
        default=config["password"],
        help="Basic auth password (default: from KAFKA_REST_BASIC_AUTH_PASSWORD)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds (default: 30)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Publish command
    publish_parser = subparsers.add_parser(
        "publish", help="Publish dataset ingestion request(s)"
    )
    publish_parser.add_argument(
        "--s3-url",
        help="S3 URL of the dataset to ingest",
    )
    publish_parser.add_argument(
        "--dataset-id",
        help="Dataset identifier (auto-generated from URL if not provided)",
    )
    publish_parser.add_argument(
        "--topic",
        "-t",
        default=config["default_topic"],
        help=f"Kafka topic to publish to (default: {config['default_topic']})",
    )
    publish_parser.add_argument(
        "--key",
        "-k",
        help="Optional message key for partitioning",
    )
    publish_parser.add_argument(
        "--from-file",
        "-f",
        help="JSON file containing dataset(s) to publish",
    )
    publish_parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Show what would be published without actually publishing",
    )

    # List topics command
    subparsers.add_parser("list-topics", help="List available Kafka topics")

    # Topic info command
    topic_info_parser = subparsers.add_parser(
        "topic-info", help="Get information about a topic"
    )
    topic_info_parser.add_argument("topic", help="Topic name")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    client = KafkaRestClient(
        base_url=args.url,
        username=args.username,
        password=args.password,
        timeout=args.timeout,
    )

    if args.command == "publish":
        cmd_publish(args, client)
    elif args.command == "list-topics":
        cmd_list_topics(args, client)
    elif args.command == "topic-info":
        cmd_topic_info(args, client)


if __name__ == "__main__":
    main()
