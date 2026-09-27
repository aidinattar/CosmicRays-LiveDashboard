"""S3 -> Kafka stream emulator."""

from __future__ import annotations

import argparse
import json
import time
from typing import Iterable

import boto3
import pandas as pd
import urllib3
from botocore import UNSIGNED
from botocore.client import Config
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from tqdm import tqdm

from cosmic_rays.config import settings


def build_s3_client():
    kwargs = {
        "endpoint_url": settings.s3_endpoint,
        "verify": settings.s3_verify_tls,
    }
    if settings.s3_access_key_id and settings.s3_secret_access_key:
        kwargs.update(
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
        )
    else:
        kwargs["config"] = Config(signature_version=UNSIGNED)

    if not settings.s3_verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return boto3.client("s3", **kwargs)


def list_object_keys(s3_client) -> Iterable[str]:
    """Yield every object key in the configured bucket, including paginated results."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.s3_bucket):
        for item in page.get("Contents", []):
            yield item["Key"]


def reset_topics(admin: KafkaAdminClient) -> None:
    existing = set(admin.list_topics())
    to_delete = [
        topic
        for topic in (settings.stream_topic, settings.results_topic)
        if topic in existing
    ]
    if to_delete:
        admin.delete_topics(to_delete)
        deadline = time.time() + 30
        while time.time() < deadline:
            if not set(to_delete) & set(admin.list_topics()):
                break
            time.sleep(0.5)


def ensure_topics(admin: KafkaAdminClient) -> None:
    existing = set(admin.list_topics())
    topics = []
    for name in (settings.stream_topic, settings.results_topic):
        if name not in existing:
            topics.append(
                NewTopic(
                    name=name,
                    num_partitions=settings.topic_partitions,
                    replication_factor=settings.topic_replication_factor,
                )
            )
    if topics:
        try:
            admin.create_topics(new_topics=topics)
        except TopicAlreadyExistsError:
            # Another process may have created the topic between list/create.
            pass


def stream_forever(reset: bool = False) -> None:
    producer = KafkaProducer(bootstrap_servers=list(settings.kafka_bootstrap_servers))
    admin = KafkaAdminClient(
        bootstrap_servers=list(settings.kafka_bootstrap_servers),
        api_version=(0, 10, 2),
    )
    s3_client = build_s3_client()

    try:
        if reset:
            reset_topics(admin)
        ensure_topics(admin)

        object_keys = list(list_object_keys(s3_client))
        if not object_keys:
            raise RuntimeError(f"No objects found in S3 bucket {settings.s3_bucket!r}")

        print(
            f"Streaming {len(object_keys)} files from s3://{settings.s3_bucket} "
            f"to Kafka topic {settings.stream_topic!r}. Press Ctrl+C to stop."
        )

        while True:
            for key in object_keys:
                body = s3_client.get_object(Bucket=settings.s3_bucket, Key=key)["Body"]
                rows = pd.read_csv(body, dtype="str").to_dict("records")
                for message in tqdm(rows, desc=key, leave=False):
                    producer.send(
                        settings.stream_topic,
                        json.dumps(message).encode("utf-8"),
                    )
                    time.sleep(settings.producer_wait_seconds)
                producer.flush()
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.flush()
        producer.close()
        admin.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream miniDT detector rows from S3 to Kafka.")
    parser.add_argument(
        "--reset-topics",
        action="store_true",
        help="Delete and recreate stream/results topics before starting (matches the original notebook).",
    )
    args = parser.parse_args()
    stream_forever(reset=args.reset_topics)


if __name__ == "__main__":
    main()
