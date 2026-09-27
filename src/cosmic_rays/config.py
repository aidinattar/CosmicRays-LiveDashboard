"""Runtime configuration for the cosmic-ray streaming pipeline.

All environment-specific values live here so that the processing code stays
portable and credentials never need to be committed to Git.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional convenience at runtime
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _split_csv(value: str) -> Tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: Tuple[str, ...] = _split_csv(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "pd-slave3:9092")
    )
    stream_topic: str = os.getenv("KAFKA_STREAM_TOPIC", "topic_stream")
    results_topic: str = os.getenv("KAFKA_RESULTS_TOPIC", "topic_results")
    topic_partitions: int = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "14"))
    topic_replication_factor: int = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1"))

    spark_home: str = os.getenv("SPARK_HOME", "/usr/local/spark")
    spark_master: str = os.getenv("SPARK_MASTER", "spark://pd-master:7077")
    spark_app_name: str = os.getenv(
        "SPARK_APP_NAME", "Project_CosmicRays_Dashboard_application"
    )
    spark_ui_port: str = os.getenv("SPARK_UI_PORT", "4041")
    spark_trigger_interval: str = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")
    spark_kafka_package: str = os.getenv(
        "SPARK_KAFKA_PACKAGE",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    )

    s3_endpoint: str = os.getenv(
        "S3_ENDPOINT", "https://cloud-areapd.pd.infn.it:5210"
    )
    s3_bucket: str = os.getenv("S3_BUCKET", "mapd-minidt-stream")
    s3_access_key_id: str = os.getenv("S3_ACCESS_KEY_ID", "")
    s3_secret_access_key: str = os.getenv("S3_SECRET_ACCESS_KEY", "")
    s3_verify_tls: bool = _as_bool(os.getenv("S3_VERIFY_TLS", "false"))
    producer_wait_seconds: float = float(os.getenv("PRODUCER_WAIT_SECONDS", "0.00018"))

    @property
    def kafka_bootstrap_string(self) -> str:
        return ",".join(self.kafka_bootstrap_servers)


settings = Settings()
