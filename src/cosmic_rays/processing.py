"""Kafka -> Spark Structured Streaming -> Kafka processing application."""

from __future__ import annotations

import json
import time

import findspark
from kafka import KafkaProducer

from cosmic_rays.config import settings
from cosmic_rays.messages import empty_result_message

# Make the system Spark installation importable before importing pyspark.
findspark.init(settings.spark_home)

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, collect_list, from_json, struct, when
from pyspark.sql.types import StringType, StructField, StructType


INPUT_SCHEMA = StructType(
    [
        StructField("HEAD", StringType()),
        StructField("FPGA", StringType()),
        StructField("TDC_CHANNEL", StringType()),
        StructField("ORBIT_CNT", StringType()),
        StructField("BX_COUNTER", StringType()),
        StructField("TDC_MEAS", StringType()),
    ]
)

# Retained from the original notebook for the future DRIFTIME extension.
TIME_OFFSET_BY_CHAMBER = {
    0: 95.0 - 1.1,
    1: 95.0 + 6.4,
    2: 95.0 + 0.5,
    3: 95.0 - 2.6,
}

_RESULT_PRODUCER = None


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master(settings.spark_master)
        .appName(settings.spark_app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.jars.packages", settings.spark_kafka_package)
        .config("spark.ui.port", settings.spark_ui_port)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def build_input_dataframe(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_string)
        .option("subscribe", settings.stream_topic)
        .load()
    )
    parsed = raw.select(
        from_json(col("value").cast("string"), INPUT_SCHEMA).alias("value")
    )
    flat = parsed.select(
        "value.HEAD",
        "value.FPGA",
        "value.TDC_CHANNEL",
        "value.ORBIT_CNT",
        "value.BX_COUNTER",
        "value.TDC_MEAS",
    )

    return (
        flat.where(col("HEAD") == 2)
        .withColumn(
            "CHAMBER",
            when(
                (col("FPGA") == 0)
                & (col("TDC_CHANNEL") >= 0)
                & (col("TDC_CHANNEL") < 64),
                0,
            )
            .when(
                (col("FPGA") == 0)
                & (col("TDC_CHANNEL") >= 64)
                & (col("TDC_CHANNEL") <= 127),
                1,
            )
            .when(
                (col("FPGA") == 1)
                & (col("TDC_CHANNEL") >= 0)
                & (col("TDC_CHANNEL") < 64),
                2,
            )
            .when(
                (col("FPGA") == 1)
                & (col("TDC_CHANNEL") >= 64)
                & (col("TDC_CHANNEL") <= 127),
                3,
            )
            .when((col("FPGA") == 1) & (col("TDC_CHANNEL") == 128), 4)
            .otherwise(None),
        )
        .filter(col("CHAMBER").isNotNull())
    )


def batch_proc(df, epoch_id):
    """Compute the same per-batch quantities as the original notebook."""
    global _RESULT_PRODUCER
    if _RESULT_PRODUCER is None:
        _RESULT_PRODUCER = KafkaProducer(
            bootstrap_servers=list(settings.kafka_bootstrap_servers)
        )

    start = time.time()
    message = empty_result_message()
    message["epoch_id"] = epoch_id

    # Keep the original partition/cache strategy unchanged.
    df.coalesce(15)
    df.persist()

    try:
        t1 = time.time()
        total_hits = df.filter(col("CHAMBER") != 4).count()
        stop_1 = time.time() - t1

        t2 = time.time()
        df_counts = (
            df.filter(col("CHAMBER") != 4)
            .groupBy("CHAMBER")
            .count()
            .withColumnRenamed("count", "ch_hits")
        )
        stop_2 = time.time() - t2

        t3 = time.time()
        df_counts_ch = (
            df.filter(col("CHAMBER") != 4)
            .groupBy("TDC_CHANNEL", "CHAMBER")
            .count()
            .groupBy("CHAMBER")
            .agg(
                struct(
                    collect_list("TDC_CHANNEL").alias("bin_edges"),
                    collect_list("count").alias("counts"),
                ).alias("histo_CH")
            )
        )
        stop_3 = time.time() - t3

        t4 = time.time()
        df_counts_orb = (
            df.filter(col("CHAMBER") != 4)
            .groupBy("ORBIT_CNT", "CHAMBER")
            .agg(F.countDistinct("TDC_CHANNEL"))
            .groupBy("count(TDC_CHANNEL)", "CHAMBER")
            .count()
            .groupBy("CHAMBER")
            .agg(
                struct(
                    collect_list("count(TDC_CHANNEL)").alias("bin_edges"),
                    collect_list("count").alias("counts"),
                ).alias("histo_ORB")
            )
        )
        stop_4 = time.time() - t4

        t5 = time.time()
        list_scint = df.filter(col("CHAMBER") == 4).select("ORBIT_CNT")
        df_counts_sc = (
            df.filter(col("CHAMBER") != 4)
            .join(list_scint, on="ORBIT_CNT", how="inner")
            .groupBy("TDC_CHANNEL", "CHAMBER")
            .count()
            .groupBy("CHAMBER")
            .agg(
                struct(
                    collect_list("TDC_CHANNEL").alias("bin_edges"),
                    collect_list("count").alias("counts"),
                ).alias("histo_SC")
            )
        )
        stop_5 = time.time() - t5

        t6 = time.time()
        rows = (
            df_counts.join(df_counts_ch, on="CHAMBER")
            .join(df_counts_orb, on="CHAMBER")
            .join(df_counts_sc, on="CHAMBER")
            .sort("CHAMBER")
            .collect()
        )
        stop_6 = time.time() - t6

        message["hits"] = total_hits
        for row in rows:
            chamber_id = int(row["CHAMBER"])
            if chamber_id not in range(4):
                continue
            chamber = message[f"CH{chamber_id}"]
            chamber["total_hits"] = row["ch_hits"]
            chamber["histo_CH"]["bin_edges"] = row["histo_CH"]["bin_edges"]
            chamber["histo_CH"]["bin_counts"] = row["histo_CH"]["counts"]
            chamber["histo_ORB"]["bin_edges"] = row["histo_ORB"]["bin_edges"]
            chamber["histo_ORB"]["bin_counts"] = row["histo_ORB"]["counts"]
            chamber["histo_SC"]["bin_edges"] = row["histo_SC"]["bin_edges"]
            chamber["histo_SC"]["bin_counts"] = row["histo_SC"]["counts"]

        message["time"] = time.time() - start
        print(
            f"epoch={epoch_id} hits={total_hits} "
            f"timings=[{stop_1:.3f}, {stop_2:.3f}, {stop_3:.3f}, "
            f"{stop_4:.3f}, {stop_5:.3f}, {stop_6:.3f}]s "
            f"total={message['time']:.3f}s"
        )

        _RESULT_PRODUCER.send(
            settings.results_topic,
            json.dumps(message).encode("utf-8"),
        )
        _RESULT_PRODUCER.flush()
    finally:
        df.unpersist()


def run() -> None:
    global _RESULT_PRODUCER
    spark = create_spark_session()
    _RESULT_PRODUCER = KafkaProducer(
        bootstrap_servers=list(settings.kafka_bootstrap_servers)
    )
    query = None
    try:
        dataframe = build_input_dataframe(spark)
        query = (
            dataframe.writeStream.foreachBatch(batch_proc)
            .trigger(processingTime=settings.spark_trigger_interval)
            .start()
        )
        print(
            f"Spark processor running: {settings.stream_topic!r} -> "
            f"{settings.results_topic!r}. Press Ctrl+C to stop."
        )
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nSpark processor stopped.")
    finally:
        if query is not None and query.isActive:
            query.stop()
        if _RESULT_PRODUCER is not None:
            _RESULT_PRODUCER.flush()
            _RESULT_PRODUCER.close()
            _RESULT_PRODUCER = None
        spark.stop()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
