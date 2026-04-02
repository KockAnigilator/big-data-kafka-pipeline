import argparse
import datetime as dt
import json
import logging
import os
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable


logger = logging.getLogger("consumer_hdfs")


def run_hdfs(args: List[str], *, timeout_s: int = 120) -> subprocess.CompletedProcess:
    """
    Execute `hdfs dfs ...` using subprocess.run (no shell=True).
    """
    cmd = ["hdfs", "dfs"] + args
    return subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )


class HdfsUploader:
    def __init__(self, *, hdfs_base_dir: str = "/user/cloudera/raw_data") -> None:
        self.hdfs_base_dir = hdfs_base_dir.rstrip("/")

    def extract_date_str_from_message(self, message: Dict[str, Any]) -> str:
        # Use producer timestamp if present so filenames/directories are stable.
        ts = message.get("producer_ts_utc")
        if isinstance(ts, str) and ts:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    return dt.datetime.strptime(ts, fmt).date().isoformat()
                except ValueError:
                    pass
        # Fallback: current UTC.
        return dt.datetime.utcnow().date().isoformat()

    def compute_hdfs_paths(
        self,
        *,
        source: str,
        file_ts_ms: int,
        date_str: Optional[str] = None,
    ) -> str:
        if not date_str:
            date_str = dt.datetime.utcnow().date().isoformat()
        return (
            f"{self.hdfs_base_dir}/source={source}/date={date_str}/"
            f"file_{file_ts_ms}.json"
        )

    def put_json_message(self, message: Dict[str, Any], *, source: str) -> str:
        file_ts_ms = int(time.time() * 1000)
        date_str = self.extract_date_str_from_message(message)
        hdfs_path = self.compute_hdfs_paths(source=source, file_ts_ms=file_ts_ms, date_str=date_str)
        hdfs_dir = os.path.dirname(hdfs_path)

        # Ensure directory exists (best-effort).
        try:
            run_hdfs(["-mkdir", "-p", hdfs_dir], timeout_s=60)
        except subprocess.CalledProcessError as e:
            logger.warning("Failed to ensure HDFS directory exists: %s", e)

        with tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            suffix=".json",
            encoding="utf-8",
        ) as tmp:
            json.dump(message, tmp, ensure_ascii=False)
            tmp_path = tmp.name

        try:
            run_hdfs(["-put", tmp_path, hdfs_path], timeout_s=120)
            return hdfs_path
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka consumer: raw-data -> HDFS (JSON files)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="raw-data", help="Kafka topic name")
    parser.add_argument("--group-id", default="raw-data-hdfs", help="Kafka consumer group id")

    parser.add_argument("--hdfs-base-dir", default="/user/cloudera/raw_data", help="HDFS base directory")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")

    parser.add_argument(
        "--idle-log-interval-secs",
        type=int,
        default=10,
        help="How often to log when there are no messages",
    )
    parser.add_argument(
        "--on-invalid",
        default="skip",
        choices=["skip", "stop"],
        help="What to do if a message is not valid JSON or missing required fields",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=10,
        help="Retries on Kafka connection errors",
    )
    parser.add_argument("--retry-backoff-secs", type=int, default=5, help="Backoff between retries")
    return parser.parse_args()


def extract_source(message: Dict[str, Any]) -> Optional[str]:
    source = message.get("source")
    if isinstance(source, str) and source:
        return source
    return None


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    uploader = HdfsUploader(hdfs_base_dir=args.hdfs_base_dir)

    consumer: Optional[KafkaConsumer] = None
    retries_left = args.max_retries

    while True:
        try:
            consumer = KafkaConsumer(
                args.topic,
                bootstrap_servers=args.bootstrap_servers,
                group_id=args.group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                # We'll decode bytes -> str, then json.loads ourselves.
                value_deserializer=lambda b: b.decode("utf-8"),
                consumer_timeout_ms=1000,  # allows loop to continue for idle logging
                max_poll_records=10,
            )
            logger.info("Connected to Kafka. Subscribed topic=%s group_id=%s", args.topic, args.group_id)
            break
        except NoBrokersAvailable as e:
            retries_left -= 1
            logger.error("No Kafka brokers available: %s", e)
            if retries_left <= 0:
                return 2
            logger.info("Retrying Kafka connection in %s sec... (%s retries left)", args.retry_backoff_secs, retries_left)
            time.sleep(args.retry_backoff_secs)
        except Exception as e:
            logger.exception("Kafka consumer initialization failed: %s", e)
            return 2

    idle_count = 0

    try:
        logger.info("Starting consume loop (Ctrl+C to stop)...")
        while True:
            try:
                msg = consumer.poll(timeout_ms=1000)
            except KafkaError as e:
                logger.error("Kafka poll failed: %s", e)
                continue

            if not msg:
                idle_count += 1
                if idle_count * 1 >= args.idle_log_interval_secs:
                    logger.info("No messages yet... still waiting (idle=%ss)", idle_count)
                    idle_count = 0
                continue

            # consumer.poll may return records for multiple partitions.
            # Normalize by iterating through the batch values.
            delivered_any = False
            for _, records in msg.items():
                for record in records:
                    delivered_any = True
                    raw_value = record.value
                    try:
                        message = json.loads(raw_value)
                    except Exception:
                        logger.exception("Invalid JSON message at offset=%s. raw_value=%r", record.offset, raw_value)
                        if args.on_invalid == "stop":
                            return 3
                        # Skip poison message and advance offsets.
                        consumer.commit()
                        continue

                    source = extract_source(message)
                    if not source:
                        logger.error("Missing required field source in message. offset=%s", record.offset)
                        if args.on_invalid == "stop":
                            return 3
                        consumer.commit()
                        continue

                    # Upload and commit only after success.
                    try:
                        hdfs_path = uploader.put_json_message(message, source=source)
                        logger.info(
                            "Saved message to HDFS: topic=%s partition=%s offset=%s hdfs=%s",
                            record.topic,
                            record.partition,
                            record.offset,
                            hdfs_path,
                        )
                        consumer.commit()
                    except subprocess.CalledProcessError as e:
                        logger.error("HDFS upload failed (will not commit): %s", e.stderr.strip() if e.stderr else e)
                        # Do not commit: message should be retried on next poll.
                        continue
                    except Exception as e:
                        logger.exception("Unexpected error while uploading message: %s", e)
                        continue

            if delivered_any:
                idle_count = 0
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Committing offsets and closing consumer...")
        try:
            if consumer is not None:
                consumer.commit()
        except Exception:
            pass
        return 130
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())

