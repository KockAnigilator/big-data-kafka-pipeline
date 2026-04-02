import argparse
import datetime
import json
import logging
import os
import subprocess
import tempfile
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable


logger = logging.getLogger("consumer_hdfs")


def run_hdfs(hdfs_args):
    """
    Python 2.6 compatible wrapper around: hdfs dfs <args>
    """
    cmd = ["hdfs", "dfs"] + hdfs_args
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd, output=out)
    return out


class HdfsUploader(object):
    def __init__(self, hdfs_base_dir="/user/cloudera/raw_data"):
        self.hdfs_base_dir = hdfs_base_dir.rstrip("/")

    def extract_date_str_from_message(self, message):
        ts = message.get("producer_ts_utc")
        if isinstance(ts, (str, unicode)) and ts:
            try:
                # ts examples: 2026-04-02T12:34:56.789Z
                ts_no_z = ts[:-1]  # remove trailing 'Z'
                d = datetime.datetime.strptime(ts_no_z, "%Y-%m-%dT%H:%M:%S.%f").date()
                return d.isoformat()
            except Exception:
                pass
        return datetime.datetime.utcnow().date().isoformat()

    def compute_hdfs_path(self, source, file_ts_ms, date_str):
        return "%s/source=%s/date=%s/file_%d.json" % (self.hdfs_base_dir, source, date_str, file_ts_ms)

    def put_json_message(self, message, source):
        file_ts_ms = int(time.time() * 1000)
        date_str = self.extract_date_str_from_message(message)
        hdfs_path = self.compute_hdfs_path(source, file_ts_ms, date_str)
        hdfs_dir = os.path.dirname(hdfs_path)

        try:
            run_hdfs(["-mkdir", "-p", hdfs_dir])
        except Exception as e:
            logger.warning("Failed to ensure HDFS directory exists: %s", e)

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tmp_path = tmp.name
        try:
            content = json.dumps(message, ensure_ascii=False)
            tmp.write(content.encode("utf-8"))
            tmp.flush()
            tmp.close()

            run_hdfs(["-put", tmp_path, hdfs_path])
            return hdfs_path
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka consumer: raw-data -> HDFS (JSON files)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="raw-data", help="Kafka topic name")
    parser.add_argument("--group-id", default="raw-data-hdfs", help="Kafka consumer group id")

    parser.add_argument("--hdfs-base-dir", default="/user/cloudera/raw_data", help="HDFS base directory")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")

    parser.add_argument("--max-retries", type=int, default=10, help="Retries on Kafka connection errors")
    parser.add_argument("--retry-backoff-secs", type=int, default=5, help="Backoff between retries")
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    uploader = HdfsUploader(hdfs_base_dir=args.hdfs_base_dir)

    retries_left = args.max_retries
    consumer = None
    while True:
        try:
            def value_deserializer(b):
                if b is None:
                    return None
                try:
                    return b.decode("utf-8")
                except Exception:
                    return b

            try:
                consumer = KafkaConsumer(
                    args.topic,
                    bootstrap_servers=args.bootstrap_servers,
                    group_id=args.group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    value_deserializer=value_deserializer,
                    consumer_timeout_ms=1000,
                )
            except TypeError:
                # Some kafka-python versions may not support consumer_timeout_ms.
                consumer = KafkaConsumer(
                    args.topic,
                    bootstrap_servers=args.bootstrap_servers,
                    group_id=args.group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    value_deserializer=value_deserializer,
                )
            logger.info("Connected to Kafka. Subscribed topic=%s group_id=%s", args.topic, args.group_id)
            break
        except NoBrokersAvailable as e:
            retries_left -= 1
            logger.error("No Kafka brokers available: %s", e)
            if retries_left <= 0:
                return 2
            logger.info(
                "Retrying Kafka connection in %s sec... (%s retries left)",
                args.retry_backoff_secs,
                retries_left,
            )
            time.sleep(args.retry_backoff_secs)
        except Exception as e:
            logger.exception("Kafka consumer initialization failed: %s", e)
            return 2

    logger.info("Starting consume loop (Ctrl+C to stop)...")
    try:
        while True:
            delivered_any = False
            for record in consumer:
                delivered_any = True
                raw_value = record.value
                try:
                    message = json.loads(raw_value)
                except Exception:
                    logger.exception("Invalid JSON message at offset=%s", record.offset)
                    consumer.commit()
                    continue

                source = message.get("source")
                if not source:
                    logger.error("Missing required field source in message. offset=%s", record.offset)
                    consumer.commit()
                    continue

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
                    logger.error("HDFS upload failed (will not commit): returncode=%s", e.returncode)
                    # Do not commit: message should be retried on next loop.
                    continue
                except Exception as e:
                    logger.exception("Unexpected error while uploading message: %s", e)
                    continue

            if not delivered_any:
                # consumer_timeout_ms hit: no messages this round
                logger.info("No messages... waiting")
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

