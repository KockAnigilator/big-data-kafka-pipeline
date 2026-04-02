import argparse
import csv
import datetime
import codecs
import json
import logging
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


logger = logging.getLogger("batch_producer")


class CSVRecordReader(object):
    def __init__(self, csv_path, delimiter=",", encoding="utf-8", has_header=True):
        self.csv_path = csv_path
        self.delimiter = delimiter
        self.encoding = encoding
        self.has_header = has_header

    def iter_records(self, limit=None):
        idx = 0
        # Python 2.6 compatibility: use codecs.open for encoding support.
        with codecs.open(self.csv_path, "r", encoding=self.encoding) as f:
            if self.has_header:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                for row in reader:
                    idx += 1
                    if limit is not None and idx > limit:
                        break
                    yield dict(row)
            else:
                reader2 = csv.reader(f, delimiter=self.delimiter)
                for row in reader2:
                    idx += 1
                    if limit is not None and idx > limit:
                        break
                    if not row:
                        continue
                    record = dict((("col_%d" % i), v) for (i, v) in enumerate(row))
                    yield record


class JsonlRecordReader(object):
    def __init__(self, jsonl_path, encoding="utf-8"):
        self.jsonl_path = jsonl_path
        self.encoding = encoding

    def iter_records(self, limit=None):
        idx = 0
        with codecs.open(self.jsonl_path, "r", encoding=self.encoding) as f:
            for line in f:
                idx += 1
                if limit is not None and idx > limit:
                    break
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def resolve_reader(data_path):
    if os.path.isdir(data_path):
        csv_files = sorted(
            [
                os.path.join(data_path, p)
                for p in os.listdir(data_path)
                if p.lower().endswith(".csv")
            ]
        )
        if not csv_files:
            raise ValueError("No .csv files found in directory: %s" % data_path)
        data_path = csv_files[0]

    lower = data_path.lower()
    if lower.endswith(".csv"):
        return CSVRecordReader(data_path)
    if lower.endswith(".jsonl"):
        return JsonlRecordReader(data_path)
    raise ValueError(
        "Unsupported data format for path=%s. Expected .csv (or .jsonl for JSONL)." % data_path
    )


def utc_millis_isoformat():
    now = datetime.datetime.utcnow()
    millis = now.microsecond / 1000
    # Example: 2026-04-02T12:34:56.789Z
    return now.strftime("%Y-%m-%dT%H:%M:%S") + ".%03dZ" % millis


def build_message(record, source, record_id):
    # Ensure required field `source` is always present (and not overwritten by CSV columns).
    msg = dict(record)
    msg["record_id"] = record_id
    msg["producer_ts_utc"] = utc_millis_isoformat()
    msg["source"] = source
    return msg


def parse_args():
    parser = argparse.ArgumentParser(description="Batch producer: CSV/JSONL -> Kafka -> JSON messages")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="raw-data", help="Kafka topic name")
    parser.add_argument(
        "--data-path",
        default=os.path.join("..", "data", "dataset.csv"),
        help="Path to CSV (or directory containing CSV) relative to kafka/ (batch_producer location)",
    )
    parser.add_argument("--limit", type=int, default=10, help="Send only first N records (0 = all)")
    parser.add_argument("--source", default="batch", help='Value for required JSON field: message["source"]')
    parser.add_argument(
        "--acks",
        type=str,
        default="1",
        choices=["0", "1", "all"],
        help="Kafka producer acks setting",
    )
    parser.add_argument("--client-id", default="batch-producer", help="Kafka client id")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.abspath(os.path.join(script_dir, args.data_path))

    limit = None if args.limit == 0 else args.limit
    reader = resolve_reader(data_path)

    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            client_id=args.client_id,
            key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, unicode) else k),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks=args.acks,
            retries=3,
            linger_ms=10,
            request_timeout_ms=30000,
        )
    except NoBrokersAvailable as e:
        logger.error("No Kafka brokers available: %s", e)
        return 2
    except Exception as e:
        logger.exception("Failed to initialize Kafka producer: %s", e)
        return 2

    sent = 0
    pending = []
    flush_every = 50

    logger.info("Starting production to topic=%s bootstrap_servers=%s", args.topic, args.bootstrap_servers)
    logger.info("Data input: %s (limit=%s, source=%s)", data_path, limit, args.source)

    try:
        record_id = 0
        for record in reader.iter_records(limit=limit):
            record_id += 1
            msg = build_message(record, source=args.source, record_id=record_id)
            key = "%s_%d" % (args.source, record_id)
            try:
                fut = producer.send(args.topic, key=key, value=msg)
                pending.append((record_id, fut))
            except KafkaError as e:
                logger.exception("Kafka send failed at record_id=%s: %s", record_id, e)
                return 3

            if len(pending) >= flush_every:
                for rid, future in pending:
                    try:
                        future.get(timeout=60)
                        sent += 1
                    except Exception as e:
                        logger.exception("Delivery failed for record_id=%s: %s", rid, e)
                        return 4
                pending = []

        for rid, future in pending:
            try:
                future.get(timeout=60)
                sent += 1
            except Exception as e:
                logger.exception("Delivery failed for record_id=%s: %s", rid, e)
                return 4

        logger.info("Finished sending. Total delivered=%d", sent)
        return 0
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Attempting to shutdown producer gracefully...")
        return 130
    finally:
        if producer is not None:
            try:
                producer.flush()
            except Exception:
                pass
            try:
                producer.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())

