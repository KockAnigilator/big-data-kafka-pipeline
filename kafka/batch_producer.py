import argparse
import csv
import datetime as dt
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


logger = logging.getLogger("batch_producer")


class RecordReader(ABC):
    @abstractmethod
    def iter_records(self, *, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError


class CSVRecordReader(RecordReader):
    def __init__(
        self,
        csv_path: str,
        *,
        delimiter: str = ",",
        encoding: str = "utf-8",
        has_header: bool = True,
    ) -> None:
        self.csv_path = csv_path
        self.delimiter = delimiter
        self.encoding = encoding
        self.has_header = has_header

    def iter_records(self, *, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        # Note: keep values as strings; this is typical for ingestion pipelines.
        with open(self.csv_path, "r", encoding=self.encoding, newline="") as f:
            if self.has_header:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                for idx, row in enumerate(reader, start=1):
                    if limit is not None and idx > limit:
                        break
                    yield dict(row)
            else:
                # Fallback mode: generate column names col_0..col_N.
                reader2 = csv.reader(f, delimiter=self.delimiter)
                for idx, row in enumerate(reader2, start=1):
                    if limit is not None and idx > limit:
                        break
                    if not row:
                        continue
                    record = {f"col_{i}": v for i, v in enumerate(row)}
                    yield record


class JsonlRecordReader(RecordReader):
    def __init__(self, jsonl_path: str, *, encoding: str = "utf-8") -> None:
        self.jsonl_path = jsonl_path
        self.encoding = encoding

    def iter_records(self, *, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        with open(self.jsonl_path, "r", encoding=self.encoding) as f:
            for idx, line in enumerate(f, start=1):
                if limit is not None and idx > limit:
                    break
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def resolve_reader(data_path: str) -> RecordReader:
    if os.path.isdir(data_path):
        csv_files = sorted(
            [
                os.path.join(data_path, p)
                for p in os.listdir(data_path)
                if p.lower().endswith(".csv")
            ]
        )
        if not csv_files:
            raise ValueError(f"No .csv files found in directory: {data_path}")
        data_path = csv_files[0]

    lower = data_path.lower()
    if lower.endswith(".csv"):
        return CSVRecordReader(data_path)
    if lower.endswith(".jsonl"):
        return JsonlRecordReader(data_path)
    raise ValueError(
        f"Unsupported data format for path={data_path}. Expected .csv (or .jsonl for JSONL)."
    )


def build_message(record: Dict[str, Any], *, source: str, record_id: int) -> Dict[str, Any]:
    # Keep message self-describing for downstream consumer.
    return {
        "record_id": record_id,
        "producer_ts_utc": dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        **record,
        # Ensure required field is always present and not overwritten by CSV columns.
        "source": source,
    }


def parse_args() -> argparse.Namespace:
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


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Convert --data-path relative to kafka/ directory (this script location).
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.abspath(os.path.join(script_dir, args.data_path))

    limit = None if args.limit == 0 else args.limit

    reader = resolve_reader(data_path)

    producer: Optional[KafkaProducer] = None
    try:
        # retries/timeout are best-effort; connectivity issues are handled with explicit exception handling below.
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            client_id=args.client_id,
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
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
        for record_id, record in enumerate(reader.iter_records(limit=limit), start=1):
            msg = build_message(record, source=args.source, record_id=record_id)
            key = f"{args.source}_{record_id}"
            try:
                fut = producer.send(args.topic, key=key, value=msg)
                pending.append((record_id, fut))
            except KafkaError as e:
                logger.exception("Kafka send failed at record_id=%s: %s", record_id, e)
                # Fail fast: reconnect issues usually won't succeed for subsequent records.
                return 3

            if len(pending) >= flush_every:
                for rid, future in pending:
                    try:
                        future.get(timeout=60)
                        sent += 1
                    except Exception as e:
                        logger.exception("Delivery failed for record_id=%s: %s", rid, e)
                        return 4
                pending.clear()

        # Flush remaining pending futures
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
                producer.flush(timeout=30)
            except Exception:
                pass
            try:
                producer.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())

