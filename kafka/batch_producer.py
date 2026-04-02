#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Batch Producer: CSV → Kafka (kafka-python 0.9.5 + Python 2.6 compatible)
"""
import os
import sys
import csv
import json
import time
import argparse

from kafka import KafkaProducer

def parse_args():
    parser = argparse.ArgumentParser(description='Batch Producer: CSV → Kafka')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--topic', default='raw-data')
    parser.add_argument('--data-path', required=True, help='Path to CSV file')
    parser.add_argument('--limit', type=int, default=10, help='Number of records to send')
    parser.add_argument('--source', default='batch', help='Source identifier')
    return parser.parse_args()

def read_csv(filepath, limit):
    """Читает CSV и возвращает список словарей"""
    records = []
    with open(filepath, 'rb') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            records.append(row)
    return records

def main():
    args = parse_args()
    
    print("[INFO] Starting batch producer: data_path={}, limit={}, source={}".format(
        args.data_path, args.limit, args.source))
    
    if not os.path.exists(args.data_path):
        print("[ERROR] File not found: {}".format(args.data_path))
        sys.exit(1)
    
    records = read_csv(args.data_path, args.limit)
    print("[INFO] Read {} records from {}".format(len(records), args.data_path))
    
    def json_serializer(obj):
        return json.dumps(obj, ensure_ascii=False).encode('utf-8')
    
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=json_serializer,
    )
    
    sent = 0
    errors = 0
    
    try:
        for i, record in enumerate(records):
            record['source'] = args.source
            record['batch_index'] = i
            
            key = "{}_{}".format(args.source, i).encode('utf-8')
            value = json_serializer(record)
            
            try:
                producer.send(args.topic, key=key, value=value)
                sent += 1
                print("[INFO] Sent {}/{}: {}".format(sent, len(records), record))
            except Exception as e:
                print("[ERROR] Failed to send record {}: {}".format(i, str(e)))
                errors += 1
                continue
                
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user")
    except Exception as e:
        print("[ERROR] Producer error: {}".format(str(e)))
        errors += 1
    finally:
        producer.stop()
        print("[SUMMARY] Sent: {}, Errors: {}".format(sent, errors))

if __name__ == '__main__':
    main()