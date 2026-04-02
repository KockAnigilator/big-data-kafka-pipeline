#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Consumer для Kafka → HDFS (kafka-python 0.9.5 + Python 2.6 compatible)
"""
import os
import sys
import json
import time
import subprocess
import argparse
from datetime import datetime

# Простой импорт для kafka-python 0.9.5
from kafka import KafkaConsumer

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka → HDFS Consumer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--topic', default='raw-data')
    parser.add_argument('--group-id', default='raw-data-hdfs')
    parser.add_argument('--hdfs-base', default='/user/cloudera/raw_data')
    return parser.parse_args()

def run_hdfs_command(args_list):
    """Выполняет hdfs dfs команду через subprocess"""
    cmd = ["hdfs", "dfs"] + args_list
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        return 0, output
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output

def save_to_hdfs(data, source, date_str, timestamp):
    """Сохраняет сообщение в HDFS"""
    hdfs_path = os.path.join(args.hdfs_base, "source={}".format(source), "date={}".format(date_str))
    filename = "file_{}.json".format(timestamp)
    local_path = "/tmp/{}".format(filename)
    
    # Создаём локальный файл
    with open(local_path, 'w') as f:
        json.dump(data, f, ensure_ascii=False)
    
    # Создаём директорию в HDFS
    run_hdfs_command(["-mkdir", "-p", hdfs_path])
    
    # Копируем файл в HDFS
    hdfs_full_path = os.path.join(hdfs_path, filename)
    returncode, output = run_hdfs_command(["-put", "-f", local_path, hdfs_full_path])
    
    # Удаляем локальный файл
    if os.path.exists(local_path):
        os.remove(local_path)
    
    if returncode == 0:
        print("[INFO] Saved to HDFS: {}".format(hdfs_full_path))
        return True
    else:
        print("[ERROR] Failed to save to HDFS: {}".format(output))
        return False

def main():
    global args
    args = parse_args()
    
    print("[INFO] Starting consumer: topic={}, bootstrap_servers={}".format(args.topic, args.bootstrap_servers))
    
    # kafka-python 0.9.5 API
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset='smallest',
        consumer_timeout_ms=1000
    )
    
    processed = 0
    errors = 0
    
    try:
        for message in consumer:
            try:
                value = message.value
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                data = json.loads(value)
                
                source = data.get('source', 'batch')
                date_str = datetime.now().strftime('%Y-%m-%d')
                timestamp = int(time.time() * 1000)
                
                if save_to_hdfs(data, source, date_str, timestamp):
                    processed += 1
                else:
                    errors += 1
                    
            except Exception as e:
                print("[ERROR] Processing message: {}".format(str(e)))
                errors += 1
                continue
                
    except KeyboardInterrupt:
        print("\n[INFO] Ctrl+C pressed, shutting down...")
    except Exception as e:
        print("[ERROR] Consumer error: {}".format(str(e)))
        errors += 1
    finally:
        consumer.close()
        print("[SUMMARY] Processed: {}, Errors: {}".format(processed, errors))

if __name__ == '__main__':
    main()