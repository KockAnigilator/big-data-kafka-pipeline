# Kafka -> HDFS учебный пайплайн

Связка: **Data -> `batch_producer.py` -> Kafka (`raw-data`) -> `consumer_hdfs.py` -> HDFS**.

## 0) Что важно для Cloudera QuickStart VM (CentOS 6)
Код запускать **внутри VM**, так как:
- Kafka слушает `localhost:9092` (внутри VM).
- HDFS доступен через `hdfs dfs` (внутри VM).

В скриптах `producer`/`consumer` используется Python 3-синтаксис, поэтому на CentOS 6 убедитесь, что есть `python3.6` (или нужная версия) и `pip3.6`.

## 1) Установка зависимостей на VM
Из корня репозитория (примерно `/home/cloudera/...` или ваш путь):
```bash
python3.6 -m pip install --user -r requirements.txt
```

Нужна библиотека `kafka-python`. Команды HDFS выполняются через:
`subprocess.run(["hdfs", "dfs", ...])`.

## 2) Топик `raw-data`
Топик вы уже создали, но для проверки/восстановления:
```bash
cd /opt/kafka  # где лежит Kafka на вашей VM
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic raw-data --partitions 1 --replication-factor 1
```

Проверить:
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 3) Запуск Batch Producer (внутри VM)
Скрипт читает CSV/JSONL и отправляет **каждую строку** как отдельное JSON-сообщение в топик `raw-data`.

Пример тестовой отправки первых 10 записей:
```bash
python3.6 kafka/batch_producer.py \
  --bootstrap-servers localhost:9092 \
  --topic raw-data \
  --data-path ../data/your_file.csv \
  --limit 10 \
  --source batch
```

Важно:
- В каждом сообщении обязательно есть поле `source` (по умолчанию `batch`).
- Ключ сообщения (`key`) формируется как `<source>_<record_id>`.

Примечание про данные:
параметр `--data-path` интерпретируется **от папки `kafka/`**. Для лаб обычно используют `../data/...`.
Если у вас папка называется `Data` (как в этом репозитории на Windows), используйте `../Data/...`.

## 4) Запуск Consumer -> HDFS (внутри VM)
consumer читает сообщения из `raw-data` в цикле, для каждого сообщения сохраняет JSON в HDFS по шаблону:
`/user/cloudera/raw_data/source=<source>/date=<YYYY-MM-DD>/file_<timestamp>.json`

Запуск:
```bash
python3.6 kafka/consumer_hdfs.py \
  --bootstrap-servers localhost:9092 \
  --topic raw-data \
  --group-id raw-data-hdfs \
  --hdfs-base-dir /user/cloudera/raw_data
```

Graceful shutdown:
- остановка по `Ctrl+C`
- на выход выполняется `commit()` оффсетов (если были обработанные сообщения)

## 5) Как проверить результат в HDFS
Пример (дата подставляется вашей `YYYY-MM-DD`):
```bash
hdfs dfs -ls /user/cloudera/raw_data/source=batch/date=YYYY-MM-DD/
```

Посмотреть содержимое одного файла:
```bash
hdfs dfs -cat /user/cloudera/raw_data/source=batch/date=YYYY-MM-DD/file_<timestamp>.json | head -n 20
```

## 6) Перенос кода на VM
Вариант A (рекомендуется):
- на VM выполнить `git clone` вашего репозитория, например:
```bash
cd ~
git clone <ваш-git-url>
```

Вариант B:
- через `scp` с хоста на VM (примерно):
```bash
scp -r <папка-репозитория> <user>@<vm-ip>:/home/<user>/
```
- затем убедиться, что структура `kafka/` на месте

## 7) Чек-лист сдачи лабораторной
1. `raw-data` топик создан.
2. Consumer запущен и работает в цикле.
3. Producer отправляет первые 10 записей с `--source batch`.
4. В HDFS появились файлы по пути `source=batch/date=.../`.
5. Содержимое одного файла соответствует JSON формату из producer.

