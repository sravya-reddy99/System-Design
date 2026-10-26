# RabbitMQ and Kafka, locally (Python)

A simple producer and consumer for each, in Python.

## One time setup

You need Docker Desktop (running) and Python 3.9 or newer.

```
cd queues-demo-py
python3 -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d
```

Wait about 20 seconds. RabbitMQ is ready when `docker compose ps` shows it as healthy.
Kafka is ready when this prints a line:

```
docker logs demo-kafka 2>&1 | grep "Kafka Server started"
```

RabbitMQ also has a web page at http://localhost:15672 (login guest, password guest).
Open the Queues tab to watch the "tasks" queue while you run the steps below.

## RabbitMQ

The producer puts 5 tasks in a queue called "tasks". The consumer takes them one at a time.

```
python rabbit/producer.py          # 5 tasks, each one confirmed by the broker
python rabbit/consumer.py          # takes them, finishes them, Ctrl and C to stop
```

Try these:

**A consumer that crashes.** The task it was holding comes back.

```
python rabbit/producer.py
CRASH_ON=3 python rabbit/consumer.py     # dies while holding task 3
python rabbit/consumer.py                # gets task 3 again, marked "delivered again"
```

**A task that fails.** It gets one more try, then it is dropped.

```
python rabbit/producer.py
FAIL_ON=4 python rabbit/consumer.py
```

**Two consumers.** Start the producer with a bigger number, `python rabbit/producer.py 20`, and run the consumer in two terminals. They share the tasks.

(On Windows PowerShell, set a switch like this: `$env:CRASH_ON=3; python rabbit/consumer.py`)

## Kafka

The producer writes 12 orders to a topic called "orders", which has 3 partitions.
The consumer reads them as part of a group.

```
python kafka/producer.py           # 12 orders, prints the partition and offset of each
python kafka/consumer.py           # reads all of them as group "order-processors"
```

Try these:

**Two consumers in one group.** They split the partitions between them.

```
python kafka/consumer.py           # terminal 1
python kafka/consumer.py           # terminal 2, watch terminal 1 rearrange itself
```

**A different group reads everything again.** Messages are not deleted when read.

```
python kafka/consumer.py audit-team
```

**A consumer that crashes.** It continues from the last saved offset, so the order it was holding is read again.

```
CRASH_ON=5 python kafka/consumer.py crash-test    # dies on order 5
python kafka/consumer.py crash-test               # starts again at order 5
```

## Stop and clean up

```
docker compose down
```

This also deletes all the data.
