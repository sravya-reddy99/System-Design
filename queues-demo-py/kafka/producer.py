import json
import os
import sys

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
TOPIC = "orders"
COUNT = int(sys.argv[1]) if len(sys.argv) > 1 else 12

# Six customers. The key decides the partition, and these six happen to
# spread across all three partitions (alice, bob -> 0, dave, frank -> 1, carol, erin -> 2)
USERS = ["alice", "bob", "carol", "dave", "erin", "frank"]


def make_topic():
    # Create the topic with 3 partitions (3 separate lines, each in order).
    # If it already exists, we just say so.
    admin = AdminClient({"bootstrap.servers": BROKERS})
    # request_timeout: give up after 10 seconds if Kafka is not running, instead of waiting forever
    futures = admin.create_topics(
        [NewTopic(TOPIC, num_partitions=3, replication_factor=1)], request_timeout=10
    )
    for _, future in futures.items():
        try:
            future.result()
            print(f'created topic "{TOPIC}" with 3 partitions')
        except Exception as err:
            if "TOPIC_ALREADY_EXISTS" in str(err):
                print(f'topic "{TOPIC}" already exists')
            else:
                raise


def main():
    make_topic()

    producer = Producer(
        {
            "bootstrap.servers": BROKERS,
            # acks all: wait until all in-sync copies of the message are saved.
            # This is the strongest setting, the one that does not lose messages.
            "acks": "all",
            # Idempotent: if a send is retried after a network problem, Kafka
            # notices and does not store the same message twice.
            "enable.idempotence": True,
            # Picks the partition from the key: same key, same partition, every time
            "partitioner": "murmur2_random",
        }
    )

    def on_delivery(err, msg):
        # Called once Kafka has answered for this message
        if err is not None:
            print(f"failed: {err}")
            return
        order = json.loads(msg.value())
        print(f"order {order['order']} for {order['user']} -> partition {msg.partition()}, offset {msg.offset()}")

    for i in range(1, COUNT + 1):
        user = USERS[(i - 1) % len(USERS)]
        order = {"order": i, "user": user, "item": f"item {i}"}
        producer.produce(TOPIC, key=user, value=json.dumps(order), on_delivery=on_delivery)
        producer.flush(10)  # wait for the answer, so the output is easy to read

    print(f"done, {COUNT} orders are stored")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"producer failed: {err}")
        sys.exit(1)
