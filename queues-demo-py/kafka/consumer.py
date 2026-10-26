import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaError

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
TOPIC = "orders"
GROUP = sys.argv[1] if len(sys.argv) > 1 else "order-processors"  # same group, shared work
WORK_SECONDS = float(os.environ.get("WORK_MS", "500")) / 1000
CRASH_ON = int(os.environ["CRASH_ON"]) if os.environ.get("CRASH_ON") else None  # die before saving progress


def on_assign(consumer, partitions):
    # Called every time the group is rearranged, for example when a second consumer joins
    mine = sorted(p.partition for p in partitions)
    print(f'[group "{GROUP}"] this consumer now reads partitions: {", ".join(map(str, mine)) or "none"}', flush=True)


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": GROUP,
            # Only matters the FIRST time a group is seen.
            # After that, the group continues from where it saved its progress.
            "auto.offset.reset": "earliest",
            # We save our progress by hand, after the work is done
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC], on_assign=on_assign)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # The topic may not exist yet if the producer has not run. Keep waiting.
                if msg.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC):
                    print("waiting for the topic to exist, run the producer first", flush=True)
                    time.sleep(2)
                    continue
                raise RuntimeError(msg.error())

            order = json.loads(msg.value())
            print(
                f"partition {msg.partition()} offset {msg.offset()} key {msg.key().decode()} -> order {order['order']}",
                flush=True,
            )

            if order["order"] == CRASH_ON:
                print(f"crashing on purpose before saving progress for order {order['order']}", flush=True)
                os._exit(1)

            time.sleep(WORK_SECONDS)  # the real work goes here

            # Save progress: "I am done up to this message". Kafka stores the NEXT offset to read.
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"consumer failed: {err}")
        sys.exit(1)
