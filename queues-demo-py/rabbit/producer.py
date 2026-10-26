import json
import os
import sys

import pika

URL = os.environ.get("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE = "tasks"
COUNT = int(sys.argv[1]) if len(sys.argv) > 1 else 5


def main():
    connection = pika.BlockingConnection(pika.URLParameters(URL))
    channel = connection.channel()

    # A confirm channel: the broker tells us when it has taken responsibility
    # for each message. If it cannot, basic_publish raises an error, so we know.
    channel.confirm_delivery()

    # durable: the queue itself survives a broker restart
    channel.queue_declare(queue=QUEUE, durable=True)

    for task_id in range(1, COUNT + 1):
        body = json.dumps({"id": task_id, "text": f"task {task_id}"})
        channel.basic_publish(
            exchange="",  # the default exchange sends straight to the queue named in routing_key
            routing_key=QUEUE,
            body=body,
            # delivery_mode 2 means persistent: the message is written to disk,
            # so it survives a restart too
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
            mandatory=True,
        )
        print(f"broker confirmed task {task_id}")

    print(f"done, {COUNT} tasks are safely in the queue")
    connection.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"producer failed: {err}")
        sys.exit(1)
