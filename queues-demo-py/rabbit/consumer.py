import json
import os
import sys
import time

import pika

URL = os.environ.get("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE = "tasks"
WORK_SECONDS = float(os.environ.get("WORK_MS", "500")) / 1000  # pretend each task takes this long

# Two switches to practice with:
#   CRASH_ON=3  the consumer dies while holding task 3, before it says "done"
#   FAIL_ON=4   task 4 always raises an error
CRASH_ON = int(os.environ["CRASH_ON"]) if os.environ.get("CRASH_ON") else None
FAIL_ON = int(os.environ["FAIL_ON"]) if os.environ.get("FAIL_ON") else None


def handle(channel, method, properties, body):
    task = json.loads(body)
    again = method.redelivered  # True if this task was handed out before
    print(f"got task {task['id']}" + ("  (delivered again)" if again else ""), flush=True)

    if task["id"] == CRASH_ON and not again:
        print(f"crashing on purpose before finishing task {task['id']}", flush=True)
        os._exit(1)  # no ack was sent, so the broker will hand this task out again

    try:
        if task["id"] == FAIL_ON:
            raise RuntimeError("something went wrong")
        time.sleep(WORK_SECONDS)

        # Only now do we tell the broker the task is finished and can be deleted
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"finished task {task['id']}", flush=True)
    except Exception as err:
        if not again:
            print(f"task {task['id']} failed ({err}), putting it back to try once more", flush=True)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            print(f"task {task['id']} failed twice, giving up on it", flush=True)
            # requeue=False: dropped, or sent to a dead letter queue if one is set
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    connection = pika.BlockingConnection(pika.URLParameters(URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)

    # Give this consumer only one unfinished task at a time.
    # If we run several consumers, the work is shared fairly between them.
    channel.basic_qos(prefetch_count=1)

    # auto_ack=False: we will acknowledge by hand
    channel.basic_consume(queue=QUEUE, on_message_callback=handle, auto_ack=False)

    print(f'waiting for tasks in "{QUEUE}", press Ctrl and C to stop', flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"consumer failed: {err}")
        sys.exit(1)
