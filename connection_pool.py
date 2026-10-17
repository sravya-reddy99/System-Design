import itertools
import threading
import time
from collections import deque
from contextlib import contextmanager


class BoundedBlockingQueue:
    def __init__(self, capacity):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._items = deque()
        self._capacity = capacity
        lock = threading.Lock()
        self._not_empty = threading.Condition(lock)
        self._not_full = threading.Condition(lock)

    def put(self, item, timeout=None):
        with self._not_full:
            ok = self._not_full.wait_for(
                lambda: len(self._items) < self._capacity, timeout
            )
            if not ok:
                raise TimeoutError("queue is full")
            self._items.append(item)
            self._not_empty.notify()

    def get(self, timeout=None):
        with self._not_empty:
            ok = self._not_empty.wait_for(lambda: self._items, timeout)
            if not ok:
                raise TimeoutError("queue is empty")
            item = self._items.popleft()
            self._not_full.notify()
            return item


class ConnectionPool:
    def __init__(self, factory, size):
        self._queue = BoundedBlockingQueue(size)
        for _ in range(size):
            self._queue.put(factory())

    def acquire(self, timeout=5):
        return self._queue.get(timeout)

    def release(self, conn):
        self._queue.put(conn)

    @contextmanager
    def connection(self, timeout=5):
        conn = self.acquire(timeout)
        try:
            yield conn
        finally:
            self.release(conn)


if __name__ == "__main__":
    ids = itertools.count(1)

    def make_conn():
        return f"conn-{next(ids)}"

    pool = ConnectionPool(make_conn, size=3)

    def worker(n):
        with pool.connection() as conn:
            print(f"worker {n} using {conn}")
            time.sleep(0.5)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()