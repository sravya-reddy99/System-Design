import bisect
import hashlib
import sqlite3
import threading


def stable_hash(key):
    # Python's built in hash() changes every time the program starts,
    # so a key could land on a different shard after a restart.
    # md5 gives the same number every time, on every machine.
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


# ---------------------------------------------------------------
# Hash ring: decides which shard owns a key
# ---------------------------------------------------------------

class HashRing:
    def __init__(self, vnodes=100):
        self._vnodes = vnodes
        self._points = []  # sorted positions on the ring
        self._owner = {}   # position -> shard name

    def add(self, name):
        # Each shard gets many points on the ring so load spreads evenly.
        for i in range(self._vnodes):
            h = stable_hash(f"{name}#{i}")
            bisect.insort(self._points, h)
            self._owner[h] = name

    def remove(self, name):
        for i in range(self._vnodes):
            h = stable_hash(f"{name}#{i}")
            self._points.remove(h)
            del self._owner[h]

    def get(self, key):
        # Walk clockwise from the key's position to the next shard point.
        h = stable_hash(key)
        i = bisect.bisect(self._points, h) % len(self._points)
        return self._owner[self._points[i]]


# ---------------------------------------------------------------
# Shard: one small database that holds part of the data
# ---------------------------------------------------------------

class Shard:
    def __init__(self, name):
        self.name = name
        self._db = sqlite3.connect(":memory:", check_same_thread=False)
        self._lock = threading.Lock()
        self._db.execute(
            "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, email TEXT)"
        )

    def put(self, user_id, name, email):
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO users VALUES (?, ?, ?)",
                (user_id, name, email),
            )
            self._db.commit()

    def get(self, user_id):
        with self._lock:
            row = self._db.execute(
                "SELECT id, name, email FROM users WHERE id = ?", (user_id,)
            ).fetchone()
        return None if row is None else {"id": row[0], "name": row[1], "email": row[2]}

    def delete(self, user_id):
        with self._lock:
            self._db.execute("DELETE FROM users WHERE id = ?", (user_id,))
            self._db.commit()

    def count(self):
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def all_rows(self):
        with self._lock:
            return self._db.execute("SELECT id, name, email FROM users").fetchall()


# ---------------------------------------------------------------
# Router: the part the API server talks to
# ---------------------------------------------------------------

class ShardRouter:
    def __init__(self, shards, vnodes=100):
        self._shards = {s.name: s for s in shards}
        self._ring = HashRing(vnodes)
        for s in shards:
            self._ring.add(s.name)

    def shard_for(self, key):
        return self._shards[self._ring.get(key)]

    def all_shards(self):
        return list(self._shards.values())

    def add_shard(self, new_shard):
        # Put the new shard on the ring, then move over only the rows
        # that now belong to it. Every other row stays where it is.
        self._shards[new_shard.name] = new_shard
        self._ring.add(new_shard.name)

        moved = 0
        for shard in list(self._shards.values()):
            if shard is new_shard:
                continue
            for user_id, name, email in shard.all_rows():
                if self._ring.get(user_id) == new_shard.name:
                    new_shard.put(user_id, name, email)
                    shard.delete(user_id)
                    moved += 1
        return moved


# ---------------------------------------------------------------
# API server: knows nothing about shards except through the router
# ---------------------------------------------------------------

class ApiServer:
    def __init__(self, router):
        self._router = router

    def create_user(self, user_id, name, email):
        shard = self._router.shard_for(user_id)
        shard.put(user_id, name, email)
        return {"id": user_id, "stored_on": shard.name}

    def get_user(self, user_id):
        shard = self._router.shard_for(user_id)
        user = shard.get(user_id)
        if user is None:
            return None
        user["served_by"] = shard.name
        return user

    def count_users(self):
        # No key here, so we have to ask every shard and add up the answers.
        # This is called scatter gather, and it is the price of sharding.
        per_shard = {s.name: s.count() for s in self._router.all_shards()}
        return {"total": sum(per_shard.values()), "per_shard": per_shard}

    def handle(self, method, path, body=None):
        # A tiny stand in for real HTTP routes, so the demo reads like an API.
        if method == "POST" and path == "/users":
            return self.create_user(body["id"], body["name"], body["email"])
        if method == "GET" and path.startswith("/users/"):
            return self.get_user(path.split("/")[-1])
        if method == "GET" and path == "/stats":
            return self.count_users()
        return {"error": "not found"}


# ---------------------------------------------------------------
# Demo
# ---------------------------------------------------------------

if __name__ == "__main__":
    router = ShardRouter([Shard("shard-A"), Shard("shard-B"), Shard("shard-C")])
    api = ApiServer(router)

    print("1. Writing 3000 users through the API")
    for i in range(3000):
        api.handle("POST", "/users", {
            "id": f"user-{i}", "name": f"Name {i}", "email": f"u{i}@example.com"
        })
    print("  ", api.handle("GET", "/stats"))

    print("\n2. Reading a few users back (the router finds the right shard)")
    for uid in ["user-7", "user-42", "user-999"]:
        print("  ", api.handle("GET", f"/users/{uid}"))

    print("\n3. Adding a 4th shard and moving only what belongs to it")
    moved = router.add_shard(Shard("shard-D"))
    print(f"   rows moved: {moved}")
    print("  ", api.handle("GET", "/stats"))

    keys = [f"user-{i}" for i in range(3000)]
    missing = [k for k in keys if api.get_user(k) is None]
    print(f"   users lost after the move: {len(missing)}")