"""Verify primary + replicas + Redis are up and replicating.
Run from the project root: python scripts/check_stack.py
"""
import os
import sys
import time

import psycopg
import redis
from dotenv import load_dotenv

load_dotenv()
PW = os.getenv("PG_PASSWORD", "postgres")
PORTS = {
    "primary": int(os.getenv("PRIMARY_PORT", 5433)),
    "replica1": int(os.getenv("REPLICA1_PORT", 5434)),
    "replica2": int(os.getenv("REPLICA2_PORT", 5435)),
}


def dsn(port: int) -> str:
    return f"host=localhost port={port} dbname=app user=postgres password={PW}"


def lsn_to_int(text: str) -> int:
    hi, lo = text.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def connect_with_retry(name: str, port: int, timeout: float = 60.0):
    """Wait for a server that may still be starting up."""
    start = time.time()
    while True:
        try:
            return psycopg.connect(dsn(port), autocommit=True, connect_timeout=3)
        except psycopg.OperationalError as e:
            if time.time() - start > timeout:
                print(f"{name}: could not connect after {timeout:.0f}s: {e}")
                return None
            time.sleep(1)


def main() -> int:
    ok = True
    primary = connect_with_retry("primary", PORTS["primary"])
    if primary is None:
        return 1
    n = primary.execute("select count(*) from accounts").fetchone()[0]
    print(f"primary ok, accounts rows: {n}")

    replicas = {}
    for name in ("replica1", "replica2"):
        conn = connect_with_retry(name, PORTS[name])
        if conn is None:
            ok = False
        else:
            replicas[name] = conn

    streaming = 0
    for _ in range(30):
        streaming = primary.execute(
            "select count(*) from pg_stat_replication where state='streaming'"
        ).fetchone()[0]
        if streaming == 2:
            break
        time.sleep(1)
    print(f"replicas streaming from primary: {streaming}")
    ok &= streaming == 2

    probe_id = primary.execute(
        "insert into accounts(name) values ('probe') returning id"
    ).fetchone()[0]
    target = lsn_to_int(primary.execute("select pg_current_wal_lsn()::text").fetchone()[0])

    for name, replica in replicas.items():
        in_recovery = replica.execute("select pg_is_in_recovery()").fetchone()[0]
        if not in_recovery:
            print(f"{name}: NOT a standby")
            ok = False
            continue
        start = time.time()
        caught_up = False
        while time.time() - start < 15:
            replay = lsn_to_int(
                replica.execute("select pg_last_wal_replay_lsn()::text").fetchone()[0]
            )
            if replay >= target:
                caught_up = True
                break
            time.sleep(0.05)
        seen = replica.execute(
            "select count(*) from accounts where id=%s", (probe_id,)
        ).fetchone()[0]
        print(f"{name}: standby=True caught_up={caught_up} "
              f"({time.time() - start:.2f}s) probe_row_visible={bool(seen)}")
        ok &= caught_up and bool(seen)

    r = redis.Redis(port=int(os.getenv("REDIS_PORT", 6379)))
    print("redis ping:", r.ping())

    print("\nRESULT:", "ALL GOOD" if ok else "PROBLEM, see lines above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())