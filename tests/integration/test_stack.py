import os

import psycopg
import pytest
import redis
from dotenv import load_dotenv

load_dotenv()
PW = os.getenv("PG_PASSWORD", "postgres")


def dsn(port: int) -> str:
    return f"host=localhost port={port} dbname=app user=postgres password={PW} connect_timeout=2"


PRIMARY = int(os.getenv("PRIMARY_PORT", 5433))
REPLICAS = [int(os.getenv("REPLICA1_PORT", 5434)), int(os.getenv("REPLICA2_PORT", 5435))]

try:
    psycopg.connect(dsn(PRIMARY)).close()
    STACK_UP = True
except Exception:
    STACK_UP = False

pytestmark = pytest.mark.skipif(not STACK_UP, reason="docker stack not running")


def test_primary_has_seed_data():
    with psycopg.connect(dsn(PRIMARY)) as c:
        assert c.execute("select count(*) from orders").fetchone()[0] >= 200000


@pytest.mark.parametrize("port", REPLICAS)
def test_replicas_are_standbys(port):
    with psycopg.connect(dsn(port)) as c:
        assert c.execute("select pg_is_in_recovery()").fetchone()[0] is True


def test_replicas_are_streaming():
    with psycopg.connect(dsn(PRIMARY)) as c:
        n = c.execute(
            "select count(*) from pg_stat_replication where state='streaming'"
        ).fetchone()[0]
    assert n == 2


def test_logical_decoding_enabled():
    with psycopg.connect(dsn(PRIMARY)) as c:
        assert c.execute("show wal_level").fetchone()[0] == "logical"


def test_redis_ping():
    assert redis.Redis(port=int(os.getenv("REDIS_PORT", 6379))).ping()