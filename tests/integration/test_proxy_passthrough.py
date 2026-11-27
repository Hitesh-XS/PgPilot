"""End-to-end tests: real psycopg clients -> PgPilot proxy -> real Postgres.

Skipped automatically if the Docker stack (primary on PRIMARY_PORT) is not running.
"""
import asyncio
import os
import time

import psycopg
import pytest
from dotenv import load_dotenv

from proxy.config import Settings
from proxy.server import ProxyServer

load_dotenv()
PW = os.getenv("PG_PASSWORD", "postgres")
PRIMARY_PORT = int(os.getenv("PRIMARY_PORT", 5433))

try:
    psycopg.connect(
        f"host=127.0.0.1 port={PRIMARY_PORT} dbname=app user=postgres password={PW} connect_timeout=2"
    ).close()
    STACK_UP = True
except Exception:
    STACK_UP = False

pytestmark = pytest.mark.skipif(not STACK_UP, reason="docker stack not running")


@pytest.fixture
async def proxy():
    settings = Settings(
        listen_host="127.0.0.1", listen_port=0, client_password=PW,
        primary_host="127.0.0.1", primary_port=PRIMARY_PORT,
        backend_user="postgres", backend_password=PW, pool_max_size=10,
    )
    server = ProxyServer(settings)
    await server.start()
    yield server
    await server.stop()


def dsn(server, password=PW, **extra):
    opts = " ".join(f"{k}={v}" for k, v in extra.items())
    return (f"host=127.0.0.1 port={server.port} dbname=app user=postgres "
            f"password={password} connect_timeout=5 {opts}")


async def run(fn, *args):
    """Run blocking psycopg code in a thread so the proxy's event loop keeps serving."""
    return await asyncio.to_thread(fn, *args)


async def test_select_one(proxy):
    def work():
        with psycopg.connect(dsn(proxy)) as c:
            return c.execute("select 1").fetchone()[0]
    assert await run(work) == 1


async def test_reads_seed_data(proxy):
    def work():
        with psycopg.connect(dsn(proxy)) as c:
            return c.execute("select count(*) from accounts").fetchone()[0]
    assert await run(work) >= 1000


async def test_wrong_password_rejected(proxy):
    def work():
        with pytest.raises(psycopg.OperationalError) as e:
            psycopg.connect(dsn(proxy, password="nope"))
        return str(e.value)
    assert "password authentication failed" in await run(work)


async def test_write_commit_visible_on_primary(proxy):
    def work():
        marker = f"proxy_{time.time_ns()}"
        with psycopg.connect(dsn(proxy)) as c:
            c.execute("insert into accounts(name) values (%s)", (marker,))
            c.commit()
            seen_via_proxy = c.execute(
                "select count(*) from accounts where name=%s", (marker,)).fetchone()[0]
        with psycopg.connect(
            f"host=127.0.0.1 port={PRIMARY_PORT} dbname=app user=postgres password={PW}"
        ) as direct:
            seen_direct = direct.execute(
                "select count(*) from accounts where name=%s", (marker,)).fetchone()[0]
        return seen_via_proxy, seen_direct
    assert await run(work) == (1, 1)


async def test_rollback_discards_changes(proxy):
    def work():
        marker = f"rollback_{time.time_ns()}"
        with psycopg.connect(dsn(proxy)) as c:
            c.execute("insert into accounts(name) values (%s)", (marker,))
            c.rollback()
            return c.execute(
                "select count(*) from accounts where name=%s", (marker,)).fetchone()[0]
    assert await run(work) == 0


async def test_server_side_prepared_statements(proxy):
    def work():
        with psycopg.connect(dsn(proxy), prepare_threshold=0) as c:
            return [c.execute("select balance from accounts where id=%s", (i,)).fetchone()
                    is not None for i in range(1, 11)]
    assert all(await run(work))


async def test_error_then_connection_still_usable(proxy):
    def work():
        with psycopg.connect(dsn(proxy)) as c:
            with pytest.raises(psycopg.errors.UndefinedTable):
                c.execute("select * from no_such_table")
            c.rollback()
            return c.execute("select 42").fetchone()[0]
    assert await run(work) == 42


async def test_copy_to_stdout(proxy):
    def work():
        with psycopg.connect(dsn(proxy)) as c:
            with c.cursor().copy("copy (select id from accounts limit 50) to stdout") as copy:
                return len(b"".join(bytes(chunk) for chunk in copy).splitlines())
    assert await run(work) == 50


async def test_query_cancel(proxy):
    """Non-blocking cancel (libpq 17+ style): SSLRequest then CancelRequest."""
    def run_query():
        import threading
        with psycopg.connect(dsn(proxy)) as c:
            threading.Thread(
                target=lambda: (time.sleep(0.5), c.cancel_safe()), daemon=True).start()
            t0 = time.time()
            with pytest.raises(psycopg.errors.QueryCanceled):
                c.execute("select pg_sleep(10)")
            return time.time() - t0
    assert await run(run_query) < 5


def test_query_cancel_legacy_with_separate_proxy_process(tmp_path):
    """Legacy blocking cancel() holds the GIL, so the proxy must be a separate process
    (which is how it runs in real life)."""
    import socket
    import subprocess
    import sys
    import threading

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = {**os.environ, "PROXY_PORT": str(port), "PRIMARY_PORT": str(PRIMARY_PORT),
           "PG_PASSWORD": PW, "PROXY_LOG_LEVEL": "WARNING"}
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    proc = subprocess.Popen([sys.executable, "-m", "proxy"], cwd=root, env=env)
    try:
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            pytest.fail("proxy subprocess did not start")
        url = (f"host=127.0.0.1 port={port} dbname=app user=postgres "
               f"password={PW} connect_timeout=5")
        with psycopg.connect(url) as c:
            threading.Thread(target=lambda: (time.sleep(0.5), c.cancel()), daemon=True).start()
            t0 = time.time()
            with pytest.raises(psycopg.errors.QueryCanceled):
                c.execute("select pg_sleep(10)")
            assert time.time() - t0 < 5
    finally:
        proc.terminate()
        proc.wait(timeout=10)


async def test_session_state_reset_between_clients(proxy):
    def first():
        with psycopg.connect(dsn(proxy), autocommit=True) as c:
            c.execute("set statement_timeout = 1234")
            return c.execute("show statement_timeout").fetchone()[0]

    def second():
        with psycopg.connect(dsn(proxy), autocommit=True) as c:
            return c.execute("show statement_timeout").fetchone()[0]

    assert await run(first) == "1234ms"
    assert await run(second) == "0"


async def test_pool_reuses_backend_connections(proxy):
    def work():
        for _ in range(6):
            with psycopg.connect(dsn(proxy)) as c:
                c.execute("select 1")
    await run(work)
    await asyncio.sleep(0.3)
    stats = proxy.pool.stats()
    assert stats["total"] == 1 and stats["leased"] == 0


async def test_client_disconnect_mid_transaction_does_not_poison_pool(proxy):
    def abandon():
        c = psycopg.connect(dsn(proxy))
        c.execute("insert into accounts(name) values ('abandoned')")
        c.pgconn.finish()       # drop the socket without Terminate or commit

    def check():
        with psycopg.connect(dsn(proxy)) as c:
            return c.execute("select count(*) from accounts where name='abandoned'").fetchone()[0]

    await run(abandon)
    await asyncio.sleep(0.3)
    assert await run(check) == 0


async def test_concurrent_clients(proxy):
    def one_client(n):
        with psycopg.connect(dsn(proxy)) as c:
            for i in range(20):
                assert c.execute("select %s + %s", (n, i)).fetchone()[0] == n + i
        return True

    def many():
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(8) as ex:
            return list(ex.map(one_client, range(8)))
    assert all(await run(many))


async def test_pool_exhaustion_gives_clean_error():
    settings = Settings(
        listen_host="127.0.0.1", listen_port=0, client_password=PW,
        primary_host="127.0.0.1", primary_port=PRIMARY_PORT,
        backend_user="postgres", backend_password=PW,
        pool_max_size=1, pool_acquire_timeout=0.5,
    )
    server = ProxyServer(settings)
    await server.start()
    try:
        def work():
            holder = psycopg.connect(dsn(server))
            holder.execute("select 1")
            with pytest.raises(psycopg.OperationalError) as e:
                psycopg.connect(dsn(server))
            holder.close()
            return str(e.value)
        assert "no free connection" in await run(work)
    finally:
        await server.stop()