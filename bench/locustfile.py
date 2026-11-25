"""Load + consistency harness. Targets TARGET_PORT (default: primary).
Later we point TARGET_PORT at the proxy.
Run: locust -f bench/locustfile.py --headless -u 20 -r 5 -t 30s
"""
import os
import random
import time

import psycopg
from dotenv import load_dotenv
from locust import User, between, events, task

load_dotenv()
PORT = int(os.getenv("TARGET_PORT", os.getenv("PRIMARY_PORT", 5433)))
DSN = (f"host=localhost port={PORT} dbname=app user=postgres "
       f"password={os.getenv('PG_PASSWORD', 'postgres')}")


class PgUser(User):
    wait_time = between(0.01, 0.1)

    def on_start(self):
        self.conn = psycopg.connect(DSN, autocommit=True)

    def on_stop(self):
        self.conn.close()

    def _timed(self, name, fn):
        t0 = time.perf_counter()
        exc = None
        try:
            fn()
        except Exception as e:
            exc = e
        events.request.fire(
            request_type="PG", name=name,
            response_time=(time.perf_counter() - t0) * 1000,
            response_length=0, exception=exc, context={},
        )

    @task(8)
    def point_read(self):
        i = random.randint(1, 1000)
        self._timed("point_read", lambda: self.conn.execute(
            "select * from accounts where id=%s", (i,)).fetchone())

    @task(2)
    def report_query(self):
        self._timed("report_query", lambda: self.conn.execute(
            "select account_id, sum(amount) from orders "
            "group by account_id order by 2 desc limit 20").fetchall())

    @task(2)
    def write_then_read(self):
        """Read-your-writes check: a session must always see its own update."""
        i = random.randint(1, 1000)
        marker = random.randint(1, 10**9)

        def run():
            self.conn.execute("update accounts set balance=%s where id=%s", (marker, i))
            got = self.conn.execute(
                "select balance from accounts where id=%s", (i,)).fetchone()[0]
            if int(got) != marker:
                raise AssertionError(f"RYW violation: wrote {marker}, read {got}")

        self._timed("write_then_read", run)