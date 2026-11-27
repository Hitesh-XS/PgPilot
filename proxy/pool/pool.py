"""Connection pool toward one Postgres target (the primary, for now)."""
from __future__ import annotations

import asyncio
import logging

from proxy.pool.backend import BackendConn

log = logging.getLogger("pgpilot.pool")


class PoolTimeout(Exception):
    pass


class Pool:
    """Holds at most `max_size` backend connections (idle + leased).

    Connections are keyed by (database, startup params) so a reused connection
    always matches what the new client asked for.
    """

    def __init__(self, name: str, host: str, port: int, user: str, password: str,
                 max_size: int = 20, acquire_timeout: float = 5.0):
        self.name = name
        self.host, self.port, self.user, self.password = host, port, user, password
        self.max_size = max_size
        self.acquire_timeout = acquire_timeout
        self._idle: dict[tuple, list[BackendConn]] = {}
        self._total = 0
        self._leased = 0
        self._cond = asyncio.Condition()

    def stats(self) -> dict[str, int]:
        idle = sum(len(v) for v in self._idle.values())
        return {"total": self._total, "idle": idle, "leased": self._leased}

    @staticmethod
    def make_key(database: str, params: dict[str, str]) -> tuple:
        return (database, tuple(sorted(params.items())))

    async def acquire(self, database: str, params: dict[str, str]) -> BackendConn:
        key = self.make_key(database, params)
        to_close: list[BackendConn] = []
        try:
            async with self._cond:
                try:
                    async with asyncio.timeout(self.acquire_timeout):
                        while True:
                            bucket = self._idle.get(key, [])
                            while bucket:
                                conn = bucket.pop()
                                if conn.closed:
                                    self._total -= 1
                                    continue
                                self._leased += 1
                                return conn
                            if self._total < self.max_size:
                                self._total += 1
                                break
                            victim = self._pop_any_idle()
                            if victim is not None:   # make room by dropping a mismatched idle conn
                                self._total -= 1
                                to_close.append(victim)
                                continue
                            await self._cond.wait()
                except TimeoutError:
                    raise PoolTimeout(f"no free connection to {self.name} within "
                                      f"{self.acquire_timeout}s") from None
        finally:
            for c in to_close:
                await c.close()

        try:
            conn = await BackendConn.connect(self.host, self.port, self.user,
                                             self.password, database, params, key)
        except BaseException:
            async with self._cond:
                self._total -= 1
                self._cond.notify()
            raise
        async with self._cond:
            self._leased += 1
        return conn

    def _pop_any_idle(self) -> BackendConn | None:
        for bucket in self._idle.values():
            if bucket:
                return bucket.pop()
        return None

    async def release(self, conn: BackendConn, reusable: bool) -> None:
        keep = reusable and not conn.closed and await conn.reset()
        if not keep:
            await conn.close()
        async with self._cond:
            self._leased -= 1
            if keep:
                self._idle.setdefault(conn.key, []).append(conn)
            else:
                self._total -= 1
            self._cond.notify()

    async def close(self) -> None:
        async with self._cond:
            conns = [c for bucket in self._idle.values() for c in bucket]
            self._total -= len(conns)
            self._idle.clear()
        for c in conns:
            await c.close()