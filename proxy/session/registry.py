"""Maps the fake (pid, secret) we hand to clients onto real backend connections."""
from __future__ import annotations

import secrets

from proxy.pool.backend import BackendConn


class SessionRegistry:
    def __init__(self) -> None:
        self._next_pid = 1
        self._sessions: dict[tuple[int, int], BackendConn] = {}

    def register(self, backend: BackendConn) -> tuple[int, int]:
        key = (self._next_pid, secrets.randbits(31))
        self._next_pid += 1
        self._sessions[key] = backend
        return key

    def unregister(self, key: tuple[int, int]) -> None:
        self._sessions.pop(key, None)

    async def cancel(self, pid: int, secret: int) -> bool:
        backend = self._sessions.get((pid, secret))
        if backend is None:
            return False
        await backend.cancel()
        return True

    def __len__(self) -> int:
        return len(self._sessions)