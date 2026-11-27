"""TCP server: accepts clients and runs a ClientHandler for each."""
from __future__ import annotations

import asyncio
import logging

from proxy.config import Settings
from proxy.pool.pool import Pool
from proxy.protocol import messages as m
from proxy.protocol.codec import send
from proxy.session.handler import ClientHandler
from proxy.session.registry import SessionRegistry

log = logging.getLogger("pgpilot.server")


class ProxyServer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool = Pool("primary", settings.primary_host, settings.primary_port,
                         settings.backend_user, settings.backend_password,
                         settings.pool_max_size, settings.pool_acquire_timeout)
        self.registry = SessionRegistry()
        self.active = 0
        self.port = settings.listen_port
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._on_client, self.settings.listen_host, self.settings.listen_port)
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("PgPilot listening on %s:%s -> primary %s:%s",
                 self.settings.listen_host, self.port,
                 self.settings.primary_host, self.settings.primary_port)

    async def serve_forever(self) -> None:
        assert self._server is not None
        await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.pool.close()
        if self._server:
            await self._server.wait_closed()

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task:
            self._tasks.add(task)
        try:
            if self.active >= self.settings.max_client_connections:
                await send(writer, m.error_response("too many client connections", "53300", "FATAL"))
                writer.close()
                return
            self.active += 1
            try:
                await ClientHandler(reader, writer, self.settings, self.pool, self.registry).run()
            finally:
                self.active -= 1
        except asyncio.CancelledError:
            writer.close()
        except Exception:
            log.exception("unexpected error in client handler")
            writer.close()
        finally:
            if task:
                self._tasks.discard(task)