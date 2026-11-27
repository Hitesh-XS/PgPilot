"""One ClientHandler per client connection: handshake, then a two-way message pump."""
from __future__ import annotations

import asyncio
import hmac
import logging
import struct

from proxy.config import Settings
from proxy.pool.backend import BackendConn, BackendError
from proxy.pool.pool import Pool, PoolTimeout
from proxy.protocol import messages as m
from proxy.protocol.codec import read_message, read_startup, send
from proxy.protocol.messages import BE, FE, Message, ProtocolError
from proxy.session.registry import SessionRegistry

log = logging.getLogger("pgpilot.session")

# Client messages that each produce exactly one ReadyForQuery from the backend.
_CYCLE_ENDERS = (FE.QUERY, FE.SYNC, b"F")  # b"F" = FunctionCall


class ClientHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 settings: Settings, pool: Pool, registry: SessionRegistry):
        self.reader = reader
        self.writer = writer
        self.settings = settings
        self.pool = pool
        self.registry = registry
        self.backend: BackendConn | None = None
        self.pending_cycles = 0        # client cycles not yet answered by ReadyForQuery
        self.clean_terminate = False

    # ----------------------------------------------------------------- run
    async def run(self) -> None:
        try:
            info = await self._handshake()
            if info is not None:
                await self._serve(*info)
        except (asyncio.IncompleteReadError, ConnectionError, ProtocolError) as e:
            log.debug("client connection ended: %r", e)
        finally:
            self.writer.close()

    async def _fatal(self, text: str, code: str) -> None:
        try:
            await send(self.writer, m.error_response(text, code, severity="FATAL"))
        except OSError:
            pass

    # ----------------------------------------------------------- handshake
    async def _handshake(self):
        startup = await read_startup(self.reader)
        while startup.code in (m.SSL_REQUEST, m.GSSENC_REQUEST):
            self.writer.write(b"N")            # no TLS/GSS (yet): client continues in plaintext
            await self.writer.drain()
            startup = await read_startup(self.reader)

        if startup.code == m.CANCEL_REQUEST:
            if len(startup.data) >= 8:
                pid, secret = struct.unpack("!II", startup.data[:8])
                await self.registry.cancel(pid, secret)
            return None

        major, minor = startup.code >> 16, startup.code & 0xFFFF
        if major != 3:
            await self._fatal("unsupported frontend protocol", "0A000")
            return None

        params = m.parse_startup_params(startup.data)
        unrecognized = [k for k in params if k.startswith("_pq_.")]
        for k in unrecognized:
            del params[k]
        if minor != 0 or unrecognized:
            payload = struct.pack("!II", 0, len(unrecognized)) + b"".join(
                k.encode() + b"\0" for k in unrecognized)
            await send(self.writer, Message(b"v", payload))   # NegotiateProtocolVersion

        user = params.pop("user", None)
        if not user:
            await self._fatal("no PostgreSQL user name specified in startup packet", "28000")
            return None
        if "replication" in params:
            await self._fatal("replication connections are not supported by the proxy", "0A000")
            return None
        database = params.pop("database", user)

        # Authenticate the client ourselves (cleartext for now; SCRAM-to-client comes later).
        await send(self.writer, m.auth_cleartext())
        reply = await read_message(self.reader)
        if reply.type != FE.PASSWORD:
            raise ProtocolError("expected password message")
        given = reply.payload.split(b"\0", 1)[0]
        if not hmac.compare_digest(given, self.settings.client_password.encode()):
            await self._fatal(f'password authentication failed for user "{user}"', "28P01")
            return None
        return user, database, params

    # --------------------------------------------------------------- serve
    async def _serve(self, user: str, database: str, params: dict[str, str]) -> None:
        try:
            self.backend = await self.pool.acquire(database, params)
        except PoolTimeout as e:
            await self._fatal(str(e), "53300")
            return
        except BackendError as e:
            await self._fatal(e.fields.get("M", "backend connection failed"),
                              e.fields.get("C", "08006"))
            return
        except (OSError, TimeoutError, ProtocolError) as e:
            await self._fatal(f"could not connect to backend: {e}", "08001")
            return

        backend = self.backend
        key = self.registry.register(backend)
        reusable = False
        try:
            await send(
                self.writer,
                m.auth_ok(),
                *backend.param_status,
                m.backend_key_data(*key),
                m.ready_for_query(b"I"),
            )
            log.debug("session start user=%s db=%s backend_pid=%s", user, database, backend.pid)
            await self._pump(backend)
            reusable = self.clean_terminate and self.pending_cycles == 0 and backend.status == b"I"
        finally:
            self.registry.unregister(key)
            await self.pool.release(backend, reusable)
            self.backend = None

    async def _pump(self, backend: BackendConn) -> None:
        tasks = [
            asyncio.create_task(self._client_to_backend(backend)),
            asyncio.create_task(self._backend_to_client(backend)),
        ]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        for t in done:
            exc = t.exception() if not t.cancelled() else None
            if exc and not isinstance(exc, (asyncio.IncompleteReadError, ConnectionError)):
                log.warning("pump error: %r", exc)

    async def _client_to_backend(self, backend: BackendConn) -> None:
        while True:
            msg = await read_message(self.reader)
            if msg.type == FE.TERMINATE:
                self.clean_terminate = True
                return
            if msg.type in _CYCLE_ENDERS:
                self.pending_cycles += 1
            backend.writer.write(msg.raw)
            await backend.writer.drain()

    async def _backend_to_client(self, backend: BackendConn) -> None:
        while True:
            msg = await read_message(backend.reader)
            self.writer.write(msg.raw)
            if msg.type == BE.READY_FOR_QUERY:
                backend.status = msg.payload
                if self.pending_cycles > 0:
                    self.pending_cycles -= 1
            await self.writer.drain()