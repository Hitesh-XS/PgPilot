"""A single connection from the proxy to a Postgres server."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import struct

from proxy.protocol import messages as m
from proxy.protocol.codec import read_message, send
from proxy.protocol.messages import BE, FE, Message, ProtocolError
from proxy.protocol.scram import ScramClient

log = logging.getLogger("pgpilot.backend")

CONNECT_TIMEOUT = 5.0


class BackendError(Exception):
    """Postgres answered with an ErrorResponse during startup/auth."""

    def __init__(self, fields: dict[str, str]):
        self.fields = fields
        super().__init__(fields.get("M", "backend error"))


def md5_password(user: str, password: str, salt: bytes) -> bytes:
    inner = hashlib.md5(password.encode() + user.encode()).hexdigest().encode()
    return b"md5" + hashlib.md5(inner + salt).hexdigest().encode()


class BackendConn:
    def __init__(self, host, port, reader, writer, pid, secret, param_status, key):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader = reader
        self.writer: asyncio.StreamWriter = writer
        self.pid = pid
        self.secret = secret
        self.param_status: list[Message] = param_status
        self.key = key               # pool key (database, params)
        self.status = b"I"           # last ReadyForQuery status byte

    # ------------------------------------------------------------ connect
    @classmethod
    async def connect(cls, host: str, port: int, user: str, password: str,
                      database: str, params: dict[str, str], key) -> "BackendConn":
        async with asyncio.timeout(CONNECT_TIMEOUT):
            reader, writer = await asyncio.open_connection(host, port)
        try:
            startup = {"user": user, "database": database, **params}
            writer.write(m.startup_message(startup))
            await writer.drain()
            pid = secret = 0
            param_status: list[Message] = []
            scram: ScramClient | None = None

            while True:
                msg = await read_message(reader)
                if msg.type == BE.AUTH:
                    (code,) = struct.unpack("!I", msg.payload[:4])
                    if code == m.AUTH_OK:
                        continue
                    elif code == m.AUTH_CLEARTEXT:
                        await send(writer, Message(FE.PASSWORD, password.encode() + b"\0"))
                    elif code == m.AUTH_MD5:
                        reply = md5_password(user, password, msg.payload[4:8]) + b"\0"
                        await send(writer, Message(FE.PASSWORD, reply))
                    elif code == m.AUTH_SASL:
                        mechanisms = msg.payload[4:].split(b"\0")
                        if b"SCRAM-SHA-256" not in mechanisms:
                            raise ProtocolError(f"no supported SASL mechanism in {mechanisms}")
                        scram = ScramClient(password)
                        first = scram.first_message()
                        body = b"SCRAM-SHA-256\0" + struct.pack("!i", len(first)) + first
                        await send(writer, Message(FE.PASSWORD, body))
                    elif code == m.AUTH_SASL_CONTINUE:
                        assert scram is not None
                        await send(writer, Message(FE.PASSWORD, scram.final_message(msg.payload[4:])))
                    elif code == m.AUTH_SASL_FINAL:
                        assert scram is not None
                        scram.verify_server(msg.payload[4:])
                    else:
                        raise ProtocolError(f"unsupported authentication method {code}")
                elif msg.type == BE.PARAMETER_STATUS:
                    param_status.append(msg)
                elif msg.type == BE.BACKEND_KEY_DATA:
                    pid, secret = m.parse_backend_key_data(msg.payload)
                elif msg.type == BE.READY_FOR_QUERY:
                    conn = cls(host, port, reader, writer, pid, secret, param_status, key)
                    conn.status = msg.payload
                    return conn
                elif msg.type == BE.ERROR:
                    raise BackendError(m.parse_error_fields(msg.payload))
                elif msg.type == BE.NOTICE:
                    continue
                else:
                    raise ProtocolError(f"unexpected message {msg.type!r} during startup")
        except BaseException:
            writer.close()
            raise

    # -------------------------------------------------------------- usage
    @property
    def closed(self) -> bool:
        return self.writer.is_closing() or self.reader.at_eof()

    async def cancel(self) -> None:
        """Ask Postgres to cancel the running query (new connection, per protocol)."""
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT):
                reader, writer = await asyncio.open_connection(self.host, self.port)
                writer.write(m.cancel_request(self.pid, self.secret))
                await writer.drain()
                await reader.read()          # Postgres closes the socket when done
                writer.close()
        except (OSError, TimeoutError) as e:
            log.warning("cancel request failed: %s", e)

    async def reset(self) -> bool:
        """Clear session state before the connection is reused. False = discard it."""
        if self.status != b"I":
            return False
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT):
                await send(self.writer, Message(FE.QUERY, b"DISCARD ALL\0"))
                while True:
                    msg = await read_message(self.reader)
                    if msg.type == BE.READY_FOR_QUERY:
                        self.status = msg.payload
                        return msg.payload == b"I"
                    if msg.type == BE.ERROR:
                        return False
        except (OSError, TimeoutError, asyncio.IncompleteReadError, ProtocolError):
            return False

    async def close(self) -> None:
        try:
            self.writer.write(Message(FE.TERMINATE, b"").raw)
            await self.writer.drain()
        except OSError:
            pass
        self.writer.close()