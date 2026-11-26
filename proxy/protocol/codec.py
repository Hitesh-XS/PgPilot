"""Framing: read/write Postgres messages on asyncio streams."""
from __future__ import annotations

import asyncio
import struct

from proxy.protocol.messages import Message, ProtocolError, Startup

MAX_MESSAGE_SIZE = 0x3FFFFFFF   # Postgres' own ~1 GB limit
MAX_STARTUP_SIZE = 10_000       # startup packets are small; reject anything bigger


async def read_message(reader: asyncio.StreamReader) -> Message:
    """Read one regular message. Raises asyncio.IncompleteReadError on EOF."""
    header = await reader.readexactly(5)
    mtype = header[:1]
    (length,) = struct.unpack("!I", header[1:])
    if length < 4 or length > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"invalid message length {length}")
    payload = await reader.readexactly(length - 4)
    return Message(mtype, payload)


async def read_startup(reader: asyncio.StreamReader) -> Startup:
    """Read one startup-phase packet (no type byte)."""
    (length,) = struct.unpack("!I", await reader.readexactly(4))
    if length < 8 or length > MAX_STARTUP_SIZE:
        raise ProtocolError(f"invalid startup packet length {length}")
    body = await reader.readexactly(length - 4)
    (code,) = struct.unpack("!I", body[:4])
    return Startup(code=code, data=body[4:])


async def send(writer: asyncio.StreamWriter, *messages: Message) -> None:
    """Write messages and flush."""
    for m in messages:
        writer.write(m.raw)
    await writer.drain()