"""Postgres v3 wire-protocol constants, message type, and message builders.

Every regular message is: 1 type byte + int32 length (includes itself) + payload.
Startup-phase packets have no type byte (see codec.read_startup).
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

PROTOCOL_V3 = 196608  # 3.0
SSL_REQUEST = 80877103
GSSENC_REQUEST = 80877104
CANCEL_REQUEST = 80877102

# Authentication request codes (payload of an 'R' message)
AUTH_OK = 0
AUTH_CLEARTEXT = 3
AUTH_MD5 = 5
AUTH_SASL = 10
AUTH_SASL_CONTINUE = 11
AUTH_SASL_FINAL = 12


class FE:
    """Frontend (client -> server) message types."""
    QUERY = b"Q"
    PARSE = b"P"
    BIND = b"B"
    EXECUTE = b"E"
    DESCRIBE = b"D"
    CLOSE = b"C"
    SYNC = b"S"
    FLUSH = b"H"
    TERMINATE = b"X"
    PASSWORD = b"p"  # also SASLInitialResponse / SASLResponse
    COPY_DATA = b"d"
    COPY_DONE = b"c"
    COPY_FAIL = b"f"


class BE:
    """Backend (server -> client) message types."""
    AUTH = b"R"
    PARAMETER_STATUS = b"S"
    BACKEND_KEY_DATA = b"K"
    READY_FOR_QUERY = b"Z"
    ERROR = b"E"
    NOTICE = b"N"
    COMMAND_COMPLETE = b"C"
    DATA_ROW = b"D"
    ROW_DESCRIPTION = b"T"


class ProtocolError(Exception):
    """The peer sent something that violates the wire protocol."""


@dataclass(frozen=True, slots=True)
class Message:
    type: bytes      # exactly one byte
    payload: bytes

    @property
    def raw(self) -> bytes:
        """The exact bytes to put on the wire."""
        return self.type + struct.pack("!I", len(self.payload) + 4) + self.payload


@dataclass(frozen=True, slots=True)
class Startup:
    """A startup-phase packet: StartupMessage, SSLRequest, CancelRequest, ..."""
    code: int
    data: bytes  # everything after the 4-byte code


# ---------------------------------------------------------------- builders

def auth_request(code: int, extra: bytes = b"") -> Message:
    return Message(BE.AUTH, struct.pack("!I", code) + extra)


def auth_ok() -> Message:
    return auth_request(AUTH_OK)


def auth_cleartext() -> Message:
    return auth_request(AUTH_CLEARTEXT)


def parameter_status(name: str, value: str) -> Message:
    return Message(BE.PARAMETER_STATUS, name.encode() + b"\0" + value.encode() + b"\0")


def backend_key_data(pid: int, secret: int) -> Message:
    return Message(BE.BACKEND_KEY_DATA, struct.pack("!II", pid, secret))


def ready_for_query(status: bytes = b"I") -> Message:
    """status: b'I' idle, b'T' in transaction, b'E' failed transaction."""
    return Message(BE.READY_FOR_QUERY, status)


def error_response(message: str, code: str = "XX000", severity: str = "ERROR") -> Message:
    fields = (
        b"S" + severity.encode() + b"\0"
        + b"V" + severity.encode() + b"\0"
        + b"C" + code.encode() + b"\0"
        + b"M" + message.encode() + b"\0"
        + b"\0"
    )
    return Message(BE.ERROR, fields)


def startup_message(params: dict[str, str]) -> bytes:
    """Build a protocol 3.0 StartupMessage (used toward Postgres backends)."""
    body = struct.pack("!I", PROTOCOL_V3)
    for k, v in params.items():
        body += k.encode() + b"\0" + v.encode() + b"\0"
    body += b"\0"
    return struct.pack("!I", len(body) + 4) + body


def cancel_request(pid: int, secret: int) -> bytes:
    return struct.pack("!IIII", 16, CANCEL_REQUEST, pid, secret)


def ssl_request() -> bytes:
    return struct.pack("!II", 8, SSL_REQUEST)


# ----------------------------------------------------------------- parsers

def parse_startup_params(data: bytes) -> dict[str, str]:
    """Parse the key/value section of a StartupMessage (after the version int)."""
    parts = data.split(b"\0")
    # Trailing terminator yields empty strings at the end.
    while parts and parts[-1] == b"":
        parts.pop()
    if len(parts) % 2 != 0:
        raise ProtocolError("malformed startup parameters")
    return {parts[i].decode(): parts[i + 1].decode() for i in range(0, len(parts), 2)}


def parse_error_fields(payload: bytes) -> dict[str, str]:
    """Parse an ErrorResponse/NoticeResponse payload into {field_code: text}."""
    out: dict[str, str] = {}
    i = 0
    while i < len(payload) and payload[i] != 0:
        code = chr(payload[i])
        end = payload.index(b"\0", i + 1)
        out[code] = payload[i + 1:end].decode(errors="replace")
        i = end + 1
    return out


def parse_backend_key_data(payload: bytes) -> tuple[int, int]:
    pid, secret = struct.unpack("!II", payload)
    return pid, secret