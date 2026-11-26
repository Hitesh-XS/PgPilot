import asyncio
import struct

import pytest

from proxy.protocol import messages as m
from proxy.protocol.codec import read_message, read_startup


def make_reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


async def test_message_roundtrip():
    msg = m.Message(b"Q", b"select 1\0")
    got = await read_message(make_reader(msg.raw))
    assert got == msg


async def test_reads_two_messages_in_sequence():
    a = m.Message(b"Q", b"select 1\0")
    b = m.Message(b"X", b"")
    reader = make_reader(a.raw + b.raw)
    assert await read_message(reader) == a
    assert await read_message(reader) == b


async def test_empty_payload_message():
    got = await read_message(make_reader(b"S\x00\x00\x00\x04"))
    assert got == m.Message(b"S", b"")


async def test_eof_mid_message_raises():
    raw = m.Message(b"Q", b"select 1\0").raw
    with pytest.raises(asyncio.IncompleteReadError):
        await read_message(make_reader(raw[:-3]))


async def test_invalid_length_rejected():
    with pytest.raises(m.ProtocolError):
        await read_message(make_reader(b"Q\x00\x00\x00\x02"))


def test_ready_for_query_bytes():
    assert m.ready_for_query(b"I").raw == b"Z\x00\x00\x00\x05I"
    assert m.ready_for_query(b"T").raw == b"Z\x00\x00\x00\x05T"


def test_auth_ok_bytes():
    assert m.auth_ok().raw == b"R\x00\x00\x00\x08\x00\x00\x00\x00"


async def test_startup_message_roundtrip():
    raw = m.startup_message({"user": "alice", "database": "app"})
    packet = await read_startup(make_reader(raw))
    assert packet.code == m.PROTOCOL_V3
    assert m.parse_startup_params(packet.data) == {"user": "alice", "database": "app"}


async def test_ssl_request_detected():
    packet = await read_startup(make_reader(m.ssl_request()))
    assert packet.code == m.SSL_REQUEST
    assert packet.data == b""


async def test_cancel_request_parsed():
    packet = await read_startup(make_reader(m.cancel_request(1234, 5678)))
    assert packet.code == m.CANCEL_REQUEST
    assert struct.unpack("!II", packet.data) == (1234, 5678)


async def test_oversized_startup_rejected():
    with pytest.raises(m.ProtocolError):
        await read_startup(make_reader(struct.pack("!I", 5_000_000)))


def test_error_response_fields():
    msg = m.error_response("boom", code="08006")
    fields = m.parse_error_fields(msg.payload)
    assert fields["M"] == "boom"
    assert fields["C"] == "08006"
    assert fields["S"] == "ERROR"
    assert msg.type == m.BE.ERROR


def test_backend_key_data_roundtrip():
    msg = m.backend_key_data(42, 4_000_000_000)
    assert m.parse_backend_key_data(msg.payload) == (42, 4_000_000_000)


def test_parameter_status_payload():
    assert m.parameter_status("client_encoding", "UTF8").payload == b"client_encoding\0UTF8\0"


def test_malformed_startup_params_rejected():
    with pytest.raises(m.ProtocolError):
        m.parse_startup_params(b"user\0alice\0database\0\0")