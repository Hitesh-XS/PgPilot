"""SCRAM-SHA-256 client (RFC 5802 / RFC 7677), as used by PostgreSQL.

Pure functions and one small class, no I/O. Passwords are assumed to be ASCII
(no SASLprep normalisation), which is fine for service credentials.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os


class ScramError(Exception):
    pass


def _hmac(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


class ScramClient:
    def __init__(self, password: str, username: str = "", nonce: str | None = None):
        # PostgreSQL ignores the SCRAM username and uses the startup user, so "" is fine.
        self._password = password.encode()
        self._nonce = nonce or base64.b64encode(os.urandom(18)).decode()
        self._first_bare = f"n={username},r={self._nonce}"
        self._server_signature: bytes | None = None

    def first_message(self) -> bytes:
        return ("n,," + self._first_bare).encode()

    def final_message(self, server_first: bytes) -> bytes:
        text = server_first.decode()
        attrs = dict(p.split("=", 1) for p in text.split(","))
        server_nonce, salt_b64, iterations = attrs["r"], attrs["s"], int(attrs["i"])
        if not server_nonce.startswith(self._nonce):
            raise ScramError("server nonce does not extend client nonce")

        salted = hashlib.pbkdf2_hmac(
            "sha256", self._password, base64.b64decode(salt_b64), iterations
        )
        client_key = _hmac(salted, b"Client Key")
        stored_key = hashlib.sha256(client_key).digest()
        without_proof = f"c=biws,r={server_nonce}"
        auth_message = f"{self._first_bare},{text},{without_proof}".encode()

        client_sig = _hmac(stored_key, auth_message)
        proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
        self._server_signature = _hmac(_hmac(salted, b"Server Key"), auth_message)
        return f"{without_proof},p={base64.b64encode(proof).decode()}".encode()

    def verify_server(self, server_final: bytes) -> None:
        text = server_final.decode()
        if text.startswith("e="):
            raise ScramError(f"server rejected authentication: {text[2:]}")
        if not text.startswith("v="):
            raise ScramError("malformed server-final message")
        got = base64.b64decode(text[2:])
        if self._server_signature is None or not hmac.compare_digest(got, self._server_signature):
            raise ScramError("server signature mismatch")