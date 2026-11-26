import base64

import pytest

from proxy.protocol.scram import ScramClient, ScramError

# Test vector from RFC 7677 section 3 (SCRAM-SHA-256).
USER = "user"
PASSWORD = "pencil"
CLIENT_NONCE = "rOprNGfwEbeRWgbNEkqO"
SERVER_FIRST = (
    b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
    b"s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
)
EXPECTED_CLIENT_FINAL = (
    b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
    b"p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
)
SERVER_FINAL = b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="


def make_client():
    return ScramClient(PASSWORD, username=USER, nonce=CLIENT_NONCE)


def test_first_message():
    assert make_client().first_message() == b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO"


def test_client_final_matches_rfc_vector():
    assert make_client().final_message(SERVER_FIRST) == EXPECTED_CLIENT_FINAL


def test_server_signature_verifies():
    c = make_client()
    c.final_message(SERVER_FIRST)
    c.verify_server(SERVER_FINAL)  # must not raise


def test_wrong_server_signature_rejected():
    c = make_client()
    c.final_message(SERVER_FIRST)
    bad = b"v=" + base64.b64encode(b"x" * 32)
    with pytest.raises(ScramError):
        c.verify_server(bad)


def test_server_error_reported():
    c = make_client()
    c.final_message(SERVER_FIRST)
    with pytest.raises(ScramError):
        c.verify_server(b"e=invalid-proof")


def test_server_nonce_must_extend_client_nonce():
    c = make_client()
    with pytest.raises(ScramError):
        c.final_message(b"r=otherNonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")


def test_wrong_password_gives_different_proof():
    good = make_client().final_message(SERVER_FIRST)
    bad = ScramClient("wrong", username=USER, nonce=CLIENT_NONCE).final_message(SERVER_FIRST)
    assert good != bad


def test_random_nonce_is_unique():
    assert ScramClient("x").first_message() != ScramClient("x").first_message()