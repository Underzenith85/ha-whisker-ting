"""Deterministic known-vector tests for pure Cognito SRP calculations."""

from __future__ import annotations

import datetime

from custom_components.whisker_ting.auth.srp import CognitoSRP


def _fixed_srp() -> CognitoSRP:
    """Build an SRP calculator with a deterministic private ephemeral value."""
    srp = CognitoSRP(
        "fixture-user",
        "fixture-password",
        pool_id="us-west-2_FIXTURE",
        client_id="fixture-client",
    )
    srp.small_a_value = int("123456789abcdef", 16)
    srp.large_a_value = srp._calculate_a()
    return srp


def test_password_key_matches_known_vector() -> None:
    """The derived authentication key remains byte-for-byte stable."""
    srp = _fixed_srp()
    server_b = srp.big_n - 1_234_567

    key = srp.get_password_authentication_key(
        "fixture-id", "fixture-password", server_b, "deadbeef"
    )

    assert key.hex() == "1879fc80f4b17167d0ee30008de5ba66"


def test_challenge_response_matches_known_vector() -> None:
    """The timestamped password-verifier signature remains stable."""
    srp = _fixed_srp()
    response = srp.process_challenge(
        {
            "USERNAME": "fixture-user",
            "USER_ID_FOR_SRP": "fixture-id",
            "SALT": "deadbeef",
            "SRP_B": format(srp.big_n - 1_234_567, "x"),
            "SECRET_BLOCK": "Zml4dHVyZS1zZWNyZXQ=",
        },
        srp.get_auth_params(),
        current_time=datetime.datetime(2026, 8, 22, 12, 34, 56, tzinfo=datetime.UTC),
    )

    assert response == {
        "TIMESTAMP": "Sat Aug 22 12:34:56 UTC 2026",
        "USERNAME": "fixture-user",
        "PASSWORD_CLAIM_SECRET_BLOCK": "Zml4dHVyZS1zZWNyZXQ=",
        "PASSWORD_CLAIM_SIGNATURE": "sDJsYGOSsJjDymvApL28POVKJOcmrhisf9Cs4rY4CpQ=",
    }
