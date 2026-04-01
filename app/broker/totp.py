"""Helpers for normalizing and validating broker TOTP secrets."""

from urllib.parse import parse_qs, urlparse

import pyotp

_BASE32_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567=")


def normalize_totp_secret(value: str) -> str:
    """Return a normalized base32 TOTP secret or raise ValueError."""
    candidate = (value or "").strip()
    if not candidate:
        raise ValueError("TOTP secret is required.")

    if candidate.lower().startswith("otpauth://"):
        parsed = urlparse(candidate)
        secrets = parse_qs(parsed.query).get("secret", [])
        if not secrets or not secrets[0].strip():
            raise ValueError("TOTP URI is missing a secret parameter.")
        candidate = secrets[0]

    candidate = candidate.replace(" ", "").replace("-", "").upper()
    invalid_chars = sorted({char for char in candidate if char not in _BASE32_CHARS})
    if invalid_chars:
        raise ValueError("TOTP secret must use only A-Z and digits 2-7.")

    try:
        pyotp.TOTP(candidate).now()
    except Exception as exc:
        raise ValueError(f"Invalid TOTP secret: {exc}") from exc

    return candidate
