import hashlib
import hmac
import time
from typing import Union


def generate_webhook_signature(
    secret_key: str,
    payload: Union[str, bytes],
    timestamp: int | None = None,
) -> str:
    """
    Generates an HMAC-SHA256 signature header for webhook payloads.
    Includes timestamp protection against replay attacks (Stripe format).

    Format: t={timestamp},v1={hex_signature}
    """
    if timestamp is None:
        timestamp = int(time.time())

    if isinstance(payload, bytes):
        payload_str = payload.decode("utf-8")
    else:
        payload_str = str(payload)

    # Cryptographically bind timestamp to the payload: "{timestamp}.{payload}"
    signed_payload = f"{timestamp}.{payload_str}".encode("utf-8")
    secret_bytes = secret_key.encode("utf-8")

    signature = hmac.new(secret_bytes, signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"


def verify_webhook_signature(
    secret_key: str,
    signature_header: str,
    payload: Union[str, bytes],
    tolerance_seconds: int = 300,
) -> bool:
    """
    Verifies an incoming webhook signature against the secret key and payload.
    Rejects signatures older than tolerance_seconds (default 300s = 5 minutes).

    Uses hmac.compare_digest for constant-time comparison to prevent timing attacks.
    """
    if not signature_header or not secret_key:
        return False

    # 1. Parse header components: t={timestamp},v1={sig}
    timestamp: int | None = None
    signatures: list[str] = []

    try:
        parts = signature_header.split(",")
        for part in parts:
            if "=" not in part:
                continue
            key, val = part.strip().split("=", 1)
            if key == "t":
                timestamp = int(val)
            elif key == "v1":
                signatures.append(val)
    except (ValueError, TypeError):
        return False

    if timestamp is None or not signatures:
        return False

    # 2. Check for replay attacks: is timestamp within the allowed window?
    current_time = int(time.time())
    if abs(current_time - timestamp) > tolerance_seconds:
        return False

    # 3. Compute expected signature
    if isinstance(payload, bytes):
        payload_str = payload.decode("utf-8")
    else:
        payload_str = str(payload)

    signed_payload = f"{timestamp}.{payload_str}".encode("utf-8")
    secret_bytes = secret_key.encode("utf-8")
    expected_sig = hmac.new(secret_bytes, signed_payload, hashlib.sha256).hexdigest()

    # 4. Constant-time comparison against v1 signatures
    for sig in signatures:
        if hmac.compare_digest(sig, expected_sig):
            return True

    return False
