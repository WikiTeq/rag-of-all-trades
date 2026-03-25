import json

from cryptography.fernet import Fernet

from utils.config import settings


def encrypt_secret(data: dict) -> str:
    """Encrypt a dict to a Fernet token string. Never log the result."""
    key = settings.env.CONNECTOR_ENCRYPTION_KEY.encode()
    f = Fernet(key)
    return f.encrypt(json.dumps(data).encode()).decode()


def decrypt_secret(token: str) -> dict:
    """Decrypt a Fernet token string to a dict. Never log the returned dict."""
    if not token:
        raise ValueError("decrypt_secret called with empty token")
    key = settings.env.CONNECTOR_ENCRYPTION_KEY.encode()
    f = Fernet(key)
    try:
        return json.loads(f.decrypt(token.encode()).decode())
    except Exception as exc:
        raise ValueError(f"Failed to decrypt secret: {exc}") from exc
