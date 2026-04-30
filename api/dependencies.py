import secrets

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from utils.config import settings

_bearer = HTTPBearer(auto_error=False)


def require_api_key(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> None:
    """No-op when API_KEY is not configured. Returns 401 when key is wrong or missing."""
    expected = settings.env.API_KEY
    if not expected:
        return
    if credentials is None or not secrets.compare_digest(expected, credentials.credentials):
        raise HTTPException(status_code=401, detail="Unauthorized")
