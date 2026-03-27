"""Auth router — login, refresh, me, broker credentials CRUD."""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    hash_password,
    verify_password,
)
from app.database import get_db
from app.models.user import BrokerCredential, User
from app.schemas.auth import (
    BrokerCredentialIn,
    BrokerCredentialOut,
    LoginIn,
    MessageOut,
    RefreshIn,
    TokenOut,
    UserOut,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


def _mask_value(value: str) -> str:
    """Show first 4 and last 2 chars, mask the rest."""
    if not value or len(value) <= 6:
        return "****"
    return value[:4] + "*" * (len(value) - 6) + value[-2:]


# ── Auth endpoints ────────────────────────────────────────────────────────────


@router.post("/login", response_model=TokenOut)
async def login(payload: LoginIn, db: AsyncSession = Depends(get_db)):
    """Authenticate and return JWT access + refresh tokens."""
    result = await db.execute(select(User).where(User.username == payload.username))
    user = result.scalar_one_or_none()

    if user is None or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    return TokenOut(
        access=create_access_token(user.id),
        refresh=create_refresh_token(user.id),
    )


@router.post("/refresh", response_model=TokenOut)
async def refresh_token(payload: RefreshIn):
    """Return a new access token from a refresh token."""
    token_data = decode_token(payload.refresh)
    if token_data.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
        )
    user_id = int(token_data["sub"])
    return TokenOut(
        access=create_access_token(user_id),
        refresh=create_refresh_token(user_id),
    )


@router.get("/me", response_model=UserOut)
async def me(user: User = Depends(get_current_user)):
    """Return the authenticated user's profile."""
    return UserOut(id=user.id, username=user.username, email=user.email)


# ── Broker Credential endpoints ──────────────────────────────────────────────


@router.post("/broker/credentials", response_model=MessageOut)
async def save_credentials(
    payload: BrokerCredentialIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Save or update Angel One broker credentials (encrypted)."""
    result = await db.execute(
        select(BrokerCredential).where(BrokerCredential.user_id == user.id)
    )
    cred = result.scalar_one_or_none()

    if cred:
        cred.api_key = payload.api_key
        cred.client_id = payload.client_id
        cred.password = payload.password
        cred.totp_secret = payload.totp_secret
        cred.updated_at = datetime.now(timezone.utc)
        verb = "updated"
    else:
        cred = BrokerCredential(user_id=user.id)
        cred.api_key = payload.api_key
        cred.client_id = payload.client_id
        cred.password = payload.password
        cred.totp_secret = payload.totp_secret
        cred.updated_at = datetime.now(timezone.utc)
        db.add(cred)
        verb = "saved"

    return MessageOut(message=f"Broker credentials {verb} successfully.")


@router.get("/broker/credentials")
async def get_credentials(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return masked broker credentials."""
    result = await db.execute(
        select(BrokerCredential).where(BrokerCredential.user_id == user.id)
    )
    cred = result.scalar_one_or_none()

    if cred is None:
        raise HTTPException(status_code=404, detail="No broker credentials found.")

    return BrokerCredentialOut(
        api_key=_mask_value(cred.api_key),
        client_id=_mask_value(cred.client_id),
        password=_mask_value(cred.password),
        totp_secret=_mask_value(cred.totp_secret),
        has_session=bool(cred.jwt_token),
        updated_at=str(cred.updated_at) if cred.updated_at else None,
    )


@router.delete("/broker/credentials", response_model=MessageOut)
async def delete_credentials(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete stored broker credentials."""
    result = await db.execute(
        select(BrokerCredential).where(BrokerCredential.user_id == user.id)
    )
    cred = result.scalar_one_or_none()

    if cred is None:
        raise HTTPException(status_code=404, detail="No broker credentials found.")

    await db.delete(cred)
    return MessageOut(message="Broker credentials deleted.")
