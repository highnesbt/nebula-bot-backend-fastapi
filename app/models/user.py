"""User and BrokerCredential models — ported from Django accounts app."""

from datetime import datetime, timezone

from cryptography.fernet import Fernet
from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.config import settings
from app.database import Base


# ── Fernet encryption helpers ─────────────────────────────────────────────────


def _get_fernet() -> Fernet:
    key = settings.NEBULA_ENCRYPTION_KEY.encode()
    return Fernet(key)


def encrypt_value(value: str) -> str:
    if not value:
        return value
    return _get_fernet().encrypt(value.encode()).decode()


def decrypt_value(value: str) -> str:
    if not value:
        return value
    return _get_fernet().decrypt(value.encode()).decode()


# ── Models ────────────────────────────────────────────────────────────────────


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(150), unique=True, index=True)
    email: Mapped[str] = mapped_column(String(254), default="")
    hashed_password: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    broker_credential: Mapped["BrokerCredential"] = relationship(
        back_populates="user", uselist=False, lazy="selectin"
    )
    config: Mapped["GlobalConfig"] = relationship(
        back_populates="user", uselist=False, lazy="selectin"
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} username={self.username}>"


class BrokerCredential(Base):
    """Angel One API credentials — encrypted at rest with Fernet."""

    __tablename__ = "broker_credentials"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), unique=True)

    # All sensitive fields stored encrypted
    _api_key: Mapped[str] = mapped_column("api_key", Text, default="")
    _client_id: Mapped[str] = mapped_column("client_id", Text, default="")
    _password: Mapped[str] = mapped_column("password", Text, default="")
    _totp_secret: Mapped[str] = mapped_column("totp_secret", Text, default="")

    # Cached session token (not encrypted — rotates frequently)
    jwt_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    feed_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_token: Mapped[str | None] = mapped_column(Text, nullable=True)

    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationship
    user: Mapped["User"] = relationship(back_populates="broker_credential")

    # ── Encrypted property accessors ──────────────────────────────────────

    @property
    def api_key(self) -> str:
        return decrypt_value(self._api_key) if self._api_key else ""

    @api_key.setter
    def api_key(self, value: str):
        self._api_key = encrypt_value(value)

    @property
    def client_id(self) -> str:
        return decrypt_value(self._client_id) if self._client_id else ""

    @client_id.setter
    def client_id(self, value: str):
        self._client_id = encrypt_value(value)

    @property
    def password(self) -> str:
        return decrypt_value(self._password) if self._password else ""

    @password.setter
    def password(self, value: str):
        self._password = encrypt_value(value)

    @property
    def totp_secret(self) -> str:
        return decrypt_value(self._totp_secret) if self._totp_secret else ""

    @totp_secret.setter
    def totp_secret(self, value: str):
        self._totp_secret = encrypt_value(value)

    def __repr__(self) -> str:
        return f"<BrokerCredential user_id={self.user_id}>"


# Forward reference fix — import after Base is defined
from app.models.stock import GlobalConfig  # noqa: E402, F401
