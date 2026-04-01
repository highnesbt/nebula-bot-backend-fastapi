"""Auth-related Pydantic schemas."""

from pydantic import BaseModel, field_validator

from app.broker.totp import normalize_totp_secret


class LoginIn(BaseModel):
    username: str
    password: str


class RefreshIn(BaseModel):
    refresh: str


class TokenOut(BaseModel):
    access: str
    refresh: str


class UserOut(BaseModel):
    id: int
    username: str
    email: str


class BrokerCredentialIn(BaseModel):
    api_key: str
    client_id: str
    password: str
    totp_secret: str

    @field_validator("totp_secret")
    @classmethod
    def validate_totp_secret(cls, value: str) -> str:
        return normalize_totp_secret(value)


class BrokerCredentialOut(BaseModel):
    api_key: str
    client_id: str
    password: str
    totp_secret: str
    has_session: bool
    updated_at: str | None = None


class MessageOut(BaseModel):
    message: str
