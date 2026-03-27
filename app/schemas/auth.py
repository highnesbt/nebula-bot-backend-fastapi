"""Auth-related Pydantic schemas."""

from pydantic import BaseModel


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


class BrokerCredentialOut(BaseModel):
    api_key: str
    client_id: str
    password: str
    totp_secret: str
    has_session: bool
    updated_at: str | None = None


class MessageOut(BaseModel):
    message: str
