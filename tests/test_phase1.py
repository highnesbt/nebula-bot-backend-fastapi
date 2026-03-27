"""Phase 1 tests — models, auth, and API endpoints."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from passlib.hash import django_pbkdf2_sha256
from app.auth import create_access_token, hash_password, verify_password
from app.main import app
from app.models.user import BrokerCredential, User, encrypt_value, decrypt_value
from sqlalchemy.ext.asyncio import AsyncSession
from httpx import AsyncClient


# ── Password hashing tests ───────────────────────────────────────────────────


class TestPasswordHashing:
    def test_hash_and_verify(self):
        pw = "secret123"
        hashed = hash_password(pw)
        assert hashed != pw
        assert verify_password(pw, hashed)

    def test_wrong_password(self):
        hashed = hash_password("correct")
        assert not verify_password("wrong", hashed)

    def test_verify_migrated_django_password(self):
        hashed = django_pbkdf2_sha256.hash("legacy-password")
        assert verify_password("legacy-password", hashed)


# ── Encryption tests ─────────────────────────────────────────────────────────


class TestEncryption:
    def test_encrypt_decrypt_roundtrip(self):
        original = "my-api-key-12345"
        encrypted = encrypt_value(original)
        assert encrypted != original
        assert decrypt_value(encrypted) == original

    def test_empty_string(self):
        assert encrypt_value("") == ""
        assert decrypt_value("") == ""


# ── JWT tests ─────────────────────────────────────────────────────────────────


class TestJWT:
    def test_create_and_decode_token(self):
        from app.auth import decode_token

        token = create_access_token(42)
        payload = decode_token(token)
        assert payload["sub"] == "42"
        assert payload["type"] == "access"

    def test_invalid_token_raises(self):
        from app.auth import decode_token
        from fastapi import HTTPException

        with pytest.raises(HTTPException):
            decode_token("invalid.token.here")


# ── User model tests ─────────────────────────────────────────────────────────


class TestUserModel:
    @pytest.mark.asyncio
    async def test_create_user(self, db_session: AsyncSession):
        user = User(
            username="alice",
            email="alice@test.com",
            hashed_password=hash_password("pw"),
        )
        db_session.add(user)
        await db_session.commit()
        await db_session.refresh(user)

        assert user.id is not None
        assert user.username == "alice"
        assert user.is_active is True

    @pytest.mark.asyncio
    async def test_broker_credential_encryption(self, db_session: AsyncSession, test_user: User):
        cred = BrokerCredential(user_id=test_user.id)
        cred.api_key = "ABCDEF123456"
        cred.client_id = "S12345"
        cred.password = "secretpw"
        cred.totp_secret = "JBSWY3DPEH"
        db_session.add(cred)
        await db_session.commit()
        await db_session.refresh(cred)

        assert cred._api_key != "ABCDEF123456"
        assert cred.api_key == "ABCDEF123456"
        assert cred.client_id == "S12345"
        assert cred.password == "secretpw"
        assert cred.totp_secret == "JBSWY3DPEH"


# ── Login API tests ──────────────────────────────────────────────────────────


class TestLoginAPI:
    @pytest.mark.asyncio
    async def test_login_success(self, client: AsyncClient, test_user: User):
        resp = await client.post(
            "/api/auth/login",
            json={"username": "testuser", "password": "testpassword123"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "access" in data
        assert "refresh" in data

    @pytest.mark.asyncio
    async def test_login_wrong_password(self, client: AsyncClient, test_user: User):
        resp = await client.post(
            "/api/auth/login",
            json={"username": "testuser", "password": "wrong"},
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_login_nonexistent_user(self, client: AsyncClient):
        resp = await client.post(
            "/api/auth/login",
            json={"username": "nobody", "password": "pw"},
        )
        assert resp.status_code == 401


# ── /me API tests ─────────────────────────────────────────────────────────────


class TestMeAPI:
    @pytest.mark.asyncio
    async def test_me_authenticated(self, client: AsyncClient, auth_headers: dict):
        resp = await client.get("/api/auth/me", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == "testuser"

    @pytest.mark.asyncio
    async def test_me_no_token(self, client: AsyncClient):
        resp = await client.get("/api/auth/me")
        assert resp.status_code == 403


# ── Broker credentials API tests ─────────────────────────────────────────────


class TestBrokerCredentialsAPI:
    @pytest.mark.asyncio
    async def test_save_and_get_credentials(
        self, client: AsyncClient, auth_headers: dict
    ):
        resp = await client.post(
            "/api/auth/broker/credentials",
            json={
                "api_key": "MYAPIKEY123",
                "client_id": "S99999",
                "password": "topsecret",
                "totp_secret": "JBSWY3DPEH",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 200
        assert "saved" in resp.json()["message"]

        resp = await client.get(
            "/api/auth/broker/credentials", headers=auth_headers
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "****" in data["api_key"] or "*" in data["api_key"]

    @pytest.mark.asyncio
    async def test_delete_credentials(
        self, client: AsyncClient, auth_headers: dict
    ):
        await client.post(
            "/api/auth/broker/credentials",
            json={
                "api_key": "KEY",
                "client_id": "CID",
                "password": "PW",
                "totp_secret": "TS",
            },
            headers=auth_headers,
        )
        resp = await client.delete(
            "/api/auth/broker/credentials", headers=auth_headers
        )
        assert resp.status_code == 200

        resp = await client.get(
            "/api/auth/broker/credentials", headers=auth_headers
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_credentials_not_found(
        self, client: AsyncClient, auth_headers: dict
    ):
        resp = await client.get(
            "/api/auth/broker/credentials", headers=auth_headers
        )
        assert resp.status_code == 404


class TestWebSocketAuth:
    def test_notifications_ws_requires_valid_token(self):
        client = TestClient(app)
        with pytest.raises(Exception):
            with client.websocket_connect("/ws/notifications?token=invalid.token"):
                pass

    def test_notifications_ws_accepts_valid_token(self):
        client = TestClient(app)
        with patch("app.routers.ws.get_user_from_token", AsyncMock(return_value=object())):
            token = create_access_token(1)
            with client.websocket_connect(f"/ws/notifications?token={token}") as ws:
                ws.send_text("ping")
