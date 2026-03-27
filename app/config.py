"""Nebula v2 — Application settings via Pydantic BaseSettings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Database ──────────────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql+asyncpg://nebula:nebula@localhost:5432/nebula"

    # ── Auth / JWT ────────────────────────────────────────────────────────────
    SECRET_KEY: str = "change-me-to-a-random-secret"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_DAYS: int = 30

    # ── Encryption (Fernet key for broker credentials) ────────────────────────
    NEBULA_ENCRYPTION_KEY: str = ""

    # ── Broker backend ────────────────────────────────────────────────────────
    NEBULA_BROKER_BACKEND: str = "paper"  # live | paper | mock

    # ── CORS ──────────────────────────────────────────────────────────────────
    CORS_ALLOWED_ORIGINS: str = "http://localhost:5173"

    # ── Timezone ──────────────────────────────────────────────────────────────
    TIMEZONE: str = "Asia/Kolkata"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.CORS_ALLOWED_ORIGINS.split(",")]


settings = Settings()
