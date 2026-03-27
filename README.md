# Nebula v2 — FVG Intraday Trading Engine

FastAPI-based trading engine with tick-first candle architecture, robust order lifecycle management, and April 2026 SEBI/NSE compliance.

---

## Quick Start

### 1. Prerequisites

- **Python 3.11+** (required)
- **PostgreSQL** (production) or SQLite (dev/test)

### 2. Setup

```bash
cd nebula-backend

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Environment Variables

Copy the example and edit:

```bash
cp .env.example .env
```

Required variables in `.env`:

```env
# Database
DATABASE_URL=sqlite+aiosqlite:///./nebula.db          # Dev (SQLite)
# DATABASE_URL=postgresql+asyncpg://user:pass@host/nebula  # Production

# Auth
SECRET_KEY=your-secret-key-here
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_DAYS=30

# Encryption (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
NEBULA_ENCRYPTION_KEY=your-fernet-key-here

# Broker mode: live | paper | mock
NEBULA_BROKER_BACKEND=mock

# CORS
CORS_ALLOWED_ORIGINS=http://localhost:5173
```

### 4. Database Migration

```bash
# Generate initial migration
alembic revision --autogenerate -m "initial"

# Apply migrations
alembic upgrade head
```

### 5. Create Admin User

```bash
python -c "
import asyncio
from app.database import async_session_factory, Base, engine
from app.models.user import User
from app.auth import hash_password

async def create_user():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with async_session_factory() as db:
        user = User(username='admin', email='admin@nebula.local', hashed_password=hash_password('changeme'))
        db.add(user)
        await db.commit()
        print(f'Created user: admin (id={user.id})')

asyncio.run(create_user())
"
```

### 6. Run

```bash
# Development
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Production
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

> **Note:** Use `--workers 1` — the engine uses in-process state (APScheduler, tick evaluator).

### 7. Run Tests

```bash
pytest tests/ -v
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/login` | Login → JWT tokens |
| POST | `/api/auth/refresh` | Refresh access token |
| GET | `/api/auth/me` | Current user profile |
| POST | `/api/auth/broker/credentials` | Save broker creds (encrypted) |
| GET | `/api/auth/broker/credentials` | Get masked broker creds |
| DELETE | `/api/auth/broker/credentials` | Delete broker creds |
| GET | `/api/config/` | Get global config |
| PUT | `/api/config/` | Update global config |
| GET | `/api/watchlist/` | List watchlist |
| POST | `/api/watchlist/` | Add to watchlist |
| PUT | `/api/watchlist/{id}` | Update watchlist item |
| DELETE | `/api/watchlist/{id}` | Remove from watchlist |
| GET | `/api/search/` | Search stock symbols |
| GET | `/api/ltp/` | Get LTP for watchlist |
| GET | `/api/engine/status` | Engine status |
| POST | `/api/engine/start` | Start engine |
| POST | `/api/engine/stop` | Stop engine (kill switch) |
| POST | `/api/engine/exit-all` | Force exit all positions |
| GET | `/api/engine/gaps` | List FVG gaps |
| GET | `/api/engine/positions` | List open positions |
| GET | `/api/engine/positions/history` | Position history |
| GET | `/api/engine/signals` | Signal history |
| GET | `/api/engine/signals/{id}` | Signal detail |
| WS | `/ws/notifications` | Real-time events |
| WS | `/ws/market-data` | Real-time ticks |

---

## Architecture

```
nebula-backend/
├── app/
│   ├── main.py              # FastAPI app + lifespan
│   ├── config.py             # Pydantic settings
│   ├── database.py           # Async SQLAlchemy
│   ├── auth.py               # JWT + bcrypt
│   ├── models/               # SQLAlchemy models (9 tables)
│   ├── schemas/              # Pydantic request/response
│   ├── routers/              # API endpoints (auth, stock, engine, ws)
│   ├── engine/               # Core trading logic
│   │   ├── fvg_detector.py   # Fair Value Gap detection
│   │   ├── trend.py          # EMA-based trend detection
│   │   ├── atr.py            # Average True Range
│   │   ├── candle_builder.py # Tick → OHLC candle aggregation
│   │   ├── position_manager.py # ATR trailing stop loss
│   │   ├── tick_evaluator.py # Hot-path tick evaluation
│   │   ├── executor.py       # Order execution (PendingOrder-based)
│   │   ├── order_manager.py  # Order lifecycle state machine
│   │   ├── order_ws.py       # Angel One order-update WebSocket
│   │   ├── auto_exit.py      # Time-based exit (15:14 IST)
│   │   ├── broadcast.py      # WebSocket broadcast manager
│   │   ├── scheduler.py      # APScheduler setup
│   │   ├── rate_limiter.py   # Token-bucket (≤9 orders/sec)
│   │   └── pricing.py        # NSE tick-size rounding
│   └── broker/               # Broker client abstraction
│       ├── client.py         # Factory (live/paper/mock)
│       ├── smart_client.py   # Angel One SmartConnect wrapper
│       ├── paper_client.py   # Paper trading simulator
│       ├── mock_client.py    # Test mock
│       ├── constants.py      # API constants
│       └── exceptions.py     # Broker errors
├── alembic/                  # Database migrations
├── tests/                    # 67 tests across 4 test files
├── requirements.txt
└── .env
```

---

## SEBI April 2026 Compliance

| Regulation | Implementation |
|-----------|----------------|
| LIMIT orders only | `ORDER_TYPE_MARKET` removed from constants |
| ≤9 orders/sec | `OrderRateLimiter` (token-bucket) |
| Static IP | Deploy requirement (whitelist in Angel One) |
| Midnight re-auth | Scheduled job at 00:01 IST |
| OTR price band | Entry validated within ±0.75% of LTP |
| Kill switch | `/api/engine/stop` cancels all pending orders |
| Audit trail | `AuditLog` table (5-year immutable logging) |

---

## Broker Modes

| Mode | Setting | Behavior |
|------|---------|----------|
| **Live** | `NEBULA_BROKER_BACKEND=live` | Real Angel One API |
| **Paper** | `NEBULA_BROKER_BACKEND=paper` | Simulated fills in memory |
| **Mock** | `NEBULA_BROKER_BACKEND=mock` | Fixed responses for tests |

# nebula-bot-backend-fastapi
