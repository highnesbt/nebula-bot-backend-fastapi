"""Broker client factory — returns live, paper, or mock client."""

from app.config import settings


def get_broker_client(api_key: str = ""):
    """Return the appropriate broker client based on NEBULA_BROKER_BACKEND."""
    backend = settings.NEBULA_BROKER_BACKEND

    if backend == "live":
        from app.broker.smart_client import AngelOneClient
        return AngelOneClient(api_key=api_key)
    elif backend == "paper":
        from app.broker.paper_client import PaperTradingClient
        return PaperTradingClient(api_key=api_key)
    else:
        from app.broker.mock_client import MockBrokerClient
        return MockBrokerClient(api_key=api_key)
