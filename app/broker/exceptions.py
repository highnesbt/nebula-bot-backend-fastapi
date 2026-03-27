"""Broker-related exceptions."""


class BrokerError(Exception):
    """Base exception for broker operations."""
    pass


class BrokerAuthError(BrokerError):
    """Authentication failed."""
    pass


class BrokerOrderError(BrokerError):
    """Order placement failed."""
    pass


class BrokerDataError(BrokerError):
    """Data fetch failed."""
    pass
