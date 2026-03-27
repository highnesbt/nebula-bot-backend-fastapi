"""Mock broker client for testing — returns fake data, no real API calls."""


class MockBrokerClient:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self._logged_in = False

    def login(self, client_id: str, password: str, totp_secret: str):
        self._logged_in = True

    def get_ltp(self, exchange: str, symbol: str, token: str) -> float:
        return 100.0

    def get_candle_data(self, symbol_token: str, interval: str,
                        from_date: str, to_date: str, exchange: str = "NSE") -> list:
        return []

    def place_order(self, symbol: str, symbol_token: str, transaction_type: str,
                    quantity: int, exchange: str = "NSE", price: float = 0.0) -> str:
        return "MOCK_ORDER_001"

    def get_order_fill_price(self, order_id: str, max_attempts: int = 5) -> float | None:
        return 100.0

    def cancel_order(self, order_id: str, variety: str = "NORMAL") -> bool:
        return True

    def modify_order(self, order_id: str, new_price: float, **kwargs) -> bool:
        return True

    def get_order_status(self, order_id: str) -> dict:
        return {"orderstatus": "complete", "averageprice": "100.0", "filledshares": "1"}

    def search_scrip(self, exchange: str, query: str) -> list:
        return [
            {"exchange": exchange, "tradingsymbol": f"{query}-EQ", "symboltoken": "12345"},
        ]
