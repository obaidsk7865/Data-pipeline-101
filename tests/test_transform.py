import pandas as pd
from transform import transform_market_response

SAMPLE_JSON = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 100000,
        "market_cap": 2000000000000,
        "total_volume": 50000000000,
        "circulating_supply": 19000000,
        "last_updated": "2025-11-13T08:27:11.083Z"
    }
]

def test_transform_basic():
    df = transform_market_response(SAMPLE_JSON)
    assert len(df) == 1
    assert "symbol" in df.columns
    assert "price_usd" in df.columns
