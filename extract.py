import requests

url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum",
    "order": "market_cap_desc",
    "per_page": 2,
    "page": 1,
    "sparkline": "false",
    "price_change_percentage": "24h"
}

response = requests.get(url, params=params)
data = response.json()

print(data)
