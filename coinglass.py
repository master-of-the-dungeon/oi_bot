import requests

url = "https://open-api-v3.coinglass.com/api/futures/openInterest/ohlc-history?exchange=Binance&symbol=BTCUSDT&interval=1d"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

print(response.text)