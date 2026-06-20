import urllib.request
import json

url = "https://external-api.kalshi.com/trade-api/v2/markets?series_ticker=KXDOGE15M"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
try:
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read())
        markets = data.get("markets", [])
        print("Total KXDOGE15M markets:", len(markets))
        for m in markets:
            print(f"Ticker: {m.get('ticker')} | Status: {m.get('status')} | Close: {m.get('close_time')} | Strike: {m.get('floor_strike')} | Title: {m.get('title')}")
except Exception as e:
    print("Error:", e)
