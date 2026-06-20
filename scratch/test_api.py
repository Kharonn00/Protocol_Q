import urllib.request
import json

url = "https://external-api.kalshi.com/trade-api/v2/markets?series_ticker=KXDOGE15M&status=open"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
try:
    with urllib.request.urlopen(req) as response:
        html = response.read()
        data = json.loads(html)
        print("Success! Data keys:", data.keys())
        markets = data.get("markets", [])
        print("Number of open markets for KXDOGE15M:", len(markets))
        if markets:
            print("First market sample:")
            m = markets[0]
            print("Ticker:", m.get("ticker"))
            print("Title:", m.get("title"))
            print("Subtitle:", m.get("subtitle"))
            print("Close time:", m.get("close_time"))
            print("Floor strike:", m.get("floor_strike"))
except Exception as e:
    print("Error:", e)
