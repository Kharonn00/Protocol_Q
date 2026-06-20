import urllib.request
import json
import urllib.parse

def query_api(path):
    url = f"https://external-api.kalshi.com/trade-api/v2{path}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read())
    except Exception as e:
        print(f"Error: {e}")
        return None

markets_data = query_api("/markets?series_ticker=KXBTC15M&status=open")
if markets_data and markets_data.get("markets"):
    contract_id = markets_data["markets"][0]["ticker"]
    print(f"Checking orderbook for {contract_id}...")
    safe_contract_id = urllib.parse.quote(contract_id, safe='')
    ob_data = query_api(f"/markets/{safe_contract_id}/orderbook?depth=5")
    if ob_data:
        print(json.dumps(ob_data, indent=2))
