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
        print(f"Error querying {path}: {e}")
        return None

# Step 1: Find active market for KXDOGE15M
markets_data = query_api("/markets?series_ticker=KXDOGE15M&status=open")
if not markets_data:
    print("Could not fetch markets")
    exit(1)

markets = markets_data.get("markets", [])
if not markets:
    print("No open markets found for KXDOGE15M")
    exit(0)

market = markets[0]
contract_id = market.get("ticker")
print(f"Active contract: {contract_id}")

# Step 2: Get orderbook for active contract
safe_contract_id = urllib.parse.quote(contract_id, safe='')
ob_data = query_api(f"/markets/{safe_contract_id}/orderbook?depth=1")
if not ob_data:
    print("Could not fetch orderbook")
    exit(1)

print("Orderbook keys:", ob_data.keys())
ob_fp = ob_data.get("orderbook_fp")
ob_standard = ob_data.get("orderbook")

best_yes_bid = 0.0
best_no_bid = 0.0

if ob_fp:
    yes_bids = ob_fp.get("yes_dollars", [])
    no_bids = ob_fp.get("no_dollars", [])
    print("FP YES bids:", yes_bids)
    print("FP NO bids:", no_bids)
    if yes_bids:
        best_yes_bid = float(yes_bids[0][0])
    if no_bids:
        best_no_bid = float(no_bids[0][0])
elif ob_standard:
    yes_bids = ob_standard.get("yes", [])
    no_bids = ob_standard.get("no", [])
    print("Standard YES bids:", yes_bids)
    print("Standard NO bids:", no_bids)
    if yes_bids:
        best_yes_bid = float(yes_bids[0][0]) / 100.0
    if no_bids:
        best_no_bid = float(no_bids[0][0]) / 100.0

print(f"best_yes_bid: {best_yes_bid}")
print(f"best_no_bid: {best_no_bid}")

best_yes_ask = 1.00 - best_no_bid
best_no_ask = 1.00 - best_yes_bid
print(f"best_yes_ask (1.00 - best_no_bid): {best_yes_ask}")
print(f"best_no_ask (1.00 - best_yes_bid): {best_no_ask}")

print(f"YES ask between 0.80 and 0.90? {0.80 <= best_yes_ask <= 0.90}")
print(f"NO ask between 0.80 and 0.90? {0.80 <= best_no_ask <= 0.90}")
