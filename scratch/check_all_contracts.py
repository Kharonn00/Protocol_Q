import csv
import sys
import collections
import re
from decimal import Decimal

sys.stdout.reconfigure(encoding='utf-8')

info_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\log-info.csv"
cutoff_str = "2026-06-19 00:15:55.971"

# We want to find all orders placed, filled, and their details.
orders = {} # client_order_id or order_id -> details
trades = collections.defaultdict(list) # contract_id -> list of events

with open(info_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader, None)
    for row in reader:
        if not row or len(row) < 2:
            continue
        ts, msg = row[0], row[1]
        ts_norm = ts.replace('T', ' ').replace('Z', '')
        if ts_norm <= cutoff_str:
            continue
        if msg.startswith('{'):
            continue
            
        # Extract contract ID if present
        contract_match = re.search(r'KX[A-Z0-9\-]+', msg)
        cid = contract_match.group(0) if contract_match else None
        
        # We can collect all lines chronologically per contract
        if cid:
            trades[cid].append((ts, msg))

for cid, logs in sorted(trades.items()):
    print(f"\n=================== CONTRACT: {cid} ===================")
    for ts, msg in logs:
        # Highlight important events
        if any(kw in msg for kw in ["ACTIVE", "PLACED", "finalized", "fill", "expired", "expired.", "BUZZER", "PAYOUT", "TP HIT", "TOTAL TP FILLED"]):
            print(f"  {ts} | {msg}")
