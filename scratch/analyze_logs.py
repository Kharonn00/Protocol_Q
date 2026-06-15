import csv
import re
import os
from decimal import Decimal

csv_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\log-events-viewer-result.csv"
output_path = r"C:\Users\A2\.gemini\antigravity-cli\brain\33622c45-3a4d-4749-a2df-160c7268c385\trading_report.md"

contract_pattern = re.compile(r'KX[A-Z0-9\-]+')

# We'll parse the file row by row
contracts = {}

def get_or_create_contract(cid):
    if not cid:
        return None
    if cid not in contracts:
        contracts[cid] = {
            "id": cid,
            "asset": cid.split('15M')[0].replace("KX", "").replace("-", ""),
            "bids": [],
            "buys": [],
            "tp_routes": [],
            "tp_hits": [],
            "tp_cancels": [],
            "other_logs": []
        }
    return contracts[cid]

with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    try:
        header = next(reader)
    except StopIteration:
        header = []
        
    for row in reader:
        if len(row) < 2:
            continue
        ts_raw, message = row[0], row[1]
        dt_match = re.search(r'\[(.*?)\]', message)
        dt_str = dt_match.group(1) if dt_match else ""
        matches = contract_pattern.findall(message)
        cid = matches[0] if matches else None
        
        if cid:
            cdata = get_or_create_contract(cid)
            if "EDGE FOUND" in message:
                z_score = re.search(r'Z-Score:\s*([+\-0-9\.]+)', message)
                ask = re.search(r'Ask:\s*\$([0-9]+(?:\.[0-9]+)?)', message)
                qty = re.search(r'Sniping\s*(\d+)', message)
                cdata["bids"].append({
                    "dt": dt_str,
                    "z_score": z_score.group(1) if z_score else "N/A",
                    "ask": ask.group(1) if ask else "N/A",
                    "qty": qty.group(1) if qty else "N/A"
                })
            elif "PAPER ORDER PLACED" in message and "BUY" in message:
                side = re.search(r"'(YES|NO)'", message)
                price = re.search(r'@\s*\$([0-9]+(?:\.[0-9]+)?)', message)
                qty = re.search(r'BUY\s*(\d+)x', message)
                cdata["buys"].append({
                    "dt": dt_str,
                    "side": side.group(1) if side else "N/A",
                    "price": price.group(1) if price else "N/A",
                    "qty": qty.group(1) if qty else "N/A",
                    "filled_qty": 0
                })
            elif "PAPER BROKER PARTIAL" in message and "BUY fill:" in message:
                qty = re.search(r'Total:\s*(\d+)/', message)
                if qty and cdata["buys"]:
                    cdata["buys"][-1]["filled_qty"] = int(qty.group(1))
            elif "Order filled" in message:
                qty = re.search(r'filled\s*\((\d+)/', message)
                if qty and cdata["buys"]:
                    cdata["buys"][-1]["filled_qty"] = int(qty.group(1))
            elif "Routing Take-Profit" in message:
                qty = re.search(r'Sell\s*(\d+)', message)
                side = re.search(r"'(YES|NO)'", message)
                price = re.search(r'@\s*\$([0-9]+(?:\.[0-9]+)?)', message)
                cdata["tp_routes"].append({
                    "dt": dt_str,
                    "qty": qty.group(1) if qty else "N/A",
                    "side": side.group(1) if side else "N/A",
                    "price": price.group(1) if price else "N/A"
                })
            elif "TAKE PROFIT HIT" in message:
                qty = re.search(r'Sold\s*(\d+)x', message)
                price = re.search(r'@\s*\$([0-9]+(?:\.[0-9]+)?)', message)
                cdata["tp_hits"].append({
                    "dt": dt_str,
                    "qty": qty.group(1) if qty else "N/A",
                    "price": price.group(1) if price else "N/A"
                })
            elif "buzzer" in message or "Trailing TP cancelled" in message:
                qty = re.search(r'Final filled quantity:\s*(\d+)/', message)
                cdata["tp_cancels"].append({
                    "dt": dt_str,
                    "filled_qty": qty.group(1) if qty else "0"
                })
            else:
                cdata["other_logs"].append(message)

# Let's aggregate trades
trades_summary = []
total_trades = 0
successful_tps = 0
expired_buzzer = 0
total_profit = Decimal("0.00")

for cid, cdata in sorted(contracts.items()):
    if not cdata["buys"]:
        continue
        
    total_trades += 1
    
    # Aggregate buy orders
    total_buy_qty = 0
    total_buy_cost = Decimal("0.00")
    side = "N/A"
    for b in cdata["buys"]:
        side = b["side"]
        qty = b["filled_qty"]
        price = Decimal(b["price"])
        total_buy_qty += qty
        total_buy_cost += Decimal(qty) * price
        
    # Aggregate sells/TP hits
    total_sell_qty = 0
    total_sell_revenue = Decimal("0.00")
    for s in cdata["tp_hits"]:
        qty = int(s["qty"])
        price = Decimal(s["price"])
        total_sell_qty += qty
        total_sell_revenue += Decimal(qty) * price
        
    # Sells at buzzer
    for c in cdata["tp_cancels"]:
        qty = int(c["filled_qty"])
        avg_tp_price = Decimal("0.00")
        if cdata["tp_routes"]:
            avg_tp_price = sum(Decimal(tp["price"]) for tp in cdata["tp_routes"]) / len(cdata["tp_routes"])
        total_sell_qty += qty
        total_sell_revenue += Decimal(qty) * avg_tp_price

    # Profit
    # Remaining portion expired at buzzer, settled at $0.00.
    profit = total_sell_revenue - total_buy_cost
    roi = (profit / total_buy_cost * 100) if total_buy_cost > 0 else Decimal("0.00")
    
    status = "EXPIRED AT BUZZER"
    if total_sell_qty == total_buy_qty and total_buy_qty > 0:
        status = "TAKE PROFIT HIT"
        successful_tps += 1
    else:
        expired_buzzer += 1
        
    total_profit += profit
    
    # Weighted average entry price
    avg_entry_price = total_buy_cost / Decimal(total_buy_qty) if total_buy_qty > 0 else Decimal("0.00")
    # Average TP target
    avg_tp_target = sum(Decimal(tp["price"]) for tp in cdata["tp_routes"]) / len(cdata["tp_routes"]) if cdata["tp_routes"] else Decimal("0.00")
    
    trades_summary.append({
        "contract": cid,
        "asset": cdata["asset"],
        "side": side,
        "entry_qty": total_buy_qty,
        "entry_price": avg_entry_price,
        "tp_price": avg_tp_target,
        "status": status,
        "profit": profit,
        "roi": roi
    })

# Write the updated Markdown Report
with open(output_path, 'w', encoding='utf-8') as md:
    md.write("# Kalshi Trading Bot Log Analysis Report (Aggregated)\n\n")
    md.write("This report analyzes the trading activity of the bot over the last 24 hours based on `log-events-viewer-result.csv`.\n\n")
    
    md.write("## Executive Summary\n\n")
    md.write(f"- **Total Events Traded / Bidded On**: {total_trades}\n")
    md.write(f"- **Successful Take-Profit (ROI secured)**: {successful_tps}\n")
    md.write(f"- **Expired at Buzzer (Held to expiry)**: {expired_buzzer}\n")
    md.write(f"- **Net Profit / Loss (P&L)**: ${total_profit:.2f}\n\n")
    
    md.write("## Trade Details Table\n\n")
    md.write("| Contract | Asset | Side | Total Entry Qty | Avg Entry Price | Avg TP Target | Status | P&L | ROI |\n")
    md.write("|---|---|---|---|---|---|---|---|---|\n")
    for t in trades_summary:
        roi_str = f"{t['roi']:+.1f}%" if t['roi'] != 0 else "0.0%"
        md.write(f"| {t['contract']} | {t['asset']} | {t['side']} | {t['entry_qty']} | ${t['entry_price']:.2f} | ${t['tp_price']:.2f} | **{t['status']}** | ${t['profit']:+.2f} | {roi_str} |\n")
        
    md.write("\n## Contract Lifecycle Timelines\n\n")
    for cid, cdata in sorted(contracts.items()):
        if not cdata["buys"]:
            continue
        md.write(f"### {cid} ({cdata['asset']})\n")
        for b in cdata["bids"]:
            md.write(f"- **{b['dt']}**: [EDGE DETECTED] Z-Score = {b['z_score']}, Ask = ${b['ask']}, requested qty = {b['qty']}\n")
        for bu in cdata["buys"]:
            md.write(f"- **{bu['dt']}**: [BUY ORDER PLACED] Placed buy order for {bu['qty']}x '{bu['side']}' @ ${bu['price']}. Filled quantity: {bu['filled_qty']}\n")
        for tp in cdata["tp_routes"]:
            md.write(f"- **{tp['dt']}**: [TAKE-PROFIT ROUTED] Placed sell order for {tp['qty']}x @ ${tp['price']}\n")
        for th in cdata["tp_hits"]:
            md.write(f"- **{th['dt']}**: [TAKE-PROFIT HIT] Sold {th['qty']}x @ ${th['price']}. P&L Secured.\n")
        for tc in cdata["tp_cancels"]:
            md.write(f"- **{tc['dt']}**: [BUZZER EXPIRED] TP order unresolved at buzzer. Cancelled. Final filled qty: {tc['filled_qty']}\n")
        md.write("\n")

print("Analysis successfully written to:", output_path)
