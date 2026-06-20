import re

warnings_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_warnings.txt"
others_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_others.txt"

# Let's collect all lines from both files and sort them by timestamp
all_logs = []
with open(warnings_path, 'r', encoding='utf-8') as f:
    for line in f:
        all_logs.append(line.strip())
with open(others_path, 'r', encoding='utf-8') as f:
    for line in f:
        all_logs.append(line.strip())

# Sort logs by their timestamp prefix (first 23 characters)
all_logs.sort(key=lambda x: x[:23])

# Filter and print lines that have to do with trading, executions, P&L, balance, limits, etc.
trading_keywords = ["BUY", "SELL", "fill", "TP", "PnL", "Net P&L", "Theta Harvester", "Z-Score Breakout", "Z-SCORE BREAKOUT", "Sniping", "DRAWDOWN"]
for log in all_logs:
    if any(kw in log for kw in trading_keywords):
        try:
            print(log)
        except Exception:
            # handle print encoding errors
            pass
