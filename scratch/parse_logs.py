import csv
import json
import collections
import re
import sys

# Set stdout to use utf-8
sys.stdout.reconfigure(encoding='utf-8')

info_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\log-info.csv"
cutoff_str = "2026-06-19 00:15:55.971"

all_warnings = []
all_others = []

with open(info_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader, None) # skip header
    for row in reader:
        if not row or len(row) < 2:
            continue
        ts, msg = row[0], row[1]
        ts_norm = ts.replace('T', ' ').replace('Z', '')
        if ts_norm <= cutoff_str:
            continue
        if msg.startswith('{'):
            continue # ignore metrics
            
        if "WARNING" in msg:
            all_warnings.append((ts, msg))
        elif "ERROR" in msg:
            print("ERROR:", ts, msg)
        else:
            all_others.append((ts, msg))

print(f"Total Warnings: {len(all_warnings)}")
print(f"Total Others: {len(all_others)}")

# Write out the files FIRST
with open(r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_warnings.txt", 'w', encoding='utf-8') as f:
    for ts, msg in all_warnings:
        f.write(f"{ts} | {msg}\n")

with open(r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_others.txt", 'w', encoding='utf-8') as f:
    for ts, msg in all_others:
        f.write(f"{ts} | {msg}\n")

print("Files written successfully.")

# Count unique messages or similar patterns
warn_counts = collections.Counter()
for ts, msg in all_warnings:
    simplified = re.sub(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\]', '[TS]', msg)
    simplified = re.sub(r'KX\S+', '[CONTRACT]', simplified)
    simplified = re.sub(r'Price: \$\d+\.\d+|Mean: \$\d+\.\d+|Z-Score: [+\-]?\d+\.\d+|Ticks: \d+/\d+', '[TELEMETRY]', simplified)
    warn_counts[simplified] += 1

print("\n--- WARNING TYPES AND COUNTS ---")
for msg, count in warn_counts.most_common():
    try:
        print(f"{count}x: {msg}")
    except Exception as e:
        print(f"{count}x: [Encoding error print: {repr(msg)}]")
