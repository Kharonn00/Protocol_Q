import collections
import re

others_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_others.txt"

other_counts = collections.Counter()
with open(others_path, 'r', encoding='utf-8') as f:
    for line in f:
        # simplify
        simplified = re.sub(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\]', '[TS]', line)
        simplified = re.sub(r'KX\S+', '[CONTRACT]', simplified)
        simplified = re.sub(r'Price: \$\d+\.\d+|Mean: \$\d+\.\d+|Z-Score: [+\-]?\d+\.\d+|Ticks: \d+/\d+', '[TELEMETRY]', simplified)
        other_counts[simplified] += 1

print("--- OTHER LOGS TYPES AND COUNTS ---")
for msg, count in other_counts.most_common(40):
    try:
        print(f"{count}x: {msg.strip()}")
    except Exception as e:
        print(f"{count}x: [Encoding error print: {repr(msg)}]")
