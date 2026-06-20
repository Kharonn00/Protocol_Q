import sys
sys.stdout.reconfigure(encoding='utf-8')

warnings_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_warnings.txt"
others_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_others.txt"

def find_keywords(path, label):
    print(f"=== KEYWORDS IN {label} ===")
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if any(kw in line.upper() for kw in ["PAYOUT", "BUZZER", "WON", "LOSS", "SETTLEMENT", "DRAWDOWN"]):
                print(line.strip())

find_keywords(warnings_path, "WARNINGS")
find_keywords(others_path, "OTHERS")
