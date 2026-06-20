import sys
sys.stdout.reconfigure(encoding='utf-8')

warnings_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_warnings.txt"
others_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_others.txt"

def print_lifecycle(cid):
    print(f"=== LIFECYCLE FOR {cid} ===")
    with open(warnings_path, 'r', encoding='utf-8') as f:
        for line in f:
            if cid in line:
                print("WARN:", line.strip())
    with open(others_path, 'r', encoding='utf-8') as f:
        for line in f:
            if cid in line:
                print("INFO:", line.strip())

print_lifecycle("KXDOGE15M-26JUN191000-00")
print_lifecycle("KXDOGE15M-26JUN191015-15")
print_lifecycle("KXDOGE15M-26JUN191030-30")
print_lifecycle("KXDOGE15M-26JUN191045-45")
print_lifecycle("KXHYPE15M-26JUN191100-00")
print_lifecycle("KXDOGE15M-26JUN191145-45")
