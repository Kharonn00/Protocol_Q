others_path = r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_others.txt"

with open(others_path, 'r', encoding='utf-8') as f:
    for line in f:
        if "14:5" in line and "HYPE-USD" in line:
            print(line.strip())
