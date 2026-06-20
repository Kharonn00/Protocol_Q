with open(r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_warnings.txt", 'r', encoding='utf-8') as f:
    for line in f:
        # filter logs containing Z-SCORE BREAKOUT or THETA HARVESTER ACTIVE
        if "Z-SCORE BREAKOUT" in line or "THETA HARVESTER ACTIVE" in line or "Z-Score Breakout" in line:
            print(line.strip())
