with open(r"C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\scratch\extracted_others.txt", 'r', encoding='utf-8') as f:
    lines = f.readlines()
    
# Find the line with "DRAWDOWN LIMIT REACHED" and print 10 lines before and after
for i, line in enumerate(lines):
    if "DRAWDOWN LIMIT REACHED" in line:
        print(f"--- Trigger found at line {i} ---")
        start = max(0, i - 15)
        end = min(len(lines), i + 5)
        for j in range(start, end):
            print(f"{j}: {lines[j].strip()}")
