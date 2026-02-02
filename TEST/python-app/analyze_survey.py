import csv
import re
import statistics

CSV_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - フォームの回答.csv"

def parse_score(text):
    match = re.search(r'\((\d)\)', text)
    if match:
        return int(match.group(1))
    return None

def calculate_sus(row):
    # Columns 3 to 12 (indices 3-12) corresponds to Q1-Q10
    # Q1, Q3, Q5, Q7, Q9: Score = X - 1
    # Q2, Q4, Q6, Q8, Q10: Score = 5 - X
    
    scores = []
    for i in range(10):
        col_idx = 3 + i
        raw_val = parse_score(row[col_idx])
        if raw_val is None:
            return None
        
        if (i + 1) % 2 == 1: # Odd question (1, 3, ...)
            scores.append(raw_val - 1)
        else: # Even question (2, 4, ...)
            scores.append(5 - raw_val)
            
    return sum(scores) * 2.5

def main():
    sus_scores = []
    
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        for row in reader:
            name = row[1]
            if "テスト" in name or "test" in name.lower():
                continue
                
            sus = calculate_sus(row)
            if sus is not None:
                sus_scores.append(sus)
                print(f"{name}: {sus}")

    if sus_scores:
        avg = statistics.mean(sus_scores)
        stdev = statistics.stdev(sus_scores)
        print("-" * 20)
        print(f"Total Participants: {len(sus_scores)}")
        print(f"Average SUS Score: {avg:.2f}")
        print(f"Std Dev: {stdev:.2f}")
    else:
        print("No valid scores found.")

if __name__ == "__main__":
    main()
