import csv
import re

SURVEY_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - フォームの回答.csv"
GROUP_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - グループ.csv"

NAME_MAP = {
    "芹澤尚舜": "せり",
    "峪紳大朗": "さこ",
    "佐竹宏紀": "ひろくん",
    "今村真沙斗": "まさと",
    "宮澤匠": "たくみ",
    "鈴木俊詞": "しゅんじ",
    "永沼翔翼": "つばさ",
    "田中 翔太郎": "たなか",
    "岡本悠吾": "ゆうご",
    "矢野温加": "やん",
    "塙裕貴": "はなわ",
    "板井孝樹": "いたい"
}

def load_group_mapping():
    mapping = {}
    with open(GROUP_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
        header_idx = -1
        for i, row in enumerate(lines):
            if len(row) > 1 and row[1] == 'id':
                header_idx = i
                break
        
        for row in lines[header_idx+1:]:
            if len(row) < 7: continue
            user_name = row[0]
            if not user_name: continue
            
            # Task 1
            task1 = row[2]
            rec1 = row[3] == "あり"
            mapping[(user_name, task1, 1)] = rec1
            
            # Task 2
            task2 = row[5]
            rec2 = row[6] == "あり"
            mapping[(user_name, task2, 2)] = rec2
            
    return mapping

def parse_likert(val):
    match = re.search(r'\((\d)\)', val)
    return int(match.group(1)) if match else None

def analyze():
    # Build simple mapping: (nickname, task) -> is_rec
    # Note: Using task name from Group file, hopefully matches Survey file
    # Actually, let's just use (nickname, task) -> rec
    
    group_map = load_group_mapping() 
    # Key: (user, task, order), we might need to be careful if task name differs
    # Simplification: (nickname, task) -> rec. 
    # Assumes user doesn't do same task twice (which is true)
    
    nick_task_rec = {}
    for (user, task, order), is_rec in group_map.items():
        nick_task_rec[(user, task)] = is_rec

    print("=== Group Mapping Content ===")
    for k, v in nick_task_rec.items():
        print(f"{k}: Rec={v}")

    data_points = {
        "Readability": {"Rec": [], "No Rec": []},
        "Requirements": {"Rec": [], "No Rec": []},
        "RecUseful": {"Rec": [], "No Rec": []},
        "RecSurprise": {"Rec": [], "No Rec": []}
    }

    sus_scores = []
    q_scores = [[] for _ in range(10)]

    with open(SURVEY_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        for row_idx, row in enumerate(reader):
            if "テスト" in row[1] or "test" in row[1].lower(): continue
            name = row[1]
            if "石埜" in name: continue 
            
            nickname = NAME_MAP.get(name)
            if not nickname:
                print(f"Skip unknown name: {name}")
                continue

            print(f"\nProcessing {name} ({nickname})")

            # SUS (Cols 3-12)
            # odd questions (0, 2, ..): score - 1
            # even questions (1, 3, ..): 5 - score
            current_sus_raw = []
            valid_sus = True
            for i in range(10):
                val = parse_likert(row[3+i])
                if val:
                    q_scores[i].append(val)
                    if i % 2 == 0: # Q1, Q3... (Indices 0, 2...) -> score-1
                        current_sus_raw.append(val - 1)
                    else:          # Q2, Q4... (Indices 1, 3...) -> 5-score
                        current_sus_raw.append(5 - val)
                else:
                    valid_sus = False
            
            if valid_sus:
                total = sum(current_sus_raw) * 2.5
                sus_scores.append(total)
                print(f"  SUS: {total}")

            # Task 1 (Cols 13-17)
            task1 = row[13]
            if task1:
                is_rec = nick_task_rec.get((nickname, task1))
                if is_rec is None:
                    print(f"  Warning: No condition found for {nickname}, {task1}")
                else:
                    cond = "Rec" if is_rec else "No Rec"
                    print(f"  Task 1: {task1} ({cond})")
                    
                    # Readability col 14
                    val = parse_likert(row[14])
                    if val: 
                        data_points["Readability"][cond].append((nickname, val))
                        print(f"    Readability: {val} (raw: {row[14]})")
                    else:
                        print(f"    Readability: Failed to parse '{row[14]}'")

                    # Requirements col 15
                    val = parse_likert(row[15])
                    if val:
                        data_points["Requirements"][cond].append((nickname, val))
                    
                    if is_rec:
                        val = parse_likert(row[16])
                        if val: data_points["RecUseful"][cond].append((nickname, val))
                        val = parse_likert(row[17])
                        if val: data_points["RecSurprise"][cond].append((nickname, val))

            # Task 2 (Cols 20-24)
            if len(row) > 20:
                task2 = row[20]
                if task2:
                    is_rec = nick_task_rec.get((nickname, task2))
                    if is_rec is None:
                        print(f"  Warning: No condition found for {nickname}, {task2}")
                    else:
                        cond = "Rec" if is_rec else "No Rec"
                        print(f"  Task 2: {task2} ({cond})")
                        
                        # Readability col 21
                        val = parse_likert(row[21])
                        if val: 
                            data_points["Readability"][cond].append((nickname, val))
                            print(f"    Readability: {val} (raw: {row[21]})")
                        else:
                            print(f"    Readability: Failed to parse '{row[21]}'")

                        # Requirements col 22
                        val = parse_likert(row[22])
                        if val:
                            data_points["Requirements"][cond].append((nickname, val))
                        
                        if is_rec:
                            val = parse_likert(row[23])
                            if val: data_points["RecUseful"][cond].append((nickname, val))
                            val = parse_likert(row[24])
                            if val: data_points["RecSurprise"][cond].append((nickname, val))

    print("\n=== Summary of Collected Data ===")
    
    # Calculate SUS
    if sus_scores:
        avg_sus = sum(sus_scores) / len(sus_scores)
        print(f"Total SUS Score: {avg_sus:.1f} (n={len(sus_scores)})")
        
        print("--- Per Question SUS ---")
        for i in range(10):
            vals = q_scores[i]
            avg = sum(vals)/len(vals)
            print(f"Q{i+1}: {avg:.2f}")

    for metric, conds in data_points.items():
        print(f"--- {metric} ---")
        for cond, values in conds.items():
            print(f"  {cond} (n={len(values)}): {values}")
            if values:
                avg = sum(v[1] for v in values) / len(values)
                print(f"    Avg: {avg:.2f}")

if __name__ == "__main__":
    analyze()
