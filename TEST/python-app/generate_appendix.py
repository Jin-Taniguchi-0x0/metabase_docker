
import json
import csv
import statistics
from datetime import datetime
import collections

# File Paths
LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"
GROUP_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - グループ.csv"
SURVEY_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - フォームの回答.csv"

# Name Mapping
NAME_MAP = {
    "芹澤尚舜": "せり", "峪紳大朗": "さこ", "佐竹宏紀": "ひろくん", "今村真沙斗": "まさと",
    "宮澤匠": "たくみ", "鈴木俊詞": "しゅんじ", "永沼翔翼": "つばさ", "田中 翔太郎": "たなか",
    "岡本悠吾": "ゆうご", "矢野温加": "やん", "塙裕貴": "はなわ", "板井孝樹": "いたい"
}

# Answer Keys
ANSWERS = {
    "Wine": {
        "A": ["romania", "ルーマニア"],
        "B": ["20", "20.1", "20ドル"],
        "C": ["moscato", "pinot", "モスカート", "ピノ"]
    },
    "UFO": {
        "A": ["2000", "1990"],
        "B": ["us", "usa", "アメリカ"],
        "C": ["light", "光"]
    }
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
        if header_idx == -1: return {}

        for row in lines[header_idx+1:]:
            if len(row) < 7: continue
            user_name = row[0]
            if not user_name: continue
            
            # Task 1
            if row[1]: mapping[str(row[1])] = {"user": user_name, "task": row[2], "rec": row[3] == "あり", "order": 1}
            # Task 2
            if row[4]: mapping[str(row[4])] = {"user": user_name, "task": row[5], "rec": row[6] == "あり", "order": 2}
    return mapping

def load_logs(log_file):
    logs = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try: logs.append(json.loads(line))
            except: continue
    return logs

def grade_answer(task, q_key, user_text):
    if not user_text: return False
    user_text = user_text.lower()
    keywords = ANSWERS.get(task, {}).get(q_key, [])
    for kw in keywords:
        if kw in user_text:
            return True
    return False

def get_survey_answers():
    user_answers = {} # user -> {task: {A: val, B: val, C: val}}
    
    with open(SURVEY_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Headers are messy, index based
        # Task 1 QA: 27, 28, 29
        # Task 2 QA: 33, 34, 35 (Let's verify by checking row length mostly)
        # Actually, let's look at the row content for headers? No, headers are repeated or weird.
        # Based on previous `head`, Task 1 answers are approx around col 27.
        # Let's count properly: 
        # 0:Timestamp, 1:Name ... 13:Task1Name ... 20:Task2Name ... 
        # Let's dynamically find answers based on Task name position.
        
        # Actually, let's assume specific indices based on typical Google Form structure or the row I saw.
        # Row 1 (Header): ... 質問Aの回答, 質問Bの回答, 質問Cの回答 ... (Repeated)
        
        # Let's refine logic: iterate rows, identify name, identify Task1/Task2, grab corresponding cols.
        # Task 1 Answers are typically before Task 2 Name.
        # The `head` output showed:
        # Col 27: 質問A (Task 1) - "test"
        # ...
        # Col 37?: 質問A (Task 2)
        
        # Let's use a simpler heuristic:
        # Task 1 is at index 13.
        # Task 2 is at index 20 (Wait, 13+metrics+text...).
        # Let's map indices once I see the first row.
        
        # Actually, I'll just hardcode indices based on standard form output if possible, or scan.
        # Task 1 Qs: 27, 28, 29
        # Task 2 Qs: 39, 40, 41 (Estimated. Let's look at the head again or be robust)
        
        # Robust strategy:
        # Find column indices for "質問Aの回答". There should be two.
        # Assign first set to Task 1, second set to Task 2.
        
        header = next(reader)
        qa_indices = [i for i, h in enumerate(header) if "質問Aの回答" in h]
        qb_indices = [i for i, h in enumerate(header) if "質問Bの回答" in h]
        qc_indices = [i for i, h in enumerate(header) if "質問Cの回答" in h]
        
        for row in reader:
            name = row[1]
            if "石埜" in name or "test" in name.lower(): continue
            nickname = NAME_MAP.get(name)
            if not nickname: continue
            
            task1 = row[13]
            task2 = row[20] # This might vary. Let's check if col 20 is a task name.
            
            # Extract Task 1
            if task1 and len(qa_indices) > 0:
                t1a = row[qa_indices[0]] if len(row) > qa_indices[0] else ""
                t1b = row[qb_indices[0]] if len(row) > qb_indices[0] else ""
                t1c = row[qc_indices[0]] if len(row) > qc_indices[0] else ""
                
                if nickname not in user_answers: user_answers[nickname] = {}
                user_answers[nickname][task1] = score_task(task1, t1a, t1b, t1c)
                
            # Extract Task 2
            if task2 and len(qa_indices) > 1:
                t2a = row[qa_indices[1]] if len(row) > qa_indices[1] else ""
                t2b = row[qb_indices[1]] if len(row) > qb_indices[1] else ""
                t2c = row[qc_indices[1]] if len(row) > qc_indices[1] else ""
                
                user_answers[nickname][task2] = score_task(task2, t2a, t2b, t2c)
                
    return user_answers

def score_task(task, a, b, c):
    score = 0
    if grade_answer(task, "A", a): score += 1
    if grade_answer(task, "B", b): score += 1
    if grade_answer(task, "C", c): score += 1
    return f"{score}/3"

def main():
    group_map = load_group_mapping() # dash_id -> meta
    logs = load_logs(LOG_FILE)
    survey_scores = get_survey_answers() # user -> task -> score_str
    
    # Debug output
    print("Debug: Group Mapping Keys Sample:", list(group_map.keys())[:2])
    if group_map:
        first_val = list(group_map.values())[0]
        print("Debug: First Group Map Value:", first_val)
        print("Debug: Nickname Mapped:", NAME_MAP.get(first_val['user']))

    print("Debug: Survey Scores Keys:", list(survey_scores.keys()))
    
    dash_logs = collections.defaultdict(list)
    sample_view = None
    for log in logs:
        did = str(log.get('dashboard_id'))
        if did in group_map:
            dash_logs[did].append(log)
            if log.get('action') == 'create_view' and not sample_view:
                sample_view = log

    print("Debug: Sample Create View Log:", json.dumps(sample_view, ensure_ascii=False) if sample_view else "None")
            
    # Aggregate Metrics
    data = []
    
    for did, meta in group_map.items():
        events = dash_logs.get(did, [])
        if not events: continue
        events.sort(key=lambda x: x['timestamp'])
        
        # Time
        start = datetime.fromisoformat(events[0]['timestamp'])
        end = datetime.fromisoformat(events[-1]['timestamp'])
        duration = (end - start).total_seconds() / 60
        
        # Views
        created_views = 0
        rec_selected = 0
        unique_configs = set()
        rec_shown = 0
        
        # Track Recs Shown
        # Logic: If action=generate_recommendations, sum len(recommendations).
        # Important: Ensure recommendation was enabled/requested?
        # Assuming all logged generate_recommendations were shown to user.
        
        for e in events:
            action = e.get('action')
            
            if action == 'create_view':
                created_views += 1
                if e.get('recommendation_source') == 'recommendation':
                    rec_selected += 1
                
                # Unique Check
                # Config signature: chart_type + x + y
                ctype = e.get('card_type')
                xaxis = e.get('x_axis')
                yaxis = e.get('y_axis')
                # Aggregation might matter too but let's stick to axes
                if ctype:
                    unique_configs.add(e.get('card_name', 'Unknown') + ctype)
            
            if action == 'generate_recommendations':
                recs = e.get('recommendations', [])
                if recs:
                    rec_shown += len(recs)
                    
        # Correctness
        # Check if user is in NAME_MAP (Full Name) or is already a Nickname
        nickname = NAME_MAP.get(meta['user'])
        if not nickname:
            # Check if user is a value in NAME_MAP (already a nickname)
            if meta['user'] in NAME_MAP.values():
                nickname = meta['user']
                
        correctness = "-"
        if nickname and meta['task'] in survey_scores.get(nickname, {}):
            correctness = survey_scores[nickname][meta['task']]
            
        data.append({
            "User": f"P{did}", # Anonymized ID using Dashboard ID? Or just P-Order
            "RealUser": nickname, # Internal tracking
            "Condition": "Rec" if meta['rec'] else "No Rec",
            "Task": meta['task'],
            "Group": "A" if meta['order'] == 1 and meta['task'] == "UFO" else "B", # Simplistic group logic, maybe not needed
            "Duration": f"{duration:.1f}",
            "Created": created_views,
            "Unique": len(unique_configs),
            "RecShown": rec_shown,
            "RecSelected": rec_selected,
            "Correctness": correctness
        })
        
    # Assign IDs
    users = sorted(list(set(m['user'] for m in group_map.values())))
    user_id_map = {u: f"P{i+1:02d}" for i, u in enumerate(users)}
    
    # Determine Groups (A-D)
    # Logic: Group users by their (Task1, Rec1) -> (Task2, Rec2) pattern
    # Actually, simplistic view:
    # Group A: UFO(Rec) -> Wine(No Rec)
    # Group B: UFO(No Rec) -> Wine(Rec)
    # Group C: Wine(Rec) -> UFO(No Rec)
    # Group D: Wine(No Rec) -> UFO(Rec)
    
    # Let's verify each user's pattern
    user_patterns = {}
    for uid, pid in user_id_map.items():
        # Find tasks for this user
        tasks = sorted([m for m in group_map.values() if m['user'] == uid], key=lambda x: x['order'])
        if len(tasks) < 2: 
            user_patterns[uid] = "Unknown"
            continue
            
        t1, t2 = tasks[0], tasks[1]
        
        # Categorize
        if t1['task'] == "UFO":
            if t1['rec']: group = "A" # UFO(Rec) -> Wine(No Rec) inferred
            else: group = "B" # UFO(No Rec) -> Wine(Rec) inferred
        else: # Wine First
            if t1['rec']: group = "C" # Wine(Rec) -> UFO(No Rec) inferred
            else: group = "D" # Wine(No Rec) -> UFO(Rec) inferred
        user_patterns[uid] = group
    
    # Generate Tables
    conditions = [
        ("UFO", "Rec"),
        ("UFO", "No Rec"),
        ("Wine", "Rec"),
        ("Wine", "No Rec")
    ]
    
    print("# Appendix Tables\n")
    
    for task, cond in conditions:
        print(f"### {task} - {cond}")
        print("| User | Group | Task Duration | Created Views | Unique Views | Rec Shown | Rec Selected | Correctness |")
        print("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")
        
        subset = [d for d in data if d['Task'] == task and d['Condition'] == cond]
        # Sort by User ID
        subset.sort(key=lambda x: user_id_map.get(x['RealUser'], "P99"))
        
        for row in subset:
             uid = user_id_map.get(row['RealUser'], "Unknown")
             group = user_patterns.get(row['RealUser'], "Unknown")
             # Format times and counts
             print(f"| {uid} | {group} | {row['Duration']} min | {row['Created']} | {row['Unique']} | {row['RecShown']} | {row['RecSelected']} | {row['Correctness']} |")
        print("\n")

if __name__ == "__main__":
    main()
