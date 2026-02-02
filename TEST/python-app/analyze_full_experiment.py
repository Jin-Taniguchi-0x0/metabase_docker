import json
import csv
import statistics
from datetime import datetime

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"
GROUP_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - グループ.csv"
SURVEY_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - フォームの回答.csv"

def load_group_mapping():
    mapping = {} # dashboard_id -> {user, task, rec_condition}
    
    with open(GROUP_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Skip header lines (rows 1 and 2) based on file view
        # Row 1: ,,,,,,,, (empty)
        # Row 2: ,1回目,,,2回目,,,,
        # Row 3: ,id,task,rec,id,task,rec,memo,
        # Actual data starts from row 4
        
        lines = list(reader)
        # Find the header row index
        header_idx = -1
        for i, row in enumerate(lines):
            if len(row) > 1 and row[1] == 'id':
                header_idx = i
                break
        
        if header_idx == -1:
            print("Error: Could not find header row in group file")
            return {}

        for row in lines[header_idx+1:]:
            if len(row) < 7: continue
            user_name = row[0]
            if not user_name: continue

            # Task 1
            dash1 = row[1]
            task1 = row[2]
            rec1 = row[3]
            if dash1:
                mapping[str(dash1)] = {"user": user_name, "task": task1, "rec": rec1 == "あり", "order": 1}
            
            # Task 2
            dash2 = row[4]
            task2 = row[5]
            rec2 = row[6]
            if dash2:
                mapping[str(dash2)] = {"user": user_name, "task": task2, "rec": rec2 == "あり", "order": 2}

    return mapping

def load_logs(log_file):
    logs = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return logs

def analyze():
    group_mapping = load_group_mapping() # dash_id -> {task, rec, user, order}
    logs = load_logs(LOG_FILE)
    
    # 1. Map Email -> Dashboard IDs from Logs
    email_to_dashboards = {}
    for log in logs:
        uid = log.get('user_id')
        did = str(log.get('dashboard_id'))
        if uid and did and did in group_mapping:
            if uid not in email_to_dashboards:
                email_to_dashboards[uid] = set()
            email_to_dashboards[uid].add(did)

    # Group logs by dashboard_id for Quantitative Analysis
    dashboard_sessions = {}
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        if dash_id in group_mapping:
            if dash_id not in dashboard_sessions:
                dashboard_sessions[dash_id] = []
            dashboard_sessions[dash_id].append(log)
            
    # Calculate metrics per dashboard
    results = []
    
    for dash_id, meta in group_mapping.items():
        events = dashboard_sessions.get(dash_id, [])
        if not events:
            # print(f"Warning: No logs for Dashboard {dash_id} ({meta['user']})")
            continue
            
        events.sort(key=lambda x: x['timestamp'])
        
        start_time = datetime.fromisoformat(events[0]['timestamp'])
        end_time = datetime.fromisoformat(events[-1]['timestamp'])
        duration_min = (end_time - start_time).total_seconds() / 60
        
        views_created = 0
        rec_used = 0
        
        for e in events:
            if e.get('action') == 'create_view':
                views_created += 1
                if e.get('recommendation_source') == 'recommendation':
                    rec_used += 1
        
        rec_rate = (rec_used / views_created * 100) if views_created > 0 else 0
        
        results.append({
            "user": meta['user'],
            "task": meta['task'],
            "rec": meta['rec'],
            "duration": duration_min,
            "views": views_created,
            "rec_used": rec_used,
            "rec_rate": rec_rate
        })
        
    # Aggregate Quantitative Results
    print("=== Quantitative Results ===")
    
    conditions = [
        ("UFO", False, "No Rec"),
        ("UFO", True, "Rec"),
        ("Wine", False, "No Rec"),
        ("Wine", True, "Rec")
    ]
    
    for task, is_rec, label in conditions:
        subset = [r for r in results if r['task'] == task and r['rec'] == is_rec]
        if not subset:
            # print(f"{task} ({label}): No Data")
            continue
            
        avg_time = statistics.mean([r['duration'] for r in subset])
        avg_views = statistics.mean([r['views'] for r in subset])
        avg_rec_rate = statistics.mean([r['rec_rate'] for r in subset])
        
        print(f"{task} - {label} (n={len(subset)}):")
        print(f"  Duration: {avg_time:.2f} min (SD: {statistics.stdev([r['duration'] for r in subset]) if len(subset)>1 else 0:.2f})")
        print(f"  Views: {avg_views:.1f}")
        if is_rec:
            print(f"  Rec Usage Rate: {avg_rec_rate:.1f}%")
        print("-" * 20)
    
    # 2. Analyze Survey (SUS + Task Specific)
    print("\n=== SUS Scores ===")
    import re
    sus_total_scores = []
    q_scores = [[] for _ in range(10)]
    
    metrics = {
        "readable": {"Rec": [], "No Rec": []},
        "requirements": {"Rec": [], "No Rec": []},
        "rec_useful": [], 
        "rec_surprise": [] 
    }

    # Manual Name Mapping (Survey Full Name -> Group CSV Nickmake)
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

    # Build (nickname, task) -> is_rec map from group_mapping
    # group_mapping values are meta dicts: {'user': nickname, 'task': 'UFO', 'rec': bool, ...}
    nickname_task_condition = {}
    for meta in group_mapping.values():
        key = (meta['user'], meta['task'])
        nickname_task_condition[key] = meta['rec']

    def parse_likert(val):
        match = re.search(r'\((\d)\)', val)
        return int(match.group(1)) if match else None

    with open(SURVEY_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if "テスト" in row[1] or "test" in row[1].lower(): continue
            name = row[1]
            if "石埜" in name: continue 
            
            nickname = NAME_MAP.get(name)
            if not nickname:
                # print(f"Warning: No mapping for {name}")
                continue

            # --- SUS ---
            scores = []
            valid = True
            for i in range(10):
                match = re.search(r'\((\d)\)', row[3+i])
                if match:
                    val = int(match.group(1))
                    q_scores[i].append(val)
                    if (i+1)%2==1: scores.append(val-1)
                    else: scores.append(5-val)
                else: valid = False
            
            if valid:
                sus_total_scores.append(sum(scores)*2.5)

            # --- Task 1 ---
            task1 = row[13] # UFO or Wine
            if task1:
                is_rec = nickname_task_condition.get((nickname, task1))
                if is_rec is not None:
                    r_cond = "Rec" if is_rec else "No Rec"
                    v = parse_likert(row[14])
                    if v: metrics["readable"][r_cond].append(v)
                    v = parse_likert(row[15])
                    if v: metrics["requirements"][r_cond].append(v)
                    if is_rec:
                        v = parse_likert(row[16])
                        if v: metrics["rec_useful"].append(v)
                        v = parse_likert(row[17])
                        if v: metrics["rec_surprise"].append(v)

            # --- Task 2 ---
            if len(row) > 20 and row[20]:
                task2 = row[20]
                is_rec = nickname_task_condition.get((nickname, task2))
                if is_rec is not None:
                    r_cond = "Rec" if is_rec else "No Rec"
                    v = parse_likert(row[21])
                    if v: metrics["readable"][r_cond].append(v)
                    v = parse_likert(row[22])
                    if v: metrics["requirements"][r_cond].append(v)
                    if is_rec:
                        v = parse_likert(row[23])
                        if v: metrics["rec_useful"].append(v)
                        v = parse_likert(row[24])
                        if v: metrics["rec_surprise"].append(v)

    if sus_total_scores:
        print(f"Average Total SUS: {statistics.mean(sus_total_scores):.1f} (SD: {statistics.stdev(sus_total_scores):.1f})")
        
        print("\n--- Per Question SUS Scores (1-5 Scale) ---")
        sus_questions = [
            "Q1. 頻繁に使いたい",
            "Q2. 複雑すぎる",
            "Q3. 使いやすい",
            "Q4. 技術サポートが必要",
            "Q5. 機能が統合されている",
            "Q6. 矛盾が多い",
            "Q7. すぐに習得できる",
            "Q8. 扱いづらい",
            "Q9. 自信を持って使える",
            "Q10. 事前学習が必要"
        ]
        
        for i, q_text in enumerate(sus_questions):
            vals = q_scores[i]
            avg = statistics.mean(vals)
            sd = statistics.stdev(vals) if len(vals) > 1 else 0
            print(f"{q_text}: {avg:.2f} (SD: {sd:.2f})")
            
        print("\n=== Task Specific Survey Scores (1-5 Scale) ===")
        # Check if lists are empty before mean
        if metrics['readable']['Rec']:
            print("Readability (Rec):", f"{statistics.mean(metrics['readable']['Rec']):.2f}")
        if metrics['readable']['No Rec']:
            print("Readability (No Rec):", f"{statistics.mean(metrics['readable']['No Rec']):.2f}")
        if metrics['requirements']['Rec']:
            print("Requirements Met (Rec):", f"{statistics.mean(metrics['requirements']['Rec']):.2f}")
        if metrics['requirements']['No Rec']:
            print("Requirements Met (No Rec):", f"{statistics.mean(metrics['requirements']['No Rec']):.2f}")
        if metrics['rec_useful']:
            print("Rec Usefulness (Rec Only):", f"{statistics.mean(metrics['rec_useful']):.2f}")
        if metrics['rec_surprise']:
            print("Rec Surprise (Rec Only):", f"{statistics.mean(metrics['rec_surprise']):.2f}")

    else:
        print("No valid SUS scores")

if __name__ == "__main__":
    analyze()
