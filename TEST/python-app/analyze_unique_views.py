import json
import csv
import statistics
from datetime import datetime

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"
GROUP_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - グループ.csv"

def load_group_mapping():
    mapping = {} # dashboard_id -> {user, task, rec_condition}
    
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
            dash1 = row[1]
            task1 = row[2]
            rec1 = row[3]
            if dash1:
                mapping[str(dash1)] = {"user": user_name, "task": task1, "rec": rec1 == "あり"}
            
            # Task 2
            dash2 = row[4]
            task2 = row[5]
            rec2 = row[6]
            if dash2:
                mapping[str(dash2)] = {"user": user_name, "task": task2, "rec": rec2 == "あり"}

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
    group_mapping = load_group_mapping()
    logs = load_logs(LOG_FILE)
    
    dashboard_sessions = {}
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        if dash_id in group_mapping:
            if dash_id not in dashboard_sessions:
                dashboard_sessions[dash_id] = []
            dashboard_sessions[dash_id].append(log)
            
    results = []
    
    for dash_id, meta in group_mapping.items():
        events = dashboard_sessions.get(dash_id, [])
        if not events: continue
            
        # Unique view calculation
        unique_view_names = set()
        for e in events:
            if e.get('action') == 'create_view':
                name = e.get('card_name')
                if name:
                    unique_view_names.add(name)
        
        results.append({
            "task": meta['task'],
            "rec": meta['rec'],
            "unique_views": len(unique_view_names)
        })
        
    # Aggregate
    conditions = [
        ("UFO", False, "No Rec"),
        ("UFO", True, "Rec"),
        ("Wine", False, "No Rec"),
        ("Wine", True, "Rec")
    ]
    
    print("| Task | Condition | Mean Unique Views |")
    print("|---|---|---|")
    
    for task, is_rec, label in conditions:
        subset = [r for r in results if r['task'] == task and r['rec'] == is_rec]
        if not subset: continue
            
        vals = [r['unique_views'] for r in subset]
        avg = statistics.mean(vals)
        sd = statistics.stdev(vals) if len(vals) > 1 else 0
        
        print(f"| {task} | {label} | {avg:.2f} (SD: {sd:.2f}) |")

if __name__ == "__main__":
    analyze()
