import json
import csv
import re
import statistics

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"
GROUP_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - グループ.csv"

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
            dash1 = row[1]
            task1 = row[2]
            rec1 = row[3] == "あり"
            if dash1: mapping[str(dash1)] = {"task": task1, "rec": rec1, "user": user_name}
            
            # Task 2
            dash2 = row[4]
            task2 = row[5]
            rec2 = row[6] == "あり"
            if dash2: mapping[str(dash2)] = {"task": task2, "rec": rec2, "user": user_name}
    return mapping

def load_logs(log_file):
    logs = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try: logs.append(json.loads(line))
            except: continue
    return logs

def extract_attributes(card_name):
    """
    Extracts potential attributes (columns) used in the view name.
    """
    if not card_name: return []
    
    # Generic parsing: split by spaces, "->", "別", "(", ")"
    # This is heuristic.
    # Improved heuristic: Look for capitalized words or known columns?
    # Or just tokens that look like columns.
    
    # Known columns from previous analysis:
    known_cols = [
        "Country", "Shape", "Duration Seconds", "Latitude", "Longitude", "Datetime", "City", "State", # UFO
        "Points", "Price", "Variety", "Province", "Winery", "Description", "Designation" # Wine
    ]
    
    found = set()
    for col in known_cols:
        if col in card_name:
            found.add(col)
            
    return list(found)

def calculate_complexity(card_name):
    """
    Estimates complexity based on name length and keywords indicating filters/aggregations.
    """
    if not card_name: return 0
    score = 1 # Base score
    
    # Filters often appear in parenthesis `(...)` or with `である`, `ではない`
    filters = card_name.count("である") + card_name.count("ではない") + card_name.count("範囲")
    score += filters
    
    # Aggregations / Groupings
    groupings = card_name.count("別")
    score += groupings
    
    # Multiple fields involved
    arrow_count = card_name.count("->")
    if arrow_count > 1: score += (arrow_count - 1) * 0.5
    
    return score

def analyze():
    group_map = load_group_mapping()
    logs = load_logs(LOG_FILE)
    
    dashboard_events = {}
    for l in logs:
        did = str(l.get('dashboard_id'))
        if did:
            if did not in dashboard_events: dashboard_events[did] = []
            dashboard_events[did].append(l)

    # Metrics storage
    # coverage_scores[(task, rec)] = [count_of_unique_attributes_per_session]
    # complexity_scores[(task, rec)] = [avg_complexity_per_session]
    
    coverage_data = {k: [] for k in [("UFO", False), ("UFO", True), ("Wine", False), ("Wine", True)]}
    complexity_data = {k: [] for k in [("UFO", False), ("UFO", True), ("Wine", False), ("Wine", True)]}
    
    for did, meta in group_map.items():
        key = (meta['task'], meta['rec'])
        events = dashboard_events.get(did, [])
        
        unique_attrs_in_session = set()
        complexity_sum = 0
        view_count = 0
        
        for e in events:
            if e.get('action') == 'create_view':
                name = e.get('card_name')
                attrs = extract_attributes(name)
                unique_attrs_in_session.update(attrs)
                
                comp = calculate_complexity(name)
                complexity_sum += comp
                view_count += 1
        
        if view_count > 0:
            coverage_data[key].append(len(unique_attrs_in_session))
            complexity_data[key].append(complexity_sum / view_count)

    # Calculate Personal Chart Variety
    variety_data = {k: [] for k in [("UFO", False), ("UFO", True), ("Wine", False), ("Wine", True)]}
    
    for did, meta in group_map.items():
        key = (meta['task'], meta['rec'])
        events = dashboard_events.get(did, [])
        types_in_session = set()
        for e in events:
             if e.get('action') == 'create_view':
                 ct = e.get('card_type')
                 if ct: types_in_session.add(ct)
        
        if types_in_session:
            variety_data[key].append(len(types_in_session))

    print("=== New Effectiveness Metrics ===")
    
    print("\n1. Personal Chart Variety (Mean Unique Chart Types per User)")
    print("| Task | Condition | Mean Variety | n |")
    print("|---|---|---|---|")
    for (task, rec), vals in variety_data.items():
        cond = "Rec" if rec else "No Rec"
        if vals:
            avg = statistics.mean(vals)
            print(f"| {task} | {cond} | {avg:.2f} | {len(vals)} |")
            
    print("\n2. View Complexity (Mean Score)")
    for (task, rec), vals in complexity_data.items():
        cond = "Rec" if rec else "No Rec"
        if vals:
            avg = statistics.mean(vals)
            print(f"| {task} | {cond} | {avg:.2f} |")

if __name__ == "__main__":
    analyze()
