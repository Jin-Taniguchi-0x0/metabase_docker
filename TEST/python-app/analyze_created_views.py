import json
import csv
from collections import Counter

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
            if dash1: mapping[str(dash1)] = {"task": task1, "rec": rec1}
            
            # Task 2
            dash2 = row[4]
            task2 = row[5]
            rec2 = row[6] == "あり"
            if dash2: mapping[str(dash2)] = {"task": task2, "rec": rec2}
    return mapping

def load_logs(log_file):
    logs = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try: logs.append(json.loads(line))
            except: continue
    return logs

def analyze():
    group_map = load_group_mapping()
    logs = load_logs(LOG_FILE)
    
    # Store data: {(task, rec) -> {'types': Counter, 'names': set}}
    data = {
        ("UFO", False): {'types': Counter(), 'names': set()},
        ("UFO", True):  {'types': Counter(), 'names': set()},
        ("Wine", False): {'types': Counter(), 'names': set()},
        ("Wine", True):  {'types': Counter(), 'names': set()},
    }
    
    # Track deleted view IDs to exclude them if desired? 
    # User asked "What kind of view added", so maybe final counts or valid adds?
    # Usually "Created" is interesting even if deleted. 
    # But let's look at "Valid" ones? 
    # Step 208 Request: "View addition and deletion counts..." 
    # Current Request: "Summary of what kind of views were added".
    # I will count ALL "create_view" actions effectively. 
    # If a user created 3 bar charts, I count 3. 
    
    dashboard_events = {}
    for l in logs:
        did = str(l.get('dashboard_id'))
        if did:
            if did not in dashboard_events: dashboard_events[did] = []
            dashboard_events[did].append(l)
            
    for did, meta in group_map.items():
        key = (meta['task'], meta['rec'])
        events = dashboard_events.get(did, [])
        
        for e in events:
            if e.get('action') == 'create_view':
                ctype = e.get('card_type')
                name = e.get('card_name')
                
                if ctype: data[key]['types'][ctype] += 1
                if name: data[key]['names'].add(name) # Unique names per condition aggregation

    print("=== View Content Analysis (Rec vs No Rec) ===")
    
    # Translation for Chart Types
    type_map = {
        "bar": "棒グラフ",
        "line": "折れ線",
        "row": "横棒",
        "area": "エリア",
        "pie": "円グラフ",
        "scalar": "数値",
        "funnel": "ファンネル",
        "map": "地図",
        "scatter": "散布図",
        "table": "テーブル",
        "pivot": "ピボット",
        "combo": "コンボ",
        "gauge": "ゲージ"
    }

    for (task, is_rec), info in sorted(data.items()):
        cond = "Rec" if is_rec else "No Rec"
        print(f"\n--- {task} ({cond}) [Total: {sum(info['types'].values())}] ---")
        
        # 1. Chart Types
        print("Chart Types:")
        total = sum(info['types'].values())
        sorted_types = info['types'].most_common()
        for t, count in sorted_types:
            t_jp = type_map.get(t, t)
            pct = (count / total * 100) if total > 0 else 0
            print(f"  {t_jp:<10}: {count} ({pct:.1f}%)")
            
        # 2. Semantic Analysis (from Card Name)
        # Typical format: "ChartType: Table -> Dimension別 Metric" or something similar
        # Let's try to extract "Dimension" (group by) and "Metric"
        
        dimensions = Counter()
        metrics = Counter()
        
        for name in info['names']:
            # Heuristic parsing based on Japanese generated names
            # e.g., "棒グラフ: Athlete Events -> Medal別 行のカウント"
            if "->" in name:
                parts = name.split("->")
                if len(parts) >= 2:
                    # Last part usually has the logic: "Medal別 行のカウント"
                    logic_part = parts[-1].strip()
                    
                    # Extract Dimension (ends with 別)
                    if "別" in logic_part:
                        dim_part = logic_part.split("別")[0]
                        # Cleanup (sometimes has table name prefix again?)
                        dimensions[dim_part] + 1
                        
                        # Extract Metric (after 別)
                        metric_part = logic_part.split("別")[-1].strip()
                        metrics[metric_part] += 1
                    else:
                        # Maybe just a metric? e.g. "行のカウント"
                        metrics[logic_part] += 1
            else:
                # Fallback
                dimensions["(Unknown)"] += 1

        # Heuristic didn't work well because logic is complex, let's just print top 10 raw names for manual summary if needed
        # Or better, just print frequent keywords?
        
        print("Top Dimensions (Parsed/Guessed):")
        # Extract keywords appearing before "別"
        dims = Counter()
        for name in info['names']:
            if "別" in name:
                # " ... Shape別 ..."
                # Find the word immediately preceding "別"
                try:
                    pre_part = name.split("別")[0]
                    # Take last word of pre_part? 
                    # Often "Ufo Scrubbed -> Shape"
                    dim = pre_part.split("->")[-1].strip()
                    dims[dim] += 1
                except: pass
        
        for d, c in dims.most_common(5):
             print(f"  {d}: {c}")

        print("Top Metrics/Focus:")
        # Look for aggregation keywords
        aggs = Counter()
        keywords = ["行のカウント", "平均", "合計", "最大", "最小"]
        for name in info['names']:
            for k in keywords:
                if k in name:
                    aggs[k] += 1
        for a, c in aggs.most_common():
            print(f"  {a}: {c}")

        # Specific check for Scatter/Map details?
        # Map usually involves Latitude/Longitude but knowing the focus (City/State) is good
        if task == "UFO":
             # Check State vs Country vs Shape
             focus = Counter()
             for k in ["State", "Country", "Shape", "City"]:
                 count = sum(1 for n in info['names'] if k in n)
                 if count > 0: focus[k] = count
             print("Key Fields Utilized:", focus)
        elif task == "Wine":
             # Check Price, Points, Country, Variety
             focus = Counter()
             for k in ["Price", "Points", "Country", "Variety", "Province"]:
                 count = sum(1 for n in info['names'] if k in n)
                 if count > 0: focus[k] = count
             print("Key Fields Utilized:", focus)

if __name__ == "__main__":
    analyze()
