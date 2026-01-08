import json
from datetime import datetime

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"

TARGETS = {
    "7": {"Condition": "Rec Available", "TaskOrder": 1, "ExpectedDataset": "UFO"},
    "8": {"Condition": "No Rec", "TaskOrder": 2, "ExpectedDataset": "Wine"}
}

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
    logs = load_logs(LOG_FILE)
    
    dashboard_sessions = {}
    
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        if dash_id in TARGETS:
            if dash_id not in dashboard_sessions:
                dashboard_sessions[dash_id] = []
            dashboard_sessions[dash_id].append(log)
            
    print("Analysis Report for Dashboards 7 & 8 (Refined)")
    print("=" * 60)
    
    # Sort by task order for display
    sorted_ids = sorted(TARGETS.keys(), key=lambda k: TARGETS[k]["TaskOrder"])
    
    results = []

    for dash_id in sorted_ids:
        meta = TARGETS[dash_id]
        events = dashboard_sessions.get(dash_id, [])
        
        if not events:
            results.append({
                "Dashboard": dash_id,
                "Dataset": meta["ExpectedDataset"],
                "Condition": meta["Condition"],
                "Duration": "N/A",
                "Views": 0,
                "Rec Usage": "N/A"
            })
            continue
            
        # Sort by timestamp
        events.sort(key=lambda x: x['timestamp'])
        
        start_time = datetime.fromisoformat(events[0]['timestamp'])
        end_time = datetime.fromisoformat(events[-1]['timestamp'])
        duration_sec = (end_time - start_time).total_seconds()
        duration_min = round(duration_sec / 60, 2)
        
        views_created = 0
        rec_used = 0
        custom_created = 0
        datasets = set()
        
        for e in events:
            if 'table_name' in e:
                datasets.add(e['table_name'])
                
            action = e.get('action')
            if action == 'create_view':
                views_created += 1
                source = e.get('recommendation_source', 'custom')
                if source == 'recommendation':
                    rec_used += 1
                else:
                    custom_created += 1
                    
        rec_usage_rate = 0.0
        if views_created > 0:
            rec_usage_rate = round((rec_used / views_created) * 100, 1)

        detected_dataset = ", ".join(datasets) if datasets else "Unknown"
        
        results.append({
            "Dashboard": dash_id,
            "Dataset": detected_dataset,
            "Condition": meta["Condition"],
            "Duration": f"{duration_min} min",
            "Views": views_created,
            "Rec Usage": f"{rec_usage_rate}% ({rec_used}/{views_created})"
        })

    # Print markdown table
    print("| Dashboard | Task | Dataset | Condition | Duration | Views | Rec Usage |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    for i, res in enumerate(results):
        print(f"| #{res['Dashboard']} | {i+1} | {res['Dataset']} | {res['Condition']} | {res['Duration']} | {res['Views']} | {res['Rec Usage']} |")
    print("\n")

if __name__ == "__main__":
    analyze()
