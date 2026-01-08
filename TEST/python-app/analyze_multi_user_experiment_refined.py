import json
from datetime import datetime
import pandas as pd

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"

USER_MAPPING = {
    "A": ["3", "4"],
    "B": ["5", "6"]
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
    
    # Group by Dashboard ID
    dashboard_sessions = {}
    
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        if dash_id not in dashboard_sessions:
            dashboard_sessions[dash_id] = []
        dashboard_sessions[dash_id].append(log)
        
    print("Experiment Analysis Report (Refined)")
    print("=" * 60)
    
    for user, dash_ids in USER_MAPPING.items():
        print(f"--- User {user} ---")
        
        for i, dash_id in enumerate(dash_ids):
            events = dashboard_sessions.get(dash_id, [])
            if not events:
                print(f"Dashboard {dash_id}: No logs found.")
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
            views_deleted = 0
            datasets = set()
            
            for e in events:
                # Track dataset
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
                    
                elif action == 'delete_view':
                    views_deleted += 1
            
            rec_usage_rate = 0
            if views_created > 0:
                rec_usage_rate = round((rec_used / views_created) * 100, 1)

            condition = "No Rec" if i == 0 else "With Rec"

            print(f"Dashboard {dash_id} (Task {i+1}: {condition}):")
            print(f"  Dataset: {', '.join(datasets)}")
            print(f"  Duration: {duration_min} min")
            print(f"  Rec Usage Rate: {rec_usage_rate}% ({rec_used}/{views_created})")
            print("-" * 30)
        print("\n")

if __name__ == "__main__":
    analyze()
