import json
import pandas as pd
from datetime import datetime

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/archive_20251209_085844/app_log.jsonl"

def load_logs(log_file):
    logs = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return logs

def classify_session(log):
    timestamp_str = log['timestamp']
    dt = datetime.fromisoformat(timestamp_str)
    dashboard_id = str(log.get('dashboard_id'))
    
    if dt.date() == datetime(2025, 12, 8).date():
        if dashboard_id == '2':
            return "Task 1 (No Rec)"
        elif dashboard_id == '3':
            return "Task 2 (With Rec)"
    elif dt.date() == datetime(2025, 12, 9).date():
        return "User Test"
    
    return "Unknown"

def analyze():
    logs = load_logs(LOG_FILE)
    
    sessions = {
        "Task 1 (No Rec)": {"events": [], "start": None, "end": None},
        "Task 2 (With Rec)": {"events": [], "start": None, "end": None},
        "User Test": {"events": [], "start": None, "end": None}
    }
    
    for log in logs:
        category = classify_session(log)
        if category in sessions:
            sessions[category]["events"].append(log)
            
    # Calculate metrics
    results = []
    
    for name, data in sessions.items():
        events = data["events"]
        if not events:
            continue
            
        # Sort by timestamp just in case
        events.sort(key=lambda x: x['timestamp'])
        
        start_time = datetime.fromisoformat(events[0]['timestamp'])
        end_time = datetime.fromisoformat(events[-1]['timestamp'])
        duration = (end_time - start_time).total_seconds()
        
        # Counters
        views_created = 0
        views_deleted = 0
        rec_sources = {"custom": 0, "recommendation": 0}
        chart_types = {}
        
        for e in events:
            action = e.get('action')
            if action == 'create_view':
                views_created += 1
                source = e.get('recommendation_source', 'custom')
                rec_sources[source] = rec_sources.get(source, 0) + 1
                
                c_type = e.get('card_type', 'unknown')
                chart_types[c_type] = chart_types.get(c_type, 0) + 1
                
            elif action == 'delete_view':
                views_deleted += 1
        
        results.append({
            "Session": name,
            "Date": start_time.strftime('%Y-%m-%d'),
            "Duration (min)": round(duration / 60, 2),
            "Views Created": views_created,
            "Views Deleted": views_deleted,
            "Chart Types": chart_types,
            "Rec Usage": rec_sources
        })
        
    # Print Report
    print(f"Analysis Report for {LOG_FILE}")
    print("="*60)
    
    for res in results:
        print(f"Session: {res['Session']}")
        print(f"  Date: {res['Date']}")
        print(f"  Duration: {res['Duration (min)']} min")
        print(f"  Views Created: {res['Views Created']}")
        print(f"    - Custom: {res['Rec Usage']['custom']}")
        print(f"    - Recommendation: {res['Rec Usage']['recommendation']}")
        print(f"  Views Deleted: {res['Views Deleted']}")
        print(f"  Chart Types Used: {res['Chart Types']}")
        print("-" * 40)

if __name__ == "__main__":
    analyze()
