import json
from datetime import datetime

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"
TARGET_DASHBOARD_ID = "5"

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
    
    events = []
    for log in logs:
        # Filter for dashboard 5
        # Also strictly ensure we are looking at recent logs if needed, 
        # but dashboard_id is a strong enough filter for a specific task session usually.
        # Assuming the user just did this, so it should be at the end of the file.
        if str(log.get('dashboard_id')) == TARGET_DASHBOARD_ID:
            events.append(log)
            
    if not events:
        print(f"No logs found for Dashboard {TARGET_DASHBOARD_ID}")
        return

    # Sort by timestamp
    events.sort(key=lambda x: x['timestamp'])
    
    start_time = datetime.fromisoformat(events[0]['timestamp'])
    end_time = datetime.fromisoformat(events[-1]['timestamp'])
    duration = (end_time - start_time).total_seconds()
    
    # Analyze actions
    views_created_custom = 0
    views_created_rec = 0
    views_deleted = 0
    chart_types = {}
    
    for e in events:
        action = e.get('action')
        
        if action == 'create_view':
            source = e.get('recommendation_source', 'custom')
            if source == 'recommendation':
                views_created_rec += 1
            else:
                views_created_custom += 1
                
            c_type = e.get('card_type', 'unknown')
            chart_types[c_type] = chart_types.get(c_type, 0) + 1
            
        elif action == 'delete_view':
            views_deleted += 1

    # Output Summary
    print(f"Summary for Dashboard {TARGET_DASHBOARD_ID}")
    print("=" * 40)
    print(f"Date: {start_time.strftime('%Y-%m-%d')}")
    print(f"Time: {start_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}")
    print(f"Duration: {round(duration / 60, 2)} min")
    print("-" * 40)
    print(f"Total Views Created: {views_created_custom + views_created_rec}")
    print(f"  - Manual (Custom): {views_created_custom}")
    print(f"  - AI Recommendation: {views_created_rec}")
    print(f"Views Deleted: {views_deleted}")
    print("-" * 40)
    print(f"Chart Types Used:")
    for c_type, count in chart_types.items():
        print(f"  - {c_type}: {count}")
    print("=" * 40)

if __name__ == "__main__":
    analyze()
