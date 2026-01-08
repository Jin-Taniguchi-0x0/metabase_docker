import json
from collections import Counter

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"

# Groups
NO_REC_DASHBOARDS = ["3", "5", "8"]
REC_DASHBOARDS = ["4", "6", "7"]

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
    
    no_rec_views = []
    rec_views = []
    
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        action = log.get('action')
        
        if action == 'create_view':
            c_type = log.get('card_type', 'unknown')
            
            if dash_id in NO_REC_DASHBOARDS:
                no_rec_views.append(c_type)
            elif dash_id in REC_DASHBOARDS:
                rec_views.append(c_type)

    no_rec_counts = Counter(no_rec_views)
    rec_counts = Counter(rec_views)
    
    # Get all unique types
    all_types = sorted(list(set(no_rec_views + rec_views)))
    
    print("View Usage Comparison (Rec vs No Rec)")
    print("=" * 60)
    print(f"{'Chart Type':<20} | {'No Rec (3,5,8)':<15} | {'With Rec (4,6,7)':<15}")
    print("-" * 60)
    
    for c_type in all_types:
        nr = no_rec_counts.get(c_type, 0)
        wr = rec_counts.get(c_type, 0)
        print(f"{c_type:<20} | {nr:<15} | {wr:<15}")
        
    print("-" * 60)
    print(f"{'Total':<20} | {len(no_rec_views):<15} | {len(rec_views):<15}")
    print("\n")
    
    print("Detailed List:")
    print("No Rec Views:", no_rec_views)
    print("With Rec Views:", rec_views)

if __name__ == "__main__":
    analyze()
