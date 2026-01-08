import json
from datetime import datetime

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"

# Groups
NO_REC_DASHBOARDS = ["3", "5", "8"]
REC_DASHBOARDS = ["4", "6", "7"]

ADVANCED_CHARTS = ["map", "gauge", "waterfall", "line"]

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
    
    group_stats = {
        "No Rec": {"dashboards": NO_REC_DASHBOARDS, "unique_types": [], "advanced_count": 0, "total_views": 0},
        "With Rec": {"dashboards": REC_DASHBOARDS, "unique_types": [], "advanced_count": 0, "total_views": 0}
    }
    
    # Store types per dashboard
    dash_types = {}
    
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        
        # Determine group
        group = None
        if dash_id in NO_REC_DASHBOARDS:
            group = "No Rec"
        elif dash_id in REC_DASHBOARDS:
            group = "With Rec"
        
        if group:
            if dash_id not in dash_types:
                dash_types[dash_id] = set()
            
            action = log.get('action')
            if action == 'create_view':
                c_type = log.get('card_type', 'unknown')
                dash_types[dash_id].add(c_type)
                
                group_stats[group]["total_views"] += 1
                if c_type in ADVANCED_CHARTS:
                    group_stats[group]["advanced_count"] += 1

    # Calculate diversity averages
    for group, data in group_stats.items():
        unique_counts = []
        for d in data["dashboards"]:
            types = dash_types.get(d, set())
            unique_counts.append(len(types))
            print(f"Debug: {group} Dash {d} Types: {types}")
            
        avg_unique = sum(unique_counts) / len(unique_counts) if unique_counts else 0
        data["avg_unique_types"] = avg_unique
    
    print("Recommendation Effectiveness Analysis (Diversity & Quality)")
    print("=" * 60)
    
    for group, data in group_stats.items():
        print(f"[{group}]")
        print(f"  Avg Unique Chart Types: {data['avg_unique_types']:.2f}")
        print(f"  Advanced Charts Created: {data['advanced_count']} (Map, Gauge, Waterfall, Line)")
        print(f"  Total Views Created: {data['total_views']}")
        if data['total_views'] > 0:
            adv_rate = (data['advanced_count'] / data['total_views']) * 100
            print(f"  Advanced Chart Rate: {adv_rate:.1f}%")
        print("-" * 30)

if __name__ == "__main__":
    analyze()
