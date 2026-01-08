import json
from datetime import datetime
import pandas as pd

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"

# Configuration for context
DASHBOARD_META = {
    "3": {"User": "A", "Task": 1, "Rec": False, "Dataset": "UFO"},
    "4": {"User": "A", "Task": 2, "Rec": True, "Dataset": "Wine"},
    "5": {"User": "B", "Task": 1, "Rec": False, "Dataset": "Wine"},
    "6": {"User": "B", "Task": 2, "Rec": True, "Dataset": "UFO"},
    "7": {"User": "C", "Task": 1, "Rec": True, "Dataset": "UFO"},
    "8": {"User": "C", "Task": 2, "Rec": False, "Dataset": "Wine"}
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
    
    # Calculate durations per dashboard
    dash_durations = {}
    
    # Group logs by dashboard
    dash_logs = {}
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        if dash_id in DASHBOARD_META:
            if dash_id not in dash_logs:
                dash_logs[dash_id] = []
            dash_logs[dash_id].append(log)
            
    # Compute duration
    for dash_id, events in dash_logs.items():
        events.sort(key=lambda x: x['timestamp'])
        start = datetime.fromisoformat(events[0]['timestamp'])
        end = datetime.fromisoformat(events[-1]['timestamp'])
        duration = (end - start).total_seconds() / 60 # minutes
        dash_durations[dash_id] = duration

    print("Experiment Reduction Rate Analysis")
    print("=" * 60)
    
    # 1. Task 1 -> Task 2 Reduction Rate per User
    print("### Task 1 -> Task 2 Time Reduction Rate")
    users = set(m["User"] for m in DASHBOARD_META.values())
    
    for user in sorted(users):
        d1 = next(d for d, m in DASHBOARD_META.items() if m["User"] == user and m["Task"] == 1)
        d2 = next(d for d, m in DASHBOARD_META.items() if m["User"] == user and m["Task"] == 2)
        
        t1 = dash_durations.get(d1, 0)
        t2 = dash_durations.get(d2, 0)
        
        if t1 == 0: continue
        
        reduction = (t1 - t2)
        reduction_rate = (reduction / t1) * 100
        
        sign = "-" if reduction < 0 else "+"
        # If reduction is negative, it means time increased.
        # "Reduction Rate" usually implies positive is good (time saved). 
        # If time increased, rate is negative.
        
        print(f"User {user}: {t1:.2f} min -> {t2:.2f} min | Reduction: {reduction_rate:.1f}%")
        
    print("\n")
    
    # 2. Rec vs No Rec Impact (Grouped by Dataset)
    print("### Impact of Recommendation (by Dataset)")
    
    datasets = set(m["Dataset"] for m in DASHBOARD_META.values())
    
    for ds in sorted(datasets):
        rec_times = []
        no_rec_times = []
        
        for d, m in DASHBOARD_META.items():
            if m["Dataset"] == ds:
                t = dash_durations.get(d, 0)
                if t == 0: continue
                
                if m["Rec"]:
                    rec_times.append(t)
                else:
                    no_rec_times.append(t)
        
        if not rec_times or not no_rec_times:
            print(f"Dataset {ds}: Insufficient data for comparison")
            continue
            
        avg_rec = sum(rec_times) / len(rec_times)
        avg_no_rec = sum(no_rec_times) / len(no_rec_times)
        
        diff = avg_no_rec - avg_rec
        rate = (diff / avg_no_rec) * 100
        
        print(f"Dataset {ds}:")
        print(f"  Avg No Rec: {avg_no_rec:.2f} min ({no_rec_times})")
        print(f"  Avg Rec:    {avg_rec:.2f} min ({rec_times})")
        print(f"  Reduction:  {rate:.1f}%")
        print("-" * 30)

if __name__ == "__main__":
    analyze()
