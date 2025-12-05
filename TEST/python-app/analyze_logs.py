import json
import os
import pandas as pd
from datetime import datetime, timedelta
import argparse

LOG_FILE = "logs/app_log.jsonl"
OUTPUT_CSV = "logs/analysis_summary.csv"
OUTPUT_HISTORY = "logs/user_history.txt"

def load_logs(log_file):
    logs = []
    if not os.path.exists(log_file):
        print(f"Log file not found: {log_file}")
        return logs
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return logs

def analyze_sessions(logs):
    # Sort logs by timestamp
    logs.sort(key=lambda x: x['timestamp'])
    
    sessions = {}
    
    # Simple sessionization: Group by user_id, split by 'login' event or long pause
    # For now, let's assume 'login' starts a new session.
    
    current_session_id = 0
    
    for log in logs:
        user_id = log.get('user_id', 'unknown')
        action = log.get('action')
        timestamp = datetime.fromisoformat(log['timestamp'])
        
        if action == 'login':
            current_session_id += 1
            
        session_key = f"{user_id}_{current_session_id}"
        
        if session_key not in sessions:
            sessions[session_key] = {
                'user_id': user_id,
                'start_time': timestamp,
                'end_time': timestamp,
                'events': [],
                'rec_enabled': log.get('recommendation_enabled', True)
            }
        
        session = sessions[session_key]
        session['events'].append(log)
        session['end_time'] = timestamp
        # Update rec_enabled if it changes (though it shouldn't usually)
        if 'recommendation_enabled' in log:
            session['rec_enabled'] = log['recommendation_enabled']

    return sessions

def calculate_metrics(sessions):
    metrics = []
    
    for session_id, data in sessions.items():
        events = data['events']
        
        # Initialize counters
        view_created_total = 0
        view_created_rec = 0
        view_created_custom = 0
        
        view_deleted = 0
        
        durations_rec = []
        durations_custom = []
        
        for event in events:
            action = event.get('action')
            details = event
            
            if action == 'create_view':
                view_created_total += 1
                source = details.get('recommendation_source', 'custom')
                duration = details.get('task_duration_sec')
                
                if source == 'recommendation':
                    view_created_rec += 1
                    if duration: durations_rec.append(duration)
                else:
                    view_created_custom += 1
                    if duration: durations_custom.append(duration)
                    
            elif action == 'delete_view':
                view_deleted += 1
                
        # Calculate averages
        avg_time_rec = sum(durations_rec) / len(durations_rec) if durations_rec else 0
        avg_time_custom = sum(durations_custom) / len(durations_custom) if durations_custom else 0
        
        session_duration = (data['end_time'] - data['start_time']).total_seconds()
        
        metrics.append({
            'Session ID': session_id,
            'User ID': data['user_id'],
            'Rec Enabled': data['rec_enabled'],
            'Start Time': data['start_time'],
            'End Time': data['end_time'],
            'Duration (sec)': session_duration,
            'Total Views Created': view_created_total,
            'Rec Views Created': view_created_rec,
            'Custom Views Created': view_created_custom,
            'Views Deleted': view_deleted,
            'Avg Time Rec (sec)': avg_time_rec,
            'Avg Time Custom (sec)': avg_time_custom
        })
        
    return pd.DataFrame(metrics)

def generate_history_report(sessions, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        for session_id, data in sessions.items():
            f.write(f"=== Session: {session_id} (User: {data['user_id']}) ===\n")
            f.write(f"Start: {data['start_time']}, End: {data['end_time']}\n")
            f.write(f"Recommendation Enabled: {data['rec_enabled']}\n")
            f.write("Events:\n")
            for event in data['events']:
                timestamp = event['timestamp']
                action = event['action']
                # Format details nicely
                details = {k: v for k, v in event.items() if k not in ['timestamp', 'action', 'user_id', 'recommendation_enabled']}
                f.write(f"  [{timestamp}] {action}: {details}\n")
            f.write("\n")

def main():
    print(f"Reading logs from {LOG_FILE}...")
    logs = load_logs(LOG_FILE)
    
    if not logs:
        print("No logs found.")
        return

    print(f"Found {len(logs)} log entries.")
    
    sessions = analyze_sessions(logs)
    print(f"Identified {len(sessions)} sessions.")
    
    df = calculate_metrics(sessions)
    
    # Save CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Metrics saved to {OUTPUT_CSV}")
    
    # Save History
    generate_history_report(sessions, OUTPUT_HISTORY)
    print(f"History report saved to {OUTPUT_HISTORY}")
    
    # Display summary
    print("\n--- Summary ---")
    print(df.describe())

if __name__ == "__main__":
    main()
