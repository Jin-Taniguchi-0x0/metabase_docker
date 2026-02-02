
import json
import collections
import glob
import os
from datetime import datetime

# Include current and archived logs
LOG_FILES = glob.glob('/Users/jin/metabase/TEST/python-app/logs/**/app_log.jsonl', recursive=True)

def parse_time(ts_str):
    if '.' in ts_str:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f")
    return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")

def analyze_participants():
    dashboards = {} 

    for log_file in LOG_FILES:
        with open(log_file, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except:
                    continue
                
                db_id = entry.get('dashboard_id')
                if not db_id:
                    continue
                
                if db_id not in dashboards:
                    dashboards[db_id] = {
                        'id': db_id,
                        'timestamps': [],
                        'task': None,
                        'rec_enabled': None,
                        'views_created': [],
                        'views_deleted': 0,
                        'final_view_count': 0
                    }
                
                db = dashboards[db_id]
                db['timestamps'].append(entry.get('timestamp'))
                
                # Identify Task
                if 'table_name' in entry:
                    tname = entry['table_name']
                    if tname == 'Wine Review':
                        db['task'] = 'Wine'
                    elif tname == 'Ufo Scrubbed':
                        db['task'] = 'UFO'
                
                # Identify Condition
                if 'recommendation_enabled' in entry:
                    db['rec_enabled'] = entry['recommendation_enabled']
                
                # Count Views
                if entry.get('action') == 'create_view':
                    c_type = entry.get('card_type')
                    # normalize
                    if c_type == 'pivot-table': c_type = 'pivot'
                    if c_type == 'scalar': c_type = 'value'
                    db['views_created'].append(c_type)
                    db['final_view_count'] += 1
                
                if entry.get('action') == 'delete_view':
                    db['views_deleted'] += 1
                    db['final_view_count'] -= 1

    print("ID | Task | Cond | Time(m) | Add | Del | Final | Unique | Types")
    print("---|---|---|---|---|---|---|---|---")

    for db_id, data in dashboards.items():
        if not data['task'] or not data['timestamps']:
            continue
            
        # Calculate duration
        times = [parse_time(t) for t in data['timestamps'] if t]
        if not times:
            continue
        start = min(times)
        end = max(times)
        duration_minutes = (end - start).total_seconds() / 60.0
        
        # Unique types
        unique_types = set(data['views_created'])
        unique_count = len(unique_types)
        
        cond = "Rec" if data['rec_enabled'] else "NoRec"
        
        print(f"{db_id} | {data['task']} | {cond} | {duration_minutes:.1f} | {len(data['views_created'])} | {data['views_deleted']} | {data['final_view_count']} | {unique_count} | {list(unique_types)}")

if __name__ == "__main__":
    analyze_participants()
