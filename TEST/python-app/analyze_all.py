import json
from datetime import datetime
import pandas as pd
import os
import collections

LOG_FILE = "logs/app_log.jsonl"

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

def analyze():
    logs = load_logs(LOG_FILE)
    if not logs:
        return

    # Group by Dashboard ID
    dashboards = {}
    
    for log in logs:
        dash_id = str(log.get('dashboard_id'))
        if not dash_id or dash_id == 'None':
            continue
            
        if dash_id not in dashboards:
            dashboards[dash_id] = []
        dashboards[dash_id].append(log)
        
    results = []
    
    # For Pattern Analysis
    card_type_patterns = {
        'Rec_Enabled': collections.Counter(),
        'Rec_Disabled': collections.Counter()
    }

    # Sort dashboards by timestamp of first event
    sorted_dash_ids = sorted(dashboards.keys(), key=lambda k: dashboards[k][0]['timestamp'])

    for dash_id in sorted_dash_ids:
        events = dashboards[dash_id]
        events.sort(key=lambda x: x['timestamp'])
        
        # Determine Dataset (most frequent table name)
        datasets = {}
        for e in events:
            if 'table_name' in e:
                t = e['table_name']
                datasets[t] = datasets.get(t, 0) + 1
        
        main_dataset = max(datasets, key=datasets.get) if datasets else "Unknown"
        
        # FILTER: Exclude Athlete Events
        if main_dataset == 'Athlete Events':
            continue

        start_time = datetime.fromisoformat(events[0]['timestamp'])
        end_time = datetime.fromisoformat(events[-1]['timestamp'])
        duration_min = round((end_time - start_time).total_seconds() / 60, 2)
        
        rec_enabled = events[0].get('recommendation_enabled', False)
        rec_str = "あり" if rec_enabled else "なし"
        
        views_created = 0
        views_deleted = 0
        rec_used = 0
        unique_view_names = set()
        
        for e in events:
            action = e.get('action')
            
            if action == 'create_view':
                views_created += 1
                unique_view_names.add(e.get('card_name'))
                
                # Track Card Types for Pattern Analysis
                c_type = e.get('card_type', 'unknown')
                if rec_enabled:
                    card_type_patterns['Rec_Enabled'][c_type] += 1
                else:
                    card_type_patterns['Rec_Disabled'][c_type] += 1
                
                if e.get('recommendation_source') == 'recommendation':
                    rec_used += 1
            
            elif action == 'delete_view':
                views_deleted += 1
        
        unique_views_count = len(unique_view_names)
        rec_rate = round((rec_used / views_created * 100), 1) if views_created > 0 else 0.0
        
        # Avg Time per View Creation (Total Duration / Views Created)
        # Note: This is a rough proxy as it includes idle time, but useful for comparison
        avg_time_per_view = round(duration_min / views_created, 2) if views_created > 0 else 0.0
        
        date_str = start_time.strftime('%Y-%m-%d')
        
        results.append({
            'id': dash_id,
            'date': date_str,
            'dataset': main_dataset,
            'rec_enabled': rec_enabled,
            'rec_str': rec_str,
            'duration': duration_min,
            'views_added': views_created,
            'views_deleted': views_deleted,
            'rec_used': rec_used,
            'rec_rate': rec_rate,
            'avg_time_per_view': avg_time_per_view,
            'unique_views': unique_views_count
        })

    # Output Tables
    print(f"分析対象ダッシュボード数 (Athlete Events除外): {len(results)}")
    
    if not results:
        print("表示するデータがありません。")
        return

    print("-" * 130)
    print(f"{'ID':<4} {'Dataset':<15} {'Rec':<5} {'時間(分)':<8} {'追加':<4} {'削除':<4} {'平均作成(分)':<12} {'Unique':<6} {'Rec使用':<8} {'率(%)':<6}")
    print("-" * 130)
    
    for r in results:
        print(f"{r['id']:<4} {r['dataset']:<15} {r['rec_str']:<5} {r['duration']:<8} {r['views_added']:<4} {r['views_deleted']:<4} {r['avg_time_per_view']:<12} {r['unique_views']:<6} {r['rec_used']:<8} {r['rec_rate']:<6}")

    # Aggregated Stats
    print("\n" + "="*30 + " 集計結果 " + "="*30)
    df = pd.DataFrame(results)
    
    # Group by Condition
    summary = df.groupby('rec_enabled').agg({
        'duration': 'mean',
        'views_added': 'mean',
        'views_deleted': 'mean',
        'avg_time_per_view': 'mean',
        'unique_views': 'mean',
        'rec_rate': 'mean'
    }).round(2)
    
    summary.index = ["Recなし", "Recあり"]
    print("\n【条件別平均値 (全体)】")
    print(summary)

    # Group by Dataset and Condition
    summary_task = df.groupby(['dataset', 'rec_enabled']).agg({
        'duration': 'mean',
        'views_added': 'mean',
        'views_deleted': 'mean',
        'avg_time_per_view': 'mean',
        'unique_views': 'mean',
        'rec_rate': 'mean'
    }).round(2)
    
    print("\n【タスク x 条件別平均値】")
    print(summary_task)


    # Pattern Analysis
    print("\n" + "="*30 + " Viewタイプ別作成数 (パターン分析) " + "="*30)
    
    all_types = set(list(card_type_patterns['Rec_Enabled'].keys()) + list(card_type_patterns['Rec_Disabled'].keys()))
    
    print(f"{'Card Type':<20} {'Recなし':<10} {'Recあり':<10} {'差分(あり-なし)':<15}")
    print("-" * 60)
    
    # Sort by total count desc
    sorted_types = sorted(all_types, key=lambda t: card_type_patterns['Rec_Enabled'][t] + card_type_patterns['Rec_Disabled'][t], reverse=True)
    
    for t in sorted_types:
        count_rec = card_type_patterns['Rec_Enabled'][t]
        count_no_rec = card_type_patterns['Rec_Disabled'][t]
        diff = count_rec - count_no_rec
        print(f"{t:<20} {count_no_rec:<10} {count_rec:<10} {diff:<15}")

if __name__ == "__main__":
    analyze()
