
import json
import collections

# Path to log file
LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"

# Chart type mapping
TYPE_MAP = {
    "visual-barChart": "Bar (棒グラフ)",
    "visual-pieChart": "Pie (円グラフ)",
    "visual-donutChart": "Pie (円グラフ)", # Mapping donut to Pie as per Table 2
    "visual-lineChart": "Line (折れ線)",
    "visual-scalar": "Scalar (数値)",
    "visual-map": "Map (地図)",
    "visual-pivotTable": "Pivot (ピボット)",
    "visual-gauge": "Gauge (ゲージ)",
    "visual-scatterChart": "Scatter (散布図)",
    "visual-areaChart": "Area (エリア)", # New
    "visual-funnel": "Funnel (ファンネル)", # New
    "visual-waterfallChart": "Waterfall (ウォーターフォール)", # New
    "visual-table": "Table (テーブル)" # New
}

# Task ID mapping (from table_id/name inference)
# 10: Athlete (Training)
# 11: UFO
# 12: Wine
TABLE_MAP = {
    "Ufo Scrubbed": "UFO",
    "Wine Review": "Wine",
    "Athlete Events": "Training"
}

def analyze():
    # Store task for each dashboard
    dashboard_tasks = {}
    
    # Store counts: task -> chart_type -> count
    rec_counts = collections.defaultdict(lambda: collections.Counter())
    
    # Track which recommendations were already counted for a specific state to avoid over-counting?
    # For now, simply count all occurrences as "Impressions"
    
    with open(LOG_FILE, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line)
            except:
                continue
                
            dash_id = entry.get('dashboard_id')
            action = entry.get('action')
            
            # 1. Identify Task for Dashboard
            if action == 'select_table' or action == 'create_view':
                table = entry.get('table_name')
                if table in TABLE_MAP:
                    task = TABLE_MAP[table]
                    # Update dashboard task (last seen table might define current context)
                    if task != "Training":
                        dashboard_tasks[dash_id] = task
            
            # 2. Count Recommendations
            if action == 'generate_recommendations':
                # Check if recommendation was enabled
                if not entry.get('recommendation_enabled'):
                    continue
                    
                # Get task
                task = dashboard_tasks.get(dash_id)
                if not task:
                    continue # Skip if task unknown or Training
                    
                recs = entry.get('recommendations', [])
                for r in recs:
                    human_type = TYPE_MAP.get(r, r)
                    rec_counts[task][human_type] += 1

    # Print results
    print("Recommendation Counts by Task (Impressions):")
    for task in ["UFO", "Wine"]:
        print(f"\nTask: {task}")
        total = sum(rec_counts[task].values())
        print(f"Total Recommendations Generated: {total}")
        for ctype, count in rec_counts[task].most_common():
            print(f"{ctype}: {count}")

if __name__ == "__main__":
    analyze()
