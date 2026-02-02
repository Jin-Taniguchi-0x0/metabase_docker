
import json

LOG_FILE = '/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl'
# 23: Wine (Rec) - Hanawa
# 24: UFO (No Rec) - Hanawa
TARGETS = [
    {'id': '23', 'name': 'Wine Task (Rec)'},
    {'id': '24', 'name': 'UFO Task (No Rec)'}
]

TYPE_MAP = {
    "visual-barChart": "Bar",
    "visual-pieChart": "Pie",
    "visual-donutChart": "Donut",
    "visual-lineChart": "Line",
    "visual-scalar": "Scalar",
    "visual-map": "Map",
    "visual-pivotTable": "Pivot",
    "visual-gauge": "Gauge",
    "visual-scatterChart": "Scatter",
    "visual-areaChart": "Area",
    "visual-funnel": "Funnel",
    "visual-waterfallChart": "Waterfall",
    "visual-table": "Table"
}

def analyze():
    events_by_db = {t['id']: [] for t in TARGETS}
    
    with open(LOG_FILE, 'r') as f:
        for line in f:
            try:
                e = json.loads(line)
            except:
                continue
            db_id = e.get('dashboard_id')
            if db_id in events_by_db:
                events_by_db[db_id].append(e)

    for target in TARGETS:
        db_id = target['id']
        print(f"\n=== {target['name']} (DB: {db_id}) ===")
        
        current_recs = []
        
        # We need to track the flow.
        # Recommendation enabled?
        # If enabled, generate_recommendations updates available choices.
        # create_view indicates a choice.
        
        events = events_by_db[db_id]
        if not events:
            print("No events found.")
            continue
            
        is_rec_enabled = events[0].get('recommendation_enabled', False)
        print(f"rec_enabled: {is_rec_enabled}")
        
        for e in events:
            action = e.get('action')
            
            if action == 'generate_recommendations':
                raw_recs = e.get('recommendations', [])
                current_recs = [TYPE_MAP.get(r, r) for r in raw_recs]
                
            elif action == 'create_view':
                card_name = e.get('card_name')
                card_type = e.get('card_type')
                source = e.get('recommendation_source')
                
                print(f"\n[Selected View]")
                print(f"  Type: {card_type}")
                print(f"  Title: {card_name}")
                print(f"  Source: {source}")
                
                if is_rec_enabled:
                    if current_recs:
                        print(f"  [Visible Recommendations]: {', '.join(current_recs)}")
                    else:
                        print(f"  [Visible Recommendations]: (None generated yet or empty)")
                else:
                    print(f"  [Visible Recommendations]: N/A (No Rec)")

if __name__ == "__main__":
    analyze()
