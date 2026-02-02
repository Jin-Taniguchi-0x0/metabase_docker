
import json

LOG_FILE = '/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl'
TARGET_DASHBOARDS = ['25', '26', '19', '20', '12', '5'] # Itai, Yugo, Takumi, Sako

def print_details():
    dashboards = {id: [] for id in TARGET_DASHBOARDS}
    
    with open(LOG_FILE, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line)
            except:
                continue
            
            db_id = entry.get('dashboard_id')
            if db_id in TARGET_DASHBOARDS:
                if entry.get('action') == 'create_view':
                    dashboards[db_id].append(entry)

    for db_id in TARGET_DASHBOARDS:
        print(f"\n=== Dashboard {db_id} ===")
        events = dashboards[db_id]
        if not events:
            print("No views found.")
            continue
            
        for e in events:
            # We want chart_type (Japanese) and card_name (Title)
            # chart_type is in 'click_create_view' usually, but 'create_view' has 'card_name' and 'card_type'
            # Let's see if we can get the japanese name from card_name or just use card_type
            c_type = e.get('card_type')
            c_name = e.get('card_name')
            rec_source = e.get('recommendation_source')
            print(f"- Type: {c_type}, Source: {rec_source}")
            print(f"  Title: {c_name}")

if __name__ == "__main__":
    print_details()
