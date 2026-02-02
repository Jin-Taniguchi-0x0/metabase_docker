import json
import collections

LOG_FILE = '/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl'

def analyze_dashboards():
    dashboards = {} # (user_id, dashboard_id) -> session_data

    with open(LOG_FILE, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line)
            except:
                continue
            
            user_id = entry.get('user_id')
            dashboard_id = entry.get('dashboard_id')
            if not user_id or not dashboard_id:
                continue
            
            key = (user_id, dashboard_id)
            if key not in dashboards:
                dashboards[key] = {
                    'condition': entry.get('recommendation_enabled'), # boolean
                    'views': [],
                    'task': None,
                    'actions': []
                }
            
            dashboards[key]['actions'].append(entry)

            # Determine task based on table creation or view creation
            if 'table_name' in entry:
                table = entry['table_name']
                if table == 'Wine Review':
                    dashboards[key]['task'] = 'Wine'
                elif table == 'Ufo Scrubbed':
                    dashboards[key]['task'] = 'UFO'
            
            # Track views
            if entry.get('action') == 'create_view':
                card_type = entry.get('card_type')
                dashboards[key]['views'].append(card_type)
            elif entry.get('action') == 'delete_view':
                # Remove the last matching view if possible, or just track deletions?
                # For simplicity, let's keep all created views to see what they TRIED to do, 
                # or strictly parse the final state. 
                # The user query implies "created View types" (Unique View Types).
                # Thesis says "Created View types".
                # I will store all created views for now, but also maybe handle deletions for "Final Dashboard" context?
                # Let's stick to "Created" as per "Unique View Types" metric usually counts what was stimulated.
                pass

    # Filter and find candidates
    wine_rec_candidates = []
    wine_norec_candidates = []
    ufo_rec_candidates = []

    print("--- Analysis Results ---")

    for key, data in dashboards.items():
        user, db_id = key
        task = data['task']
        condition = data['condition']
        views = data['views']
        unique_views = set(views)
        view_counts = collections.Counter(views)
        
        # Determine condition string
        cond_str = "Rec" if condition else "NoRec"

        if not task:
            continue

        info = f"User: {user}, DB: {db_id}, Task: {task}, Cond: {cond_str}, Views: {view_counts}, Unique: {len(unique_views)}"

        # 1. Wine Rec Diversity (Gauge/Scatter)
        if task == 'Wine' and condition:
            if 'gauge' in views or 'scatter' in views:
                wine_rec_candidates.append(info)
        
        # 2. Wine No Rec (Bar/Value dominance)
        if task == 'Wine' and not condition:
            # Check if mostly bar/scalar
            total = len(views)
            if total > 0:
                bar_scalar = view_counts['bar'] + view_counts['scalar']
                if bar_scalar / total > 0.8: # Arbitrary threshold for dominance
                    wine_norec_candidates.append(info)

        # 3. UFO Rec Pivot
        if task == 'UFO' and condition:
            if 'pivot-table' in views or 'pivot' in views:
                ufo_rec_candidates.append(info)

    print("\n[Candidate 1: Wine Rec with Gauge/Scatter]")
    for c in wine_rec_candidates:
        print(c)

    print("\n[Candidate 2: Wine No Rec dominated by Bar/Scalar]")
    for c in wine_norec_candidates:
        print(c)

    print("\n[Candidate 3: UFO Rec with Pivot]")
    for c in ufo_rec_candidates:
        print(c)

if __name__ == "__main__":
    analyze_dashboards()
