import json
import csv
import statistics
import unicodedata
from datetime import datetime

LOG_FILE = "/Users/jin/metabase/TEST/python-app/logs/app_log.jsonl"
SURVEY_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - フォームの回答.csv"
GROUP_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - グループ.csv"

NAME_MAP = {
    "芹澤尚舜": "せり",
    "峪紳大朗": "さこ",
    "佐竹宏紀": "ひろくん",
    "今村真沙斗": "まさと",
    "宮澤匠": "たくみ",
    "鈴木俊詞": "しゅんじ",
    "永沼翔翼": "つばさ",
    "田中 翔太郎": "たなか",
    "岡本悠吾": "ゆうご",
    "矢野温加": "やん",
    "塙裕貴": "はなわ",
    "板井孝樹": "いたい"
}

def char_width(char):
    w = unicodedata.east_asian_width(char)
    if w in ('W', 'F'): return 2
    return 1

def str_width(text):
    return sum(char_width(c) for c in text)

def pad_to_width(text, width):
    current_width = str_width(text)
    padding = width - current_width
    return text + ' ' * padding

def format_table(headers, rows):
    col_widths = [0] * len(headers)
    for i, h in enumerate(headers):
        col_widths[i] = max(col_widths[i], str_width(h))
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], str_width(str(cell)))
    
    separator = "|"
    for w in col_widths: separator += " " + "-" * w + " |"
    
    header_line = "|"
    for i, h in enumerate(headers):
        header_line += " " + pad_to_width(h, col_widths[i]) + " |"
        
    row_lines = []
    for row in rows:
        line = "|"
        for i, cell in enumerate(row):
            line += " " + pad_to_width(str(cell), col_widths[i]) + " |"
        row_lines.append(line)
        
    return header_line + "\n" + separator + "\n" + "\n".join(row_lines)

def load_group_mapping():
    mapping = {} 
    with open(GROUP_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
        header_idx = -1
        for i, row in enumerate(lines):
            if len(row) > 1 and row[1] == 'id':
                header_idx = i
                break
        
        for row in lines[header_idx+1:]:
            if len(row) < 7: continue
            user_name = row[0]
            if not user_name: continue
            
            # Use columns directly. Note: Task name in Group CSV might differ slightly from Survey CSV?
            # Group CSV: UFO, Wine
            # Survey CSV: UFO, Wine
            mapping[user_name] = {
                "task1": {"dash": row[1], "task": row[2], "rec": row[3] == "あり"},
                "task2": {"dash": row[4], "task": row[5], "rec": row[6] == "あり"}
            }
    return mapping

def load_logs(log_file):
    logs = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try: logs.append(json.loads(line))
            except: continue
    return logs

def parse_likert(val):
    import re
    match = re.search(r'\((\d)\)', val)
    return int(match.group(1)) if match else None

def analyze():
    group_map = load_group_mapping()
    logs = load_logs(LOG_FILE)
    
    # 1. Process Logs per Dashboard
    dash_metrics = {} # dash_id -> {duration, views, unique, rec_rate}
    
    # Group logs by dashboard
    dash_logs = {}
    for l in logs:
        did = str(l.get('dashboard_id'))
        if did:
            if did not in dash_logs: dash_logs[did] = []
            dash_logs[did].append(l)
            
    for did, events in dash_logs.items():
        events.sort(key=lambda x: x['timestamp'])
        start = datetime.fromisoformat(events[0]['timestamp'])
        end = datetime.fromisoformat(events[-1]['timestamp'])
        duration = (end - start).total_seconds() / 60
        
        views_created = 0 # Added
        views_deleted = 0 # Deleted
        rec_used = 0
        unique_names = set()
        
        for e in events:
            if e.get('action') == 'create_view':
                views_created += 1
                if e.get('recommendation_source') == 'recommendation':
                    rec_used += 1
                name = e.get('card_name')
                if name: unique_names.add(name)
            elif e.get('action') == 'delete_view':
                views_deleted += 1
        
        final_views = views_created - views_deleted
        rec_rate = (rec_used / views_created * 100) if views_created > 0 else 0
        
        dash_metrics[did] = {
            "duration": duration,
            "added": views_created,
            "deleted": views_deleted,
            "final": final_views,
            "unique": len(unique_names),
            "rec_rate": rec_rate
        }

    # 2. Process Survey
    user_survey_data = {} # nickname -> {sus, task_metrics: {(task, rec) -> {readability, ...}}}
    
    with open(SURVEY_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if "テスト" in row[1] or "test" in row[1].lower(): continue
            name = row[1]
            if "石埜" in name: continue
            nickname = NAME_MAP.get(name)
            if not nickname: continue
            
            # SUS
            qs = []
            for i in range(10):
                val = parse_likert(row[3+i])
                if val:
                    if i%2==0: qs.append(val-1) # 0, 2...
                    else: qs.append(5-val)      # 1, 3...
            sus = sum(qs)*2.5 if len(qs)==10 else 0
            
            task_data = {}
            
            # Task 1
            t1 = row[13]
            if t1:
                t1_read = parse_likert(row[14])
                t1_req = parse_likert(row[15])
                t1_use = parse_likert(row[16])
                t1_sur = parse_likert(row[17])
                task_data[t1] = {"read": t1_read, "req": t1_req, "use": t1_use, "sur": t1_sur}
                
            # Task 2
            if len(row) > 20: 
                t2 = row[20]
                if t2:
                    t2_read = parse_likert(row[21])
                    t2_req = parse_likert(row[22])
                    t2_use = parse_likert(row[23])
                    t2_sur = parse_likert(row[24])
                    task_data[t2] = {"read": t2_read, "req": t2_req, "use": t2_use, "sur": t2_sur}
            
            user_survey_data[nickname] = {"sus": sus, "tasks": task_data}

    # 3. Combine and Format
    headers = [
        "ユーザ", "SUS", "タスク", "条件", 
        "時間", "Add", "Del", "Final", "Unique", "Rec率", 
        "読取", "要件", "有用", "意外"
    ]
    
    rows = []
    
    # Sort users by SUS score desc
    sorted_users = sorted(group_map.items(), key=lambda x: user_survey_data.get(NAME_MAP.get(x[0]), {}).get('sus', 0), reverse=True)
    
    for real_name, meta in sorted_users:
        nickname = NAME_MAP.get(real_name, real_name)
        survey = user_survey_data.get(nickname, {})
        sus_score = f"{survey.get('sus', 0):.1f}"
        
        # We have task1 and task2 in meta
        # Need to match with Dash logs and Survey task data
        
        for task_key in ["task1", "task2"]:
            tm = meta[task_key]
            dash_id = tm['dash']
            task_name = tm['task'] # UFO or Wine
            is_rec = tm['rec']
            cond_str = "Rec" if is_rec else "No Rec"
            
            # Log Metrics
            dm = dash_metrics.get(str(dash_id), {})
            dur = f"{dm.get('duration',0):.1f}"
            
            added = dm.get('added', 0)
            deleted = dm.get('deleted', 0)
            final = dm.get('final', 0)
            unique = dm.get('unique', 0)
            
            rr = f"{dm.get('rec_rate', 0):.0f}%" if is_rec else "-"
            
            # Survey Metrics
            sm = survey.get('tasks', {}).get(task_name, {})
            read = sm.get('read', '-')
            req = sm.get('req', '-')
            use = sm.get('use', '-') if is_rec else "-"
            sur = sm.get('sur', '-') if is_rec else "-"
            
            row = [
                nickname, sus_score, task_name, cond_str,
                dur, added, deleted, final, unique, rr,
                read, req, use, sur
            ]
            rows.append(row)
            
    print("## 3. ユーザ別詳細データ (User Breakdown)")
    print("\n※ SUS: System Usability Scale (ユーザビリティスコア)\n※ 時間: 分\n※ Add/Del/Final: 作成数/削除数/最終数\n※ Unique: ユニークView数\n※ 読取: 情報の読み取りやすさ (1-5)\n※ 要件: 要件達成度 (1-5)\n※ 有用: 推薦の有用性 (1-5, Recのみ)\n※ 意外: 推薦の意外性 (1-5, Recのみ)\n")
    print(format_table(headers, rows))

if __name__ == "__main__":
    analyze()
