import csv
import re

CSV_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - フォームの回答.csv"
GROUP_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - グループ.csv"

NAME_MAP = {
    "芹澤尚舜": "せり", "峪紳大朗": "さこ", "佐竹宏紀": "ひろくん", "今村真沙斗": "まさと",
    "宮澤匠": "たくみ", "鈴木俊詞": "しゅんじ", "永沼翔翼": "つばさ", "田中 翔太郎": "たなか",
    "岡本悠吾": "ゆうご", "矢野温加": "やん", "塙裕貴": "はなわ", "板井孝樹": "いたい"
}

def get_rec_condition(nickname, task, group_mapping):
    for meta in group_mapping.values():
        if meta['user'] == nickname and meta['task'] == task:
            return meta['rec']
    return None

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
            if not row[0]: continue
            dash1 = row[1]; task1 = row[2]; rec1 = row[3]
            dash2 = row[4]; task2 = row[5]; rec2 = row[6]
            if dash1: mapping[str(dash1)] = {"user": row[0], "task": task1, "rec": rec1 == "あり"}
            if dash2: mapping[str(dash2)] = {"user": row[0], "task": task2, "rec": rec2 == "あり"}
    return mapping

def parse_val(text):
    match = re.search(r'\((\d)\)', text)
    return int(match.group(1)) if match else None

def main():
    mapping = load_group_mapping()
    
    print("name,task,condition,readability,requirements,helpfulness,surprise")
    
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) # header
        
        for row in reader:
            if "テスト" in row[1] or "test" in row[1].lower(): continue
            if "石埜" in row[1]: continue
            
            name = row[1]
            nickname = NAME_MAP.get(name)
            if not nickname: continue

            # Task 1 (Cols 13-17)
            t1 = row[13]
            is_rec = get_rec_condition(nickname, t1, mapping)
            if is_rec is not None:
                val_read = parse_val(row[14])
                val_req = parse_val(row[15])
                val_help = parse_val(row[16]) if is_rec else "-"
                val_surp = parse_val(row[17]) if is_rec else "-"
                print(f"{nickname},{t1},{'Rec' if is_rec else 'NoRec'},{val_read},{val_req},{val_help},{val_surp}")

            # Task 2 (Cols 20-24)
            if len(row) > 20:
                t2 = row[20]
                is_rec = get_rec_condition(nickname, t2, mapping)
                if is_rec is not None:
                    val_read = parse_val(row[21])
                    val_req = parse_val(row[22])
                    val_help = parse_val(row[23]) if is_rec else "-"
                    val_surp = parse_val(row[24]) if is_rec else "-"
                    print(f"{nickname},{t2},{'Rec' if is_rec else 'NoRec'},{val_read},{val_req},{val_help},{val_surp}")

if __name__ == "__main__":
    main()
