import csv

CSV_FILE = "/Users/jin/metabase/TEST/document/アンケート/アンケート（回答） - フォームの回答.csv"

# Users and their conditions
# User: {Dataset: Condition}
USER_CONDITIONS = {
    "せり": {"UFO": "No Rec", "Wine": "Rec"},
    "さこ": {"Wine": "No Rec", "UFO": "Rec"},
    "ひろくん": {"UFO": "Rec", "Wine": "No Rec"},
    "まさと": {"Wine": "Rec", "UFO": "No Rec"},
    "たくみ": {"UFO": "No Rec", "Wine": "Rec"},
    "しゅんじ": {"Wine": "No Rec", "UFO": "Rec"},
    "つばさ": {"UFO": "Rec", "Wine": "No Rec"},
    "たなか": {"Wine": "Rec", "UFO": "No Rec"},
    "やん": {"UFO": "No Rec", "Wine": "Rec"},
    "ゆうご": {"Wine": "No Rec", "UFO": "Rec"},
    "いたい": {"UFO": "Rec", "Wine": "No Rec"},
    "はなわ": {"Wine": "Rec", "UFO": "No Rec"},
}

POSITIVE_GROUP = ["やん", "つばさ", "しゅんじ", "せり", "たくみ", "さこ"]
NEGATIVE_GROUP = ["まさと", "たなか", "いたい", "はなわ", "ひろくん"]

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

def main():
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        # Print headers with indices for debugging
        # for i, h in enumerate(header):
        #     print(f"{i}: {h}")
            
        # Identified Indices (approximate, need verification):
        # 1: Name
        # Block 1
        # 13: Data 1
        print(f"{'User':<10} | {'Group':<8} | {'Task':<5} | {'Cond':<6} | {'Free Text Answers (Focus, Insight, Rec Thoughts)'}")
        print("-" * 150)

        for row in reader:
            real_name = row[1]
            if "テスト" in real_name or "test" in real_name.lower() or "石埜" in real_name:
                continue
            
            nickname = NAME_MAP.get(real_name)
            if not nickname:
                # print(f"Skipping {real_name}")
                continue
                
            if nickname not in USER_CONDITIONS:
                continue

            group = "Pos" if nickname in POSITIVE_GROUP else "Neg" if nickname in NEGATIVE_GROUP else "Neu"
            
            # Block 1
            data1 = row[13]
            cond1 = USER_CONDITIONS[nickname].get(data1, "Unknown")
            focus1 = row[18]
            insight1 = row[19]
            rec_thoughts1 = row[31]

            # Block 2
            data2 = row[20]
            cond2 = USER_CONDITIONS[nickname].get(data2, "Unknown")
            focus2 = row[25]
            insight2 = row[26]
            rec_thoughts2 = row[36] if len(row) > 36 else ""

            # Analysis for Task Complexity (Check all tasks for keywords)
            keywords = ["難", "複雑", "迷", "時間", "多", "わから", "difficult", "complex", "hard"]
            
            # Helper to check keywords
            def has_keyword(text):
                return any(k in text for k in keywords)

            # Block 1
            print(f"[{group.upper()}] {nickname} ({real_name}) - Task: {data1} ({cond1})")
            if has_keyword(focus1 + insight1 + rec_thoughts1):
                 print(f"  > Focus: {focus1}")
                 print(f"  > Insight: {insight1}")
                 print(f"  > Rec Thoughts: {rec_thoughts1}")
            print("-" * 40)

            # Block 2
            print(f"[{group.upper()}] {nickname} ({real_name}) - Task: {data2} ({cond2})")
            if has_keyword(focus2 + insight2 + rec_thoughts2):
                 print(f"  > Focus: {focus2}")
                 print(f"  > Insight: {insight2}")
                 print(f"  > Rec Thoughts: {rec_thoughts2}")
            print("=" * 80)

if __name__ == "__main__":
    main()
