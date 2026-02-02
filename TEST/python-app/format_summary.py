import unicodedata

def char_width(char):
    w = unicodedata.east_asian_width(char)
    if w in ('W', 'F'):
        return 2
    return 1

def str_width(text):
    width = 0
    for char in text:
        width += char_width(char)
    return width

def pad_to_width(text, width):
    current_width = str_width(text)
    padding = width - current_width
    return text + ' ' * padding

def format_table(headers, rows):
    # Calculate max width for each column
    col_widths = [0] * len(headers)
    
    # Check headers
    for i, h in enumerate(headers):
        col_widths[i] = max(col_widths[i], str_width(h))
    
    # Check rows
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], str_width(str(cell)))
            
    # Add buffer
    # col_widths = [w + 2 for w in col_widths] 

    # Build Separator
    separator = "|"
    for w in col_widths:
        separator += " " + "-" * w + " |"
        
    # Build Header
    header_line = "|"
    for i, h in enumerate(headers):
        header_line += " " + pad_to_width(h, col_widths[i]) + " |"
        
    # Build Rows
    row_lines = []
    for row in rows:
        line = "|"
        for i, cell in enumerate(row):
            line += " " + pad_to_width(str(cell), col_widths[i]) + " |"
        row_lines.append(line)
        
    return header_line + "\n" + separator + "\n" + "\n".join(row_lines)

# Data 1: Quantitative
header1 = ["タスク", "条件", "人数 ($n$)", "平均所要時間 (分)", "平均作成ビュー数", "平均ユニークView数", "推薦利用率 (%)"]
rows1 = [
    ["UFO", "No Rec", "6", "20.78 (SD: 4.49)", "6.8", "6.83 (SD: 0.98)", "-"],
    ["UFO", "Rec", "6", "22.85 (SD: 4.71)", "6.2", "6.17 (SD: 1.17)", "68.6%"],
    ["Wine", "No Rec", "6", "26.49 (SD: 4.90)", "7.2", "7.17 (SD: 0.75)", "-"],
    ["Wine", "Rec", "6", "24.51 (SD: 4.27)", "6.8", "6.83 (SD: 0.75)", "46.2%"]
]

# Data 2: SUS
header2 = ["質問項目", "平均スコア", "SD"]
rows2 = [
    ["Q1. このシステムを頻繁に使いたいと思うか", "4.25", "0.75"],
    ["Q2. このシステムは不必要に複雑だと感じたか", "2.00", "1.21"],
    ["Q3. このシステムは使いやすいと思ったか", "3.75", "0.97"],
    ["Q4. このシステムを使うには技術者のサポートが必要だと思うか", "3.50", "1.24"],
    ["Q5. このシステムの様々な機能はうまく統合されていると感じたか", "4.33", "0.65"],
    ["Q6. このシステムには矛盾が多すぎると感じたか", "1.25", "0.45"],
    ["Q7. ほとんどの人がこのシステムをすぐに使いこなせるようになると思うか", "2.83", "1.19"],
    ["Q8. このシステムは非常に扱いにくいと感じたか", "1.75", "0.75"],
    ["Q9. このシステムを使うことに自信を感じたか", "3.75", "0.75"],
    ["Q10. このシステムを使い始める前に、多くのことを学ぶ必要があると感じたか", "2.67", "1.15"]
]

# Data 3: Task Specific
header3 = ["質問項目", "Rec (推薦あり)", "No Rec (推薦なし)"]
rows3 = [
    ["作成したDashboardは、クライアントにとって情報を読み取りやすいものになったと思うか", "**4.08**", "3.67"],
    ["作成したDashboardは、タスクの要件を達成していると思うか", "4.33", "**4.42**"]
]

# Data 4: Rec Only
header4 = ["質問項目", "平均スコア"]
rows4 = [
    ["推薦システムから得られた推薦は、Dashboard作成の役に立ったと思うか", "3.92"],
    ["推薦システムから得られた推薦は、意外性のあるものだったと思うか", "3.58"]
]

def generate_markdown():
    md = "# 実験結果まとめ\n\n## 1. 定量分析結果 (ログデータ)\n\n実験タスク (UFO, Wine) および推薦の有無 (Rec, No Rec) による比較結果。\n\n"
    md += format_table(header1, rows1)
    
    md += "\n\n### 考察要素\n- **UFOタスク**: 推薦あり (Rec) の方が平均所要時間が約2分長く、作成ビュー数はわずかに減少した。推薦利用率は 68.6% と高い。\n- **Wineタスク**: 推薦あり (Rec) の方が平均所要時間が約2分短縮された。推薦利用率は 46.2% であった。\n\n"
    
    md += "## 2. 定性分析結果 (アンケート)\n\n### System Usability Scale (SUS)\n- **平均合計スコア**: 69.4 (SD: 14.1, $n=12$)\n  - 一般的に SUS スコア 68 が平均とされるため、本システムは平均以上のユーザビリティを有していると言える。\n\n#### 質問別平均スコア (1-5段階)\n"
    md += format_table(header2, rows2)
    
    md += "\n\n### タスク別評価 (1-5段階)\n\n各参加者は2つのタスク (UFO, Wine) を行い、片方で推薦あり (Rec)、もう片方で推薦なし (No Rec) の条件を割り当てられました。以下のスコアは条件ごとの平均値です ($n=12$)。\n\n"
    md += format_table(header3, rows3)
    
    md += "\n\n#### 推薦機能への評価 (Rec条件のみ)\n※推薦あり条件 (Rec) でタスクを行った際のみ回答 ($n=12$)。\n\n"
    md += format_table(header4, rows4)
    
    return md

if __name__ == "__main__":
    print(generate_markdown())
