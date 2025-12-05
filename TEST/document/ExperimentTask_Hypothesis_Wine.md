### ワインレビューデータ タスクシナリオ

### **📌 プロジェクト概要**

高級ワイン輸入商社より、新規取り扱いワインの選定支援を依頼されました。
バイヤーは「高価なワインが良いのは当たり前だが、手頃な価格で高品質な『隠れた名品』を見つけたい」と考えています。

あなたのミッションは、**インタラクティブなダッシュボードを作成し、コストパフォーマンスの高いワイン産地や品種を発掘すること**です。

### **📊 データセット詳細: wineReview.csv**
**概要:** 129971 行, 14 カラム

| カラム名 | データ型 | 統計量・詳細 |
| :--- | :--- | :--- |
| csv_id | int64 | 最小: 0<br>最大: 129970<br>平均: 64985.00 |
| country | object | ユニーク数: 43<br>上位3件:<br>US (54504件)<br>France (22093件)<br>Italy (19540件) |
| description | object | ユニーク数: 119955<br>上位3件:<br>Cigar box, café au lait, and dried tobacco aromas are followed by coffee and blackberry flavors on the palate with a moderate finish. (3件)<br>Seductively tart in lemon pith, cranberry and pomegranate, this refreshing, light-bodied quaff is infinitely drool-worthy. (3件)<br>This communicates a sense of aromatic purity, with scents of white flower, stone fruit and citrus. The mouthfeel is creamy and rich, but supported by good acidity. (3件) |
| designation | object | ユニーク数: 37979<br>上位3件:<br>Reserve (2009件)<br>Estate (1322件)<br>Riserva (698件) |
| points | int64 | 最小: 80<br>最大: 100<br>平均: 88.45 |
| price | float64 | 最小: 4.0<br>最大: 3300.0<br>平均: 35.36 |
| province | object | ユニーク数: 425<br>上位3件:<br>California (36247件)<br>Washington (8639件)<br>Bordeaux (5941件) |
| region_1 | object | ユニーク数: 1229<br>上位3件:<br>Napa Valley (4480件)<br>Columbia Valley (WA) (4124件)<br>Russian River Valley (3091件) |
| region_2 | object | ユニーク数: 17<br>上位3件:<br>Central Coast (11065件)<br>Sonoma (9028件)<br>Columbia Valley (8103件) |
| taster_name | object | ユニーク数: 19<br>上位3件:<br>Roger Voss (25514件)<br>Michael Schachner (15134件)<br>Kerin O’Keefe (10776件) |
| taster_twitter_handle | object | ユニーク数: 15<br>上位3件:<br>@vossroger (25514件)<br>@wineschach (15134件)<br>@kerinokeefe (10776件) |
| title | object | ユニーク数: 118840<br>上位3件:<br>Gloria Ferrer NV Sonoma Brut Sparkling (Sonoma County) (11件)<br>Korbel NV Brut Sparkling (California) (9件)<br>Segura Viudas NV Extra Dry Sparkling (Cava) (8件) |
| variety | object | ユニーク数: 707<br>上位3件:<br>Pinot Noir (13272件)<br>Chardonnay (11753件)<br>Cabernet Sauvignon (9472件) |
| winery | object | ユニーク数: 16757<br>上位3件:<br>Wines & Winemakers (222件)<br>Testarossa (218件)<br>DFJ Vinhos (215件) |

### **💼 バイヤーの仮説（検証対象）**

バイヤーは以下の仮説を持っていますが、**[ ]** の部分が不明確です。ダッシュボードを作成し、この空白をファクトで埋めてください。

> 「一般的に評価点（Points）が高いワインは価格（Price）も高いが、**[ A ] （国・地域名）** の **[ B ] （品種名）**に関しては、比較的低価格でありながら **[ C ] 点以上**の高評価を獲得しているものが多く、『コスパ最強』のカテゴリーと言えるのではないか。
> また、著名なレビュワーである **Kerin O’Keefe** 氏の評価傾向についても、何か特徴があるのではないかと気になっている」
> 

### **📃 納品物（作成するダッシュボードの要件）**

この仮説を検証し、バイヤーに報告するために、以下の分析視点を網羅したダッシュボードを作成してください。

**1. 価格と評価点の相関分析**

- **目的:** 価格と評価点の全体的な傾向を把握し、価格が安くても評価が高い領域（左上の領域など）にあるデータの分布を確認する。
- **ヒント:** 散布図（Scatter Plot）が最も適しています。

**2. 国・地域別のコスパ分析（[ A ], [ B ], [ C ] の特定）**

- **目的:** 国や地域（Province）、品種（Variety）ごとに、平均価格と平均評価点を比較し、コストパフォーマンスが良いエリアを特定する。

**3. 品種別の評価分布**

- **目的:** 特定の品種（Variety）が、高評価帯にどれくらい分布しているかを確認する。

**4. 追加分析：特定レビュワー「Kerin O’Keefe」の深掘り**

- **目的:** `taster_name` が "Kerin O’Keefe" のデータに着目し、新たなインサイトを発見する。
- **分析のヒント:**
    - 彼女のレビューはどの国に集中しているか？
    - **価格が $60 を超えるワイン** に限定したとき、彼女の評価点（Points）は他のレビュワーと比べてどう違うか？（厳しい？甘い？分散が大きい？）
    - 彼女が高評価をつけるワインにはどのような特徴があるか？

### **🚀 実行手順**

1. 分析アプリケーションにログインし、データソース `wine_review` に接続します。
2. **まず最初に「散布図」を作成**し、横軸に価格、縦軸に評価点をとり、全体の相関を確認します。（これは必須のステップです）
3. 続けて、上記の「3つの分析視点」に対応する他のグラフをそれぞれ作成し、ダッシュボードに配置します。
4. 完成したダッシュボードを分析し、仮説の **[ A ], [ B ], [ C ]** に当てはまる具体的なファクトを特定してください。
5. バイヤーへの提案として、Kerin O’Keefe 氏の評価傾向を踏まえた「間違いない一本」と、コスパ重視の「隠れた名品」をそれぞれピックアップしてください。
