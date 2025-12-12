### ワインレビューデータ タスクシナリオ

### **📌 プロジェクト概要**

高級ワイン輸入商社より、新規取り扱いワインの選定支援を依頼されました。
バイヤーは「高価なワインが良いのは当たり前だが、手頃な価格で高品質な『隠れた名品』を見つけたい」と考えています。

あなたのミッションは、**インタラクティブなダッシュボードを作成し、コストパフォーマンスの高いワイン産地や品種を発掘すること**です。

### **📊 データセット詳細: wineReview.csv**

**概要:** 129971 行, 14 カラム

| カラム名              | データ型 | 統計量・詳細                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| :-------------------- | :------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| csv_id                | int64    | 最小: 0<br>最大: 129970<br>平均: 64985.00                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| country               | object   | ユニーク数: 43<br>上位 3 件:<br>US (54504 件)<br>France (22093 件)<br>Italy (19540 件)                                                                                                                                                                                                                                                                                                                                                                                                              |
| description           | object   | ユニーク数: 119955<br>上位 3 件:<br>Cigar box, café au lait, and dried tobacco aromas are followed by coffee and blackberry flavors on the palate with a moderate finish. (3 件)<br>Seductively tart in lemon pith, cranberry and pomegranate, this refreshing, light-bodied quaff is infinitely drool-worthy. (3 件)<br>This communicates a sense of aromatic purity, with scents of white flower, stone fruit and citrus. The mouthfeel is creamy and rich, but supported by good acidity. (3 件) |
| designation           | object   | ユニーク数: 37979<br>上位 3 件:<br>Reserve (2009 件)<br>Estate (1322 件)<br>Riserva (698 件)                                                                                                                                                                                                                                                                                                                                                                                                        |
| points                | int64    | 最小: 80<br>最大: 100<br>平均: 88.45                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| price                 | float64  | 最小: 4.0<br>最大: 3300.0<br>平均: 35.36                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| province              | object   | ユニーク数: 425<br>上位 3 件:<br>California (36247 件)<br>Washington (8639 件)<br>Bordeaux (5941 件)                                                                                                                                                                                                                                                                                                                                                                                                |
| region_1              | object   | ユニーク数: 1229<br>上位 3 件:<br>Napa Valley (4480 件)<br>Columbia Valley (WA) (4124 件)<br>Russian River Valley (3091 件)                                                                                                                                                                                                                                                                                                                                                                         |
| region_2              | object   | ユニーク数: 17<br>上位 3 件:<br>Central Coast (11065 件)<br>Sonoma (9028 件)<br>Columbia Valley (8103 件)                                                                                                                                                                                                                                                                                                                                                                                           |
| taster_name           | object   | ユニーク数: 19<br>上位 3 件:<br>Roger Voss (25514 件)<br>Michael Schachner (15134 件)<br>Kerin O’Keefe (10776 件)                                                                                                                                                                                                                                                                                                                                                                                   |
| taster_twitter_handle | object   | ユニーク数: 15<br>上位 3 件:<br>@vossroger (25514 件)<br>@wineschach (15134 件)<br>@kerinokeefe (10776 件)                                                                                                                                                                                                                                                                                                                                                                                          |
| title                 | object   | ユニーク数: 118840<br>上位 3 件:<br>Gloria Ferrer NV Sonoma Brut Sparkling (Sonoma County) (11 件)<br>Korbel NV Brut Sparkling (California) (9 件)<br>Segura Viudas NV Extra Dry Sparkling (Cava) (8 件)                                                                                                                                                                                                                                                                                            |
| variety               | object   | ユニーク数: 707<br>上位 3 件:<br>Pinot Noir (13272 件)<br>Chardonnay (11753 件)<br>Cabernet Sauvignon (9472 件)                                                                                                                                                                                                                                                                                                                                                                                     |
| winery                | object   | ユニーク数: 16757<br>上位 3 件:<br>Wines & Winemakers (222 件)<br>Testarossa (218 件)<br>DFJ Vinhos (215 件)                                                                                                                                                                                                                                                                                                                                                                                        |

### **💼 バイヤーの仮説（検証対象）**

バイヤーは以下の仮説を持っていますが、[ ] の部分が不明確です。ダッシュボードを作成し、この空⽩をファクトで
埋めてください。

> 『弊社の評価基準において、Points が 80~85 点であれば『一般』、85~90 点であれば『高品質』、90 点以上であれば『最高品質』 と定義しています。
> 一般的に評価点（Points）が高いワインは価格（Price）も高い傾向にありますが、今回の分析では、この相関から外れた『コスパ最強』の生産国が存在するという仮説を立てています。
> 具体的には [ A ] （国名） です。この国は Points per price が非常に高く、得点の平均が『高品質』のカテゴリに属しています。さらに [ A ] （国名） の平均価格は世界平均と比べて $ [ B ] も安く、極めて高コスパなワイン産地と見なすことができるでしょう。
> つきましては、[ A ] （国名） のワインを新規に仕入れるため、ターゲットとなる最大手ワイナリー [ C ] を特定してください。 また、追加分析として [ A ] （国名） のワインの品種にはどのような特徴があるのかについても、調査結果を報告してください。』

### **📃 納品物（作成するダッシュボードの要件）**

この仮説を検証し、バイヤーに報告するために、以下の分析視点を網羅したダッシュボードを作成してください。

**I. 国別の平均 Points / Price 比較**

- **目的:** 国ごとの「Points / Price（価格あたりの評価点）」の平均値を比較し、コストパフォーマンスが高い国を把握します。
- **ヒント:** 国（Country）を軸にし、**Points / Price** の平均値を棒グラフで表示してください。（`Points / Price` カラムはテーブルに既存です）

**II. コスパ最強国 [ A ] と 価格差 [ B ] の特定**

- **目的:** 平均評価点が 85 点〜90 点（高品質）の範囲にあり、かつ「Points per price（価格あたりの評価点）」が高い国を探し、国 [ A ] を特定します。
- **ヒント:**
  - 国 [ A ] の平均価格と、全体の平均価格を比較し、その差額 [ B ] を算出する必要があります。

**III. 最大手ワイナリー [ C ] の特定**

- **目的:** 特定した国 [ A ] の中で、最も多くのワインを生産・レビューされている「最大手ワイナリー」 [ C ] を特定します。

**IV. 追加分析：国 [ A ] の品種特徴分析**

- **目的:** 国 [ A ] で生産されるワインの品種（Variety）にはどのような特徴があるか（種類、評価、価格帯など）を分析してください。
- **分析のヒント:**
  - 品種ごとのレコード数（生産量）、平均評価点、平均価格などを可視化すると特徴が見えやすくなります。

### **🚀 実行手順**

1. 分析アプリケーションにログインし、データソース `wine_review` に接続します。
2. **Step 1:** 国ごとの平均 **Points / Price** を比較する棒グラフを作成し、どの国のコスパが高いかを確認します。
3. **Step 2:** 分析を行い、**平均評価点が 85 点〜90 点（高品質）**であり、「コスパ最強」な国 **[ A ]** を特定します。
4. **Step 3:** 特出した国 [ A ] の平均価格と世界平均価格を比較し、その差 **[ B ]** を確認します。
5. **Step 4:** 特出した国 [ A ] の中で、最大手ワイナリー **[ C ]** を特定します。
6. **Step 5:** 国 [ A ] の品種（Variety）ごとの傾向（件数、評価、価格）を分析するチャートを追加し、インサイトをまとめてください。
