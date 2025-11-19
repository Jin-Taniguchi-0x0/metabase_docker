#!/bin/bash
set -e

# $POSTGRES_DB (.env で指定した 'data_db') に接続して実行
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

-- === 1. social_media_ads テーブル (主キーを修正) ===

-- 既存のテーブル定義を削除
DROP TABLE IF EXISTS social_media_ads;

-- 実際のCSV (Campaign_ID, Target_Audience...) に基づく新しい定義
CREATE TABLE social_media_ads (
    id SERIAL PRIMARY KEY,                 -- ★ 修正: 新しく連番の主キーを追加
    Campaign_ID INTEGER,                   -- ★ 修正: PRIMARY KEY制約を削除
    Target_Audience TEXT,
    Campaign_Goal TEXT,
    Duration TEXT,
    Channel_Used TEXT,
    Conversion_Rate FLOAT,
    Acquisition_Cost TEXT, -- '$' が含まれるため TEXT としてロード
    ROI FLOAT,
    Location TEXT,
    Language TEXT,
    Clicks INTEGER,
    Impressions INTEGER,
    Engagement_Score INTEGER,
    Customer_Segment TEXT,
    Date DATE,
    Company TEXT
);

-- CSVデータをインポート (CSVのヘッダー列を明記)
-- (id列はSERIALなので、COPYのリストから除外する)
\copy social_media_ads(Campaign_ID, Target_Audience, Campaign_Goal, Duration, Channel_Used, Conversion_Rate, Acquisition_Cost, ROI, Location, Language, Clicks, Impressions, Engagement_Score, Customer_Segment, Date, Company) FROM '/docker-entrypoint-initdb.d/social_media_ads.csv' DELIMITER ',' CSV HEADER;


-- === 2. wine_review テーブル (CSVからご希望の列のみを抽出する方式) ===

-- 既存のテーブル定義を削除
DROP TABLE IF EXISTS wine_review;

-- ステップ1: CSVの全データをロードするための一時テーブルを作成
CREATE TEMPORARY TABLE wine_review_temp (
    csv_id INTEGER, -- CSVの先頭の無名列
    country TEXT,
    description TEXT,
    designation TEXT,
    points INTEGER,
    price FLOAT,
    province TEXT,
    region_1 TEXT,
    region_2 TEXT,
    taster_name TEXT,
    taster_twitter_handle TEXT,
    title TEXT,
    variety TEXT,
    winery TEXT
);

-- ステップ2: 一時テーブルにCSVデータをロード
\copy wine_review_temp(csv_id, country, description, designation, points, price, province, region_1, region_2, taster_name, taster_twitter_handle, title, variety, winery) FROM '/docker-entrypoint-initdb.d/wineReview.csv' DELIMITER ',' CSV HEADER;

-- ステップ3: ご指定のスキーマで最終的なテーブルを作成
CREATE TABLE wine_review (
    id INTEGER PRIMARY KEY, -- 'csv_id' を 'id' とし、主キーに設定
    country TEXT,
    description TEXT,
    designation TEXT,
    points INTEGER,
    price FLOAT,
    province TEXT,
    region_1 TEXT,
    region_2 TEXT,
    variety TEXT,
    winery TEXT
);

-- ステップ4: 一時テーブルから必要な列だけを選択して最終テーブルに挿入
INSERT INTO wine_review (
    id,
    country,
    description,
    designation,
    points,
    price,
    province,
    region_1,
    region_2,
    variety,
    winery
)
SELECT
    csv_id, -- 'csv_id' 列を 'id' 列にマッピング
    country,
    description,
    designation,
    points,
    price,
    province,
    region_1,
    region_2,
    variety,
    winery
FROM
    wine_review_temp;

-- ステップ5: 一時テーブルは自動的に削除されます


-- === 3. athlete_events テーブル ===

-- 既存のテーブル定義を削除
DROP TABLE IF EXISTS athlete_events;

CREATE TABLE athlete_events (
    id SERIAL PRIMARY KEY,
    ID INTEGER,
    Name TEXT,
    Sex TEXT,
    Age TEXT, -- 'NA'などが含まれる可能性があるためTEXT
    Height TEXT, -- 'NA'などが含まれる可能性があるためTEXT
    Weight TEXT, -- 'NA'などが含まれる可能性があるためTEXT
    Team TEXT,
    NOC TEXT,
    Games TEXT,
    Year INTEGER,
    Season TEXT,
    City TEXT,
    Sport TEXT,
    Event TEXT,
    Medal TEXT
);

\copy athlete_events(ID, Name, Sex, Age, Height, Weight, Team, NOC, Games, Year, Season, City, Sport, Event, Medal) FROM '/docker-entrypoint-initdb.d/athlete_events.csv' DELIMITER ',' CSV HEADER;


-- === 4. UFOscrubbed テーブル ===

-- 既存のテーブル定義を削除
DROP TABLE IF EXISTS ufo_scrubbed;

CREATE TABLE ufo_scrubbed (
    id SERIAL PRIMARY KEY,
    datetime TEXT, -- 日付形式が特殊な場合があるためTEXT
    city TEXT,
    state TEXT,
    country TEXT,
    shape TEXT,
    duration_seconds TEXT, -- 数値変換エラーを防ぐためTEXT
    duration_hours_min TEXT,
    comments TEXT,
    date_posted TEXT,
    latitude TEXT, -- 数値変換エラーを防ぐためTEXT
    longitude TEXT -- 数値変換エラーを防ぐためTEXT
);

\copy ufo_scrubbed(datetime, city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted, latitude, longitude) FROM '/docker-entrypoint-initdb.d/UFOscrubbed.csv' DELIMITER ',' CSV HEADER;

EOSQL