#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

-- CSVからデータを格納するテーブル作成
CREATE TABLE social_media_ads (
    id INTEGER PRIMARY KEY,
    gender TEXT,
    age_group TEXT,
    campaign_objective TEXT,
    duration TEXT,
    platform TEXT,
    engagement_rate FLOAT,
    budget TEXT,
    cost_per_result FLOAT,
    location TEXT,
    language TEXT,
    impressions INTEGER,
    reach INTEGER,
    frequency FLOAT
);


-- CSVデータをインポート
\copy social_media_ads FROM '/docker-entrypoint-initdb.d/social_media_ads.csv' DELIMITER ',' CSV HEADER;


EOSQL
