#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

-- CSVからデータを格納するテーブル作成
CREATE TABLE social_media_ads (
    id SERIAL PRIMARY KEY,
    gender VARCHAR(10),
    age INTEGER,
    estimated_salary INTEGER,
    purchased INTEGER
);

-- CSVデータをインポート
\copy social_media_ads(gender, age, estimated_salary, purchased) FROM '/docker-entrypoint-initdb.d/social_media_ads.csv' DELIMITER ',' CSV HEADER;

EOSQL
