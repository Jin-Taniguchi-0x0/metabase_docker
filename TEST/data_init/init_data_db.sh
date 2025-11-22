#!/bin/bash
set -e

# === 1. social_media_ads テーブル ===
echo "Initializing social_media_ads..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS social_media_ads;
    CREATE TABLE social_media_ads (
        id SERIAL PRIMARY KEY,
        Campaign_ID INTEGER,
        Target_Audience TEXT,
        Campaign_Goal TEXT,
        Duration TEXT,
        Channel_Used TEXT,
        Conversion_Rate FLOAT,
        Acquisition_Cost TEXT,
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
    \copy social_media_ads(Campaign_ID, Target_Audience, Campaign_Goal, Duration, Channel_Used, Conversion_Rate, Acquisition_Cost, ROI, Location, Language, Clicks, Impressions, Engagement_Score, Customer_Segment, Date, Company) FROM '/docker-entrypoint-initdb.d/social_media_ads.csv' DELIMITER ',' CSV HEADER;
EOSQL

# === 2. wine_review テーブル ===
echo "Initializing wine_review..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS wine_review;
    CREATE TEMPORARY TABLE wine_review_temp (
        csv_id INTEGER,
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
    \copy wine_review_temp(csv_id, country, description, designation, points, price, province, region_1, region_2, taster_name, taster_twitter_handle, title, variety, winery) FROM '/docker-entrypoint-initdb.d/wineReview.csv' DELIMITER ',' CSV HEADER;
    
    CREATE TABLE wine_review (
        id INTEGER PRIMARY KEY,
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
    
    INSERT INTO wine_review (id, country, description, designation, points, price, province, region_1, region_2, variety, winery)
    SELECT csv_id, country, description, designation, points, price, province, region_1, region_2, variety, winery FROM wine_review_temp;
EOSQL

# === 3. athlete_events テーブル ===
echo "Initializing athlete_events..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS athlete_events;
    -- Use TEXT for all columns in temp table to avoid COPY errors with "NA" or bad data
    CREATE TEMPORARY TABLE athlete_events_temp (
        ID TEXT,
        Name TEXT,
        Sex TEXT,
        Age TEXT,
        Height TEXT,
        Weight TEXT,
        Team TEXT,
        NOC TEXT,
        Games TEXT,
        Year TEXT,
        Season TEXT,
        City TEXT,
        Sport TEXT,
        Event TEXT,
        Medal TEXT
    );
    \copy athlete_events_temp(ID, Name, Sex, Age, Height, Weight, Team, NOC, Games, Year, Season, City, Sport, Event, Medal) FROM '/docker-entrypoint-initdb.d/athlete_events.csv' DELIMITER ',' CSV HEADER;
    
    CREATE TABLE athlete_events (
        id SERIAL PRIMARY KEY,
        athlete_id INTEGER,
        Name TEXT,
        Sex TEXT,
        Age TEXT,
        Height TEXT,
        Weight TEXT,
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
    
    INSERT INTO athlete_events (athlete_id, Name, Sex, Age, Height, Weight, Team, NOC, Games, Year, Season, City, Sport, Event, Medal)
    SELECT 
        NULLIF(ID, 'NA')::INTEGER, 
        Name, Sex, Age, Height, Weight, Team, NOC, Games, 
        NULLIF(Year, 'NA')::INTEGER, 
        Season, City, Sport, Event, Medal 
    FROM athlete_events_temp;
EOSQL

# === 4. UFOscrubbed テーブル ===
echo "Initializing ufo_scrubbed..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DROP TABLE IF EXISTS ufo_scrubbed;
    CREATE TEMPORARY TABLE ufo_scrubbed_temp (
        datetime TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        shape TEXT,
        duration_seconds TEXT,
        duration_hours_min TEXT,
        comments TEXT,
        date_posted TEXT,
        latitude TEXT,
        longitude TEXT
    );
    \copy ufo_scrubbed_temp(datetime, city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted, latitude, longitude) FROM '/docker-entrypoint-initdb.d/UFOscrubbed.csv' DELIMITER ',' CSV HEADER;
    
    CREATE TABLE ufo_scrubbed (
        id SERIAL PRIMARY KEY,
        datetime TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        shape TEXT,
        duration_seconds TEXT,
        duration_hours_min TEXT,
        comments TEXT,
        date_posted TEXT,
        latitude FLOAT,
        longitude FLOAT
    );
    
    INSERT INTO ufo_scrubbed (datetime, city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted, latitude, longitude)
    SELECT 
        datetime, city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted, 
        NULLIF(regexp_replace(latitude, '[^0-9.-]', '', 'g'), '')::FLOAT, 
        NULLIF(regexp_replace(longitude, '[^0-9.-]', '', 'g'), '')::FLOAT 
    FROM ufo_scrubbed_temp;
EOSQL