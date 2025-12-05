import pandas as pd
import os

DATA_DIR = '/Users/jin/metabase/TEST/data_init'

def analyze_olympics():
    print("\n=== Olympics Analysis ===")
    df = pd.read_csv(os.path.join(DATA_DIR, 'athlete_events.csv'))
    
    # Golden Age
    df = df.dropna(subset=['Medal'])
    df['Decade'] = (df['Year'] // 10) * 10
    
    top_country_sport = df.groupby(['NOC', 'Sport', 'Decade']).size().reset_index(name='MedalCount')
    top_country_sport = top_country_sport.sort_values('MedalCount', ascending=False).head(5)
    print("Top 5 Golden Ages (NOC, Sport, Decade):")
    print(top_country_sport)
    
    # Physical Trends
    print("\nPhysical Trends (Example: Basketball):")
    basketball = df[df['Sport'] == 'Basketball']
    print(basketball.groupby('Decade')[['Height', 'Weight']].mean())

    # Home Advantage (Simple check for USA)
    print("\nHome Advantage Check (USA):")
    usa_medals = df[df['NOC'] == 'USA']
    print(usa_medals.groupby('City')['Medal'].count().sort_values(ascending=False).head(5))

def analyze_wine():
    print("\n=== Wine Analysis ===")
    df = pd.read_csv(os.path.join(DATA_DIR, 'wineReview.csv'))
    
    # Kerin O'Keefe
    kerin = df[df['taster_name'] == 'Kerin O’Keefe']
    print(f"Kerin O'Keefe Total Reviews: {len(kerin)}")
    print(f"Top Countries reviewed by Kerin:\n{kerin['country'].value_counts().head(3)}")
    
    kerin_high_end = kerin[(kerin['price'] > 60) & (kerin['country'] == 'Italy')]
    print(f"\nKerin O'Keefe High-End (>60) Italy Stats:\n{kerin_high_end['points'].describe()}")
    
    other_tasters = df[(df['taster_name'] != 'Kerin O’Keefe') & (df['price'] > 60) & (df['country'] == 'Italy')]
    print(f"\nOther Tasters High-End (>60) Italy Stats:\n{other_tasters['points'].describe()}")

    # Best Value (High Points, Low Price)
    # Filter for significant sample size
    value_wines = df[(df['points'] >= 90) & (df['price'] <= 20)]
    print("\nBest Value Regions (Points >= 90, Price <= 20):")
    print(value_wines['province'].value_counts().head(5))

def analyze_social_media():
    print("\n=== Social Media Analysis ===")
    df = pd.read_csv(os.path.join(DATA_DIR, 'social_media_ads.csv'))
    
    # ROI by Target/Channel
    print("Avg ROI by Target Audience & Channel:")
    print(df.groupby(['Target_Audience', 'Channel_Used'])['ROI'].mean().sort_values(ascending=False).head(5))
    
    # Acquisition Cost
    # Cost is string like "$50", need to clean
    df['Acquisition_Cost_Num'] = df['Acquisition_Cost'].replace('[\$,]', '', regex=True).astype(float)
    print("\nAvg Acquisition Cost by Channel:")
    print(df.groupby('Channel_Used')['Acquisition_Cost_Num'].mean().sort_values())

def analyze_ufo():
    print("\n=== UFO Analysis ===")
    df = pd.read_csv(os.path.join(DATA_DIR, 'UFOscrubbed.csv'), low_memory=False)
    
    # Clean datetime
    df['datetime'] = pd.to_datetime(df['datetime'].str.replace('24:00', '00:00'), errors='coerce')
    df['Year'] = df['datetime'].dt.year
    df['Decade'] = (df['Year'] // 10) * 10
    
    print("Sightings by Decade:")
    print(df['Decade'].value_counts().sort_index())
    
    print("\nTop Countries:")
    print(df['country'].value_counts().head(3))
    
    print("\nTop Shapes:")
    print(df['shape'].value_counts().head(3))
    
    # Duration
    df['duration (seconds)'] = pd.to_numeric(df['duration (seconds)'], errors='coerce')
    print("\nDuration Stats (seconds):")
    print(df['duration (seconds)'].describe())
    print("\nLong Duration (> 1 hour) by Shape:")
    long_duration = df[df['duration (seconds)'] > 3600]
    print(long_duration['shape'].value_counts().head(5))

if __name__ == "__main__":
    analyze_olympics()
    analyze_wine()
    analyze_social_media()
    analyze_ufo()
