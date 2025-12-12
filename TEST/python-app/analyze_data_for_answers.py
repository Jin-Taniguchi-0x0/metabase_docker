import pandas as pd
import warnings

warnings.filterwarnings('ignore')

def analyze_ufo():
    print("--- UFO Analysis ---")
    try:
        df = pd.read_csv('/Users/jin/metabase/TEST/data_init/UFOscrubbed.csv', low_memory=False)
    except Exception as e:
        print(f"Error loading UFO data: {e}")
        return

    # Clean datetime
    df['datetime'] = df['datetime'].astype(str).str.replace('24:00', '00:00')
    try:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    except Exception:
        pass
    
    df['year'] = df['datetime'].dt.year
    df = df.dropna(subset=['year'])
    df['year'] = df['year'].astype(int)

    # 1. Decade of surge [A]
    df['decade'] = (df['year'] // 10) * 10
    decade_counts = df['decade'].value_counts().sort_index()
    print("\nDecade Counts:")
    print(decade_counts)
    
    # 2. Country with frequent reports [B]
    country_counts = df['country'].value_counts()
    print("\nCountry Counts:")
    print(country_counts.head())
    top_country = country_counts.idxmax()
    print(f"Top Country [B]: {top_country}")

    # 3. Shape in [B] [C]
    df_b = df[df['country'] == top_country]
    shape_counts = df_b['shape'].value_counts()
    print(f"\nShape Counts in {top_country}:")
    print(shape_counts.head())
    top_shape = shape_counts.idxmax()
    print(f"Top Shape [C]: {top_shape}")

    # 4. Duration Analysis in [B] for [C] vs Others
    # Clean duration
    def clean_duration(x):
        try:
            return float(x)
        except:
            return None
    
    df_b['duration (seconds)'] = df_b['duration (seconds)'].apply(clean_duration)
    df_b = df_b.dropna(subset=['duration (seconds)'])

    df_c = df_b[df_b['shape'] == top_shape]
    df_others = df_b[df_b['shape'] != top_shape]

    print(f"\nDuration Analysis in {top_country}:")
    print(f"Shape {top_shape} [C] - Mean: {df_c['duration (seconds)'].mean():.2f}, Median: {df_c['duration (seconds)'].median()}")
    print(f"Other Shapes - Mean: {df_others['duration (seconds)'].mean():.2f}, Median: {df_others['duration (seconds)'].median()}")

    print("\nAdditional Analysis Ideas (UFO):")
    print("1. Seasonal trends in top country.")
    print("2. State-level distribution in top country.")
    print("3. Top 5 shapes duration comparison.")

def analyze_wine():
    print("\n--- Wine Analysis ---")
    try:
        df = pd.read_csv('/Users/jin/metabase/TEST/data_init/wineReview.csv')
    except Exception as e:
        print(f"Error loading Wine data: {e}")
        return

    # Clean price
    df = df.dropna(subset=['price', 'points', 'country'])
    df['points_per_price'] = df['points'] / df['price']

    # Global Average Price
    global_avg_price = df['price'].mean()
    print(f"Global Average Price: ${global_avg_price:.2f}")

    # 1. Country Analysis for [A]
    # Filter countries with enough data to be significant (e.g., > 100 reviews)
    country_stats = df.groupby('country').agg({
        'points': 'mean',
        'points_per_price': 'mean',
        'price': 'mean',
        'title': 'count'
    }).rename(columns={'title': 'count'})
    
    country_stats = country_stats[country_stats['count'] > 100]
    
    # Filter for 'High Quality' (Avg Points 85-90)
    high_quality = country_stats[(country_stats['points'] >= 85) & (country_stats['points'] < 90)]
    
    # Sort by points_per_price desc
    top_value_countries = high_quality.sort_values('points_per_price', ascending=False)
    print("\nTop Value Countries (High Quality 85-90):")
    print(top_value_countries.head())

    top_country = top_value_countries.index[0]
    print(f"Top Value Country [A]: {top_country}")

    # 2. Price Difference [B]
    avg_price_a = country_stats.loc[top_country, 'price']
    diff_b = global_avg_price - avg_price_a
    print(f"Average Price in {top_country}: ${avg_price_a:.2f}")
    print(f"Price Difference [B]: ${diff_b:.2f}")

    # 3. Largest Winery in [A] [C]
    df_a = df[df['country'] == top_country]
    winery_counts = df_a['winery'].value_counts()
    print(f"\nWinery Counts in {top_country}:")
    print(winery_counts.head())
    top_winery = winery_counts.idxmax()
    print(f"Largest Winery [C]: {top_winery}")

    # 4. Variety Analysis in [A]
    variety_stats = df_a.groupby('variety').agg({
        'points': 'mean',
        'price': 'mean',
        'title': 'count'
    }).rename(columns={'title': 'count'}).sort_values('count', ascending=False)
    
    print(f"\nTop Varieties in {top_country}:")
    print(variety_stats.head())

    print("\nAdditional Analysis Ideas (Wine):")
    print(f"1. Price distribution of {top_winery} vs others in {top_country}.")
    print(f"2. Best rated variety in {top_country} (min 10 reviews).")
    print(f"3. Relationship between points and price in {top_country} vs global.")

if __name__ == "__main__":
    analyze_ufo()
    analyze_wine()
