import pandas as pd
import os

DATA_DIR = '/Users/jin/metabase/TEST/data_init'

def check_dominance():
    df = pd.read_csv(os.path.join(DATA_DIR, 'athlete_events.csv'))
    df = df.dropna(subset=['Medal'])
    df['Decade'] = (df['Year'] // 10) * 10
    
    # Count medals by Country, Sport, Decade
    grouped = df.groupby(['NOC', 'Sport', 'Decade']).size().reset_index(name='MedalCount')
    
    # Check for > 300
    print("Combinations with > 300 medals in a decade:")
    print(grouped[grouped['MedalCount'] > 300])
    
    # Check for > 250
    print("\nCombinations with > 250 medals in a decade:")
    print(grouped[grouped['MedalCount'] > 250])

    # Check USA Athletics vs Swimming totals
    usa = df[df['NOC'] == 'USA']
    print("\nUSA Total Medals by Sport (Top 5):")
    print(usa['Sport'].value_counts().head(5))

if __name__ == "__main__":
    check_dominance()
