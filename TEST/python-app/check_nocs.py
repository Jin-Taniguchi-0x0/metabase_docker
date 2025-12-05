import pandas as pd
import os

DATA_DIR = '/Users/jin/metabase/TEST/data_init'

def check_nocs():
    df = pd.read_csv(os.path.join(DATA_DIR, 'athlete_events.csv'))
    print("Top 5 NOCs by row count:")
    print(df['NOC'].value_counts().head(5))
    
    candidates = ['USA', 'URS', 'GBR', 'FRA']
    print("\nCheck specific candidates:")
    for noc in candidates:
        count = df[df['NOC'] == noc].shape[0]
        print(f"{noc}: {count}")

if __name__ == "__main__":
    check_nocs()
