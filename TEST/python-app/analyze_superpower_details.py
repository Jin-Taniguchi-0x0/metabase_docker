import pandas as pd
import os

DATA_DIR = '/Users/jin/metabase/TEST/data_init'

def analyze_superpower_details():
    df = pd.read_csv(os.path.join(DATA_DIR, 'athlete_events.csv'))
    df = df.dropna(subset=['Medal'])
    
    def get_superpower(noc):
        if noc == 'USA':
            return 'USA'
        elif noc in ['URS', 'RUS', 'EUN']:
            return 'URS/RUS/EUN'
        else:
            return 'Other'
            
    df['Superpower'] = df['NOC'].apply(get_superpower)
    df = df[df['Superpower'] != 'Other']
    
    # Gender Analysis
    print("Female Medal Count by Superpower:")
    print(df[df['Sex'] == 'F'].groupby('Superpower').size())
    print("\nMale Medal Count by Superpower:")
    print(df[df['Sex'] == 'M'].groupby('Superpower').size())
    
    # Season Analysis
    print("\nWinter Medal Count by Superpower:")
    print(df[df['Season'] == 'Winter'].groupby('Superpower').size())
    print("\nSummer Medal Count by Superpower:")
    print(df[df['Season'] == 'Summer'].groupby('Superpower').size())

if __name__ == "__main__":
    analyze_superpower_details()
