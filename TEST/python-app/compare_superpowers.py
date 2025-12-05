import pandas as pd
import os

DATA_DIR = '/Users/jin/metabase/TEST/data_init'

def compare_superpowers():
    df = pd.read_csv(os.path.join(DATA_DIR, 'athlete_events.csv'))
    df = df.dropna(subset=['Medal'])
    
    # Create a new column for the combined entity
    def get_superpower(noc):
        if noc == 'USA':
            return 'USA'
        elif noc in ['URS', 'RUS', 'EUN']:
            return 'URS/RUS/EUN'
        else:
            return 'Other'
            
    df['Superpower'] = df['NOC'].apply(get_superpower)
    df = df[df['Superpower'] != 'Other']
    
    df['Decade'] = (df['Year'] // 10) * 10
    
    # Compare Total Medals
    print("Total Medals:")
    print(df['Superpower'].value_counts())
    
    # Compare by Sport
    print("\nTop Sports by Medal Count for each Superpower:")
    print(df.groupby(['Superpower', 'Sport']).size().reset_index(name='Count').sort_values(['Superpower', 'Count'], ascending=[True, False]).groupby('Superpower').head(3))
    
    # Compare by Decade
    print("\nMedal Count by Decade:")
    pivot = df.pivot_table(index='Decade', columns='Superpower', values='Medal', aggfunc='count').fillna(0)
    print(pivot)
    
    # Check Gymnastics specifically (often a strong suit for URS)
    print("\nGymnastics Medals by Decade:")
    gym = df[df['Sport'] == 'Gymnastics']
    gym_pivot = gym.pivot_table(index='Decade', columns='Superpower', values='Medal', aggfunc='count').fillna(0)
    print(gym_pivot)

if __name__ == "__main__":
    compare_superpowers()
