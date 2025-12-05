import pandas as pd
import os

DATA_DIR = '/Users/jin/metabase/TEST/data_init'

def check_urs_rus():
    df = pd.read_csv(os.path.join(DATA_DIR, 'athlete_events.csv'))
    
    urs = df[df['NOC'] == 'URS']
    rus = df[df['NOC'] == 'RUS']
    eun = df[df['NOC'] == 'EUN'] # Unified Team (1992)
    
    print(f"URS (Soviet Union) Count: {len(urs)}")
    if not urs.empty:
        print(f"URS Years: {urs['Year'].min()} - {urs['Year'].max()}")
        
    print(f"\nRUS (Russia) Count: {len(rus)}")
    if not rus.empty:
        print(f"RUS Years: {rus['Year'].min()} - {rus['Year'].max()}")

    print(f"\nEUN (Unified Team) Count: {len(eun)}")
    if not eun.empty:
        print(f"EUN Years: {eun['Year'].min()} - {eun['Year'].max()}")

if __name__ == "__main__":
    check_urs_rus()
