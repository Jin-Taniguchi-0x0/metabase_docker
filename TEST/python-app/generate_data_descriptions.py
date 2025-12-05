import pandas as pd
import os
import numpy as np

DATA_DIR = '/Users/jin/metabase/TEST/data_init'

DATASETS = {
    'athlete_events.csv': ['ExperimentTask_Hypothesis_Olympics.md', 'ExperimentTask_Hypothesis_Superpower.md'],
    'wineReview.csv': ['ExperimentTask_Hypothesis_Wine.md'],
    'social_media_ads.csv': ['ExperimentTask_Hypothesis_SocialMedia.md'],
    'UFOscrubbed.csv': ['ExperimentTask_Hypothesis_UFO.md']
}

def get_column_stats_row(df, col):
    dtype = str(df[col].dtype)
    
    if np.issubdtype(df[col].dtype, np.number):
        min_val = df[col].min()
        max_val = df[col].max()
        mean_val = df[col].mean()
        stats = f"最小: {min_val}<br>最大: {max_val}<br>平均: {mean_val:.2f}"
    else:
        # Treat as object/string
        unique_count = df[col].nunique()
        top3 = df[col].value_counts().head(3)
        top3_list = [f"{k} ({v}件)" for k, v in top3.items()]
        top3_str = "<br>".join(top3_list)
        stats = f"ユニーク数: {unique_count}<br>上位3件:<br>{top3_str}"
        
    return f"| {col} | {dtype} | {stats} |"

def generate_description(filename):
    print(f"\nProcessing {filename}...")
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, filename), low_memory=False)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return

    print(f"### **📊 データセット詳細: {filename}**")
    print(f"**概要:** {len(df)} 行, {len(df.columns)} カラム")
    print("\n| カラム名 | データ型 | 統計量・詳細 |")
    print("| :--- | :--- | :--- |")
    
    for col in df.columns:
        try:
            print(get_column_stats_row(df, col))
        except Exception as e:
            print(f"| {col} | Error | {e} |")

if __name__ == "__main__":
    for filename in DATASETS.keys():
        generate_description(filename)
