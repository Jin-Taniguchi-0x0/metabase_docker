import statistics

# Data from previous steps
# User: {RecRate, TimeBenefit}
# TimeBenefit > 0 means Faster with Rec relative to task avg.
# TimeBenefit < 0 means Slower with Rec relative to task avg.

data = [
    # Positive Benefit (Faster with Rec)
    {"name": "やん", "benefit": 0.56, "rec_rate": 29},
    {"name": "つばさ", "benefit": 0.27, "rec_rate": 80},
    {"name": "しゅんじ", "benefit": 0.26, "rec_rate": 60},
    {"name": "せり", "benefit": 0.13, "rec_rate": 43},
    {"name": "たくみ", "benefit": 0.13, "rec_rate": 83},
    {"name": "さこ", "benefit": 0.11, "rec_rate": 50},
    
    # Neutral
    {"name": "ゆうご", "benefit": -0.04, "rec_rate": 83},
    
    # Negative Benefit (Slower with Rec)
    {"name": "まさと", "benefit": -0.25, "rec_rate": 14},
    {"name": "たなか", "benefit": -0.27, "rec_rate": 25},
    {"name": "いたい", "benefit": -0.30, "rec_rate": 71},
    {"name": "はなわ", "benefit": -0.30, "rec_rate": 83},
    {"name": "ひろくん", "benefit": -0.31, "rec_rate": 67},
]

def analyze():
    print(f"{'Name':<6} | {'Benefit':<10} | {'Rec Rate':<8} | Group")
    print("-" * 45)
    
    low_usage = [d for d in data if d['rec_rate'] < 40]
    mid_usage = [d for d in data if 40 <= d['rec_rate'] < 70]
    high_usage = [d for d in data if d['rec_rate'] >= 70]
    
    avg_ben_low = statistics.mean([d['benefit'] for d in low_usage])
    avg_ben_mid = statistics.mean([d['benefit'] for d in mid_usage])
    avg_ben_high = statistics.mean([d['benefit'] for d in high_usage])
    
    for d in data:
        print(f"{d['name']:<6} | {d['benefit']:+.2f}      | {d['rec_rate']}%     ")
        
    print("-" * 45)
    print(f"Low Usage (<40%) Avg Benefit: {avg_ben_low:+.2f} (n={len(low_usage)})")
    print(f"Mid Usage (40-69%) Avg Benefit: {avg_ben_mid:+.2f} (n={len(mid_usage)})")
    print(f"High Usage (>=70%) Avg Benefit: {avg_ben_high:+.2f} (n={len(high_usage)})")
    
    # Check User's Hypothesis: "Low Rec -> Slower?" (i.e. Low Usage -> Negative Benefit?)
    print("\n--- Testing Hypothesis: Low Rec -> Slower (Neg Benefit) ---")
    print("Users with Low Usage (<40%):")
    for d in low_usage:
        status = "Slower" if d['benefit'] < 0 else "Faster"
        print(f"  {d['name']}: {d['rec_rate']}% -> {status} ({d['benefit']:+.2f})")

    print("\nUsers with High Usage (>=70%):")
    for d in high_usage:
        status = "Slower" if d['benefit'] < 0 else "Faster"
        print(f"  {d['name']}: {d['rec_rate']}% -> {status} ({d['benefit']:+.2f})")

if __name__ == "__main__":
    analyze()
