import re
import statistics

# Manually transcribe the data from the summary for stability/speed (or parse, but manual is safer for small n=12)
# Format: User, Sus, Task, Cond, Time, Views, Unique, RecRate, Read, Req, Use, Sur

# Task Averages (Time)
AVG_TIME = {
    ("UFO", "No Rec"): 20.78,
    ("UFO", "Rec"): 22.85,
    ("Wine", "No Rec"): 26.49,
    ("Wine", "Rec"): 24.51
}

# User Data (from the table provided in previous turn)
users = [
    {"name": "せり", "rows": [
        {"task": "UFO", "cond": "No Rec", "time": 20.4, "read": 5, "req": 4},
        {"task": "Wine", "cond": "Rec", "time": 20.8, "read": 5, "req": 4, "use": 4, "sur": 4}
    ]},
    {"name": "さこ", "rows": [
        {"task": "Wine", "cond": "No Rec", "time": 34.9, "read": 2, "req": 4},
        {"task": "UFO", "cond": "Rec", "time": 27.7, "read": 4, "req": 4, "use": 4, "sur": 3}
    ]},
    {"name": "ひろくん", "rows": [
        {"task": "UFO", "cond": "Rec", "time": 27.5, "read": 5, "req": 4, "use": 4, "sur": 4},
        {"task": "Wine", "cond": "No Rec", "time": 23.8, "read": 4, "req": 4}
    ]},
    {"name": "まさと", "rows": [
        {"task": "Wine", "cond": "Rec", "time": 25.6, "read": 4, "req": 5, "use": 4, "sur": 3},
        {"task": "UFO", "cond": "No Rec", "time": 16.6, "read": 4, "req": 5}
    ]},
    {"name": "たくみ", "rows": [
        {"task": "UFO", "cond": "No Rec", "time": 22.6, "read": 4, "req": 5},
        {"task": "Wine", "cond": "Rec", "time": 23.5, "read": 5, "req": 5, "use": 5, "sur": 4}
    ]},
    {"name": "しゅんじ", "rows": [
        {"task": "Wine", "cond": "No Rec", "time": 28.4, "read": 2, "req": 5},
        {"task": "UFO", "cond": "Rec", "time": 18.6, "read": 2, "req": 4, "use": 4, "sur": 2} # Wait, Rec time 18.6 is fast. UFO Rec Avg is 22.9
    ]},
    {"name": "つばさ", "rows": [
        {"task": "UFO", "cond": "Rec", "time": 16.1, "read": 4, "req": 5, "use": 5, "sur": 4},
        {"task": "Wine", "cond": "No Rec", "time": 25.9, "read": 4, "req": 4}
    ]},
    {"name": "たなか", "rows": [
        {"task": "Wine", "cond": "Rec", "time": 25.5, "read": 4, "req": 4, "use": 4, "sur": 4},
        {"task": "UFO", "cond": "No Rec", "time": 16.0, "read": 4, "req": 4}
    ]},
    {"name": "やん", "rows": [
        {"task": "UFO", "cond": "No Rec", "time": 28.4, "read": 4, "req": 5},
        {"task": "Wine", "cond": "Rec", "time": 19.8, "read": 4, "req": 4, "use": 2, "sur": 3}
    ]},
    {"name": "ゆうご", "rows": [
        {"task": "Wine", "cond": "No Rec", "time": 25.6, "read": 4, "req": 4},
        {"task": "UFO", "cond": "Rec", "time": 22.9, "read": 4, "req": 4, "use": 4, "sur": 4}
    ]},
    {"name": "いたい", "rows": [
        {"task": "UFO", "cond": "Rec", "time": 24.3, "read": 4, "req": 4, "use": 3, "sur": 4},
        {"task": "Wine", "cond": "No Rec", "time": 20.3, "read": 4, "req": 4}
    ]},
    {"name": "はなわ", "rows": [
        {"task": "Wine", "cond": "Rec", "time": 31.7, "read": 4, "req": 5, "use": 4, "sur": 4},
        {"task": "UFO", "cond": "No Rec", "time": 20.7, "read": 3, "req": 5}
    ]}
]

def analyze():
    print("User Sensitivity Analysis (Relative to Task Average)\n")
    print(f"{'Name':<6} | {'Delta Time (Rec - NoRec Adjusted)':<30} | {'Readability Change':<20} | {'Rec Useful'}")
    print("-" * 80)
    
    # We want to see if Rec improved their time *relative to the task baseline*
    # Improvement Metric = (NoRec_Time - TaskNoRecAvg) - (Rec_Time - TaskRecAvg)
    # Positive value => Rec helped them be faster relative to peers?
    # Actually simpler:
    #   Performance Ratio Rec = UserTimeRec / AvgTimeRec
    #   Performance Ratio NoRec = UserTimeNoRec / AvgTimeNoRec
    #   Improvement = Ratio NoRec - Ratio Rec (Higher is better, meaning they got faster relative to norm)
    
    results = []
    
    for u in users:
        rec_row = next(r for r in u['rows'] if r['cond'] == 'Rec')
        norec_row = next(r for r in u['rows'] if r['cond'] == 'No Rec')
        
        # Time Impact
        # How much did they deviate from average in Rec vs No Rec?
        # Ratio < 1 means faster than average.
        
        avg_rec = AVG_TIME[(rec_row['task'], "Rec")]
        avg_norec = AVG_TIME[(norec_row['task'], "No Rec")]
        
        ratio_rec = rec_row['time'] / avg_rec
        ratio_norec = norec_row['time'] / avg_norec
        
        # If Ratio Rec < Ratio No Rec, they improved more (or worsened less) than the task difference implies
        # Let's call "Rec Benefit Score" = Ratio NoRec - Ratio Rec
        # Example: NoRec 1.2 (slow), Rec 0.8 (fast) => Benefit 0.4
        time_benefit = ratio_norec - ratio_rec
        
        # Readability Impact
        read_diff = rec_row['read'] - norec_row['read']
        
        results.append({
            "name": u['name'],
            "time_benefit": time_benefit,
            "read_diff": read_diff,
            "usefulness": rec_row.get('use'),
            "surprise": rec_row.get('sur'),
            "raw_time_rec": rec_row['time'],
            "raw_time_norec": norec_row['time']
        })

    # Sort by Time Benefit
    results.sort(key=lambda x: x['time_benefit'], reverse=True)
    
    for r in results:
        benefited = "Yes" if r['time_benefit'] > 0.05 else ("No" if r['time_benefit'] < -0.05 else "Neutral")
        print(f"{r['name']:<6} | {r['time_benefit']:+.2f} ({benefited}) | {r['read_diff']:+d} | {r['usefulness']}")

    print("\n--- Grouping ---")
    high_benefit = [r for r in results if r['time_benefit'] > 0.1]
    negative_benefit = [r for r in results if r['time_benefit'] < -0.1]
    neutral = [r for r in results if -0.1 <= r['time_benefit'] <= 0.1]
    
    print(f"Positively Affected (Faster): {len(high_benefit)}")
    for r in high_benefit: print(f"  {r['name']} (Benefit: {r['time_benefit']:.2f}, Useful: {r['usefulness']})")
    
    print(f"Negatively Affected (Slower): {len(negative_benefit)}")
    for r in negative_benefit: print(f"  {r['name']} (Benefit: {r['time_benefit']:.2f}, Useful: {r['usefulness']})")
          
    print(f"Neutral: {len(neutral)}")
    for r in neutral: print(f"  {r['name']} (Benefit: {r['time_benefit']:.2f}, Useful: {r['usefulness']})")

if __name__ == "__main__":
    analyze()
