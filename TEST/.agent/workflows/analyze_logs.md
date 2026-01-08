---
description: analyze_logs
---

# Log Analysis Workflow

This workflow processes application logs to analyze dashboard creation behavior, comparing metrics between "Recommendation Enabled" and "Disabled" conditions across different tasks (datasets).

## Prerequisites

- Log file must exist at `logs/app_log.jsonl`.
- `analyze_all.py` script must be present in the application directory.

## Steps

1. **Navigate to the Application Directory**
   Ensure you are in the correct directory where the python app and logs reside.

   ```bash
   cd /Users/jin/metabase/TEST/python-app
   ```

2. **Run the Analysis Script**
   // turbo
   Execute the analysis script to generate statistics on terminal.

   ```bash
   python3 analyze_all.py
   ```

3. **Check the Output**
   The script calculates:

   - Dashboard creation duration (efficiency).
   - View addition/deletion counts (activity).
   - Card type usage patterns (diversity).
   - Recommendation usage rates (adoption).

   It aggregates these metrics by:

   - Overall Rec Enabled vs Disabled.
   - Per Dataset (Task) x Rec Enabled vs Disabled.

4. **(Optional) Save Report**
   If you need a persistent report, copy the output into a markdown file (e.g., `log_analysis_report.md`).
