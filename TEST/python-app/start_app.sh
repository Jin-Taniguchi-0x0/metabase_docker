#!/bin/sh

# Archive existing logs
if [ -f "logs/app_log.jsonl" ]; then
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    ARCHIVE_DIR="logs/archive_${TIMESTAMP}"
    echo "Archiving old logs to ${ARCHIVE_DIR}..."
    mkdir -p "${ARCHIVE_DIR}"
    mv logs/app_log.jsonl "${ARCHIVE_DIR}/" 2>/dev/null
    mv logs/analysis_summary.csv "${ARCHIVE_DIR}/" 2>/dev/null
    mv logs/user_history.txt "${ARCHIVE_DIR}/" 2>/dev/null
fi

# Wait for Metabase
echo "Waiting for Metabase..."
while ! nc -z metabase 3000; do sleep 1; done;

# Start App
echo "Starting Streamlit App..."
streamlit run app.py --server.port 8080
