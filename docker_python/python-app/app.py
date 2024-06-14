from flask import Flask, jsonify, request
import os
from metabase_client import get_dashboards, get_session_token

app = Flask(__name__)

# Metabaseの認証情報
METABASE_USERNAME = os.environ.get('METABASE_USERNAME')
METABASE_PASSWORD = os.environ.get('METABASE_PASSWORD')
SESSION_ID = None

@app.route('/')
def home():
    return "Welcome to the Metabase Flask API"

@app.route('/get_session')
def get_session():
    global SESSION_ID
    try:
        # セッションIDを取得
        SESSION_ID = get_session_token(METABASE_USERNAME, METABASE_PASSWORD)
        return jsonify({"session_id": SESSION_ID})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/dashboards')
def dashboards():
    global SESSION_ID
    try:
        if not SESSION_ID:
            return jsonify({"error": "Session ID is not set. Please get the session ID first."})
        
        # セッションIDを使用してダッシュボード情報を取得
        dashboards = get_dashboards(SESSION_ID)
        return jsonify(dashboards)
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
