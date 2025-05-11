import urllib.request
import json
import os

METABASE_URL = "http://metabase:3000"
METABASE_USERNAME = os.environ.get('METABASE_USERNAME')
METABASE_PASSWORD = os.environ.get('METABASE_PASSWORD')

def get_session_token(username, password):
    data = json.dumps({'username': username, 'password': password}).encode('utf-8')
    req = urllib.request.Request(f'{METABASE_URL}/api/session', data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req) as response:
            session_token = json.load(response).get('id')
            print(f"Session token: {session_token}")
            return session_token
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
        print(e.read().decode())
        raise
    except urllib.error.URLError as e:
        print(f"URL Error: {e.reason}")
        raise

def get_dashboards(session_token):
    url = f"{METABASE_URL}/api/dashboard"
    req = urllib.request.Request(url, headers={
        'Content-Type': 'application/json',
        'X-Metabase-Session': session_token
    })
    try:
        with urllib.request.urlopen(req) as response:
            dashboards = json.load(response)
            print(f"Dashboards: {json.dumps(dashboards, indent=2)}")
            return dashboards
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
        print(e.read().decode())
        raise
    except urllib.error.URLError as e:
        print(f"URL Error: {e.reason}")
        raise
