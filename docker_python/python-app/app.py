from flask import Flask, jsonify
import requests
import os

app = Flask(__name__)
METABASE_URL = os.getenv('METABASE_URL')

@app.route('/')
def home():
    response = requests.get(f'{METABASE_URL}/api/health')
    return jsonify(response.json())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
