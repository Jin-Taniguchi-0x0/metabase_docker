import streamlit as st
import jwt
import time
import requests
import pandas as pd
import os
import torch
import numpy as np
from pykeen.triples import TriplesFactory
from typing import List, Dict, Any, Optional

# --- Metabase & App 設定 ---
METABASE_SITE_URL = "http://localhost:3000"
METABASE_API_URL = "http://metabase:3000"
CARD_DISPLAY_TYPE_MAPPING = {
    "area": "visual-areaChart",
    "bar": "visual-barChart",
    "donut": "visual-donutChart",
    "line": "visual-lineChart",
    "pie": "visual-pieChart",
    "pivot-table": "visual-pivotTable",
    "map": "visual-map",
    "scatter": "visual-scatterChart",
    "table": "visual-table",
    "funnel": "visual-funnel",
    "gauge": "visual-gauge",
    "row": "visual-rowChart",
    "waterfall": "visual-waterfallChart",
    "combo": "visual-comboChart",
    "smartscalar": "visual-scalar",
    "progress": "visual-progress",
    "sankey": "visual-sankey",
    "object": "visual-object"
}

# --- KGEモデル設定 ---
MODEL_DIR = 'RotatE_1.0'
TRIPLES_FILE = 'triple.csv'
RELATION_PATTERN = 'd_j'
CANONICAL_RELATION_NAME = 'view_to_dashboard'
VIEW_PREFIX = 'visual-'

# --- Helper Functions ---
def normalize_id(input_id: Any) -> str:
    if not isinstance(input_id, str):
        input_id = str(input_id)
    translation_table = str.maketrans("０１２３４５６７８９", "0123456789")
    return input_id.translate(translation_table)

# --- Metabase連携関数 ---
def get_metabase_session(username, password):
    api_url = f"{METABASE_API_URL}/api/session"
    credentials = {"username": username, "password": password}
    try:
        response = requests.post(api_url, json=credentials)
        response.raise_for_status()
        return response.json().get("id")
    except requests.exceptions.RequestException as e:
        st.error(f"Metabaseへのログインに失敗しました: {e}")
        return None

def get_dashboard_details(session_id, dashboard_id):
    api_url = f"{METABASE_API_URL}/api/dashboard/{dashboard_id}"
    headers = {"X-Metabase-Session": session_id}
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 404:
            st.error(f"ID '{dashboard_id}' のダッシュボードが見つかりません。MetabaseでIDを確認してください。")
            return None
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"ダッシュボード情報の取得に失敗しました: {e}")
        return None

@st.cache_data
def get_db_and_table_ids(_session_id: str) -> Dict[str, Any]:
    """Sample DatabaseとAccountsテーブル、および関連フィールドのIDを動的に取得する"""
    headers = {"X-Metabase-Session": _session_id}
    try:
        db_response = requests.get(f"{METABASE_API_URL}/api/database", headers=headers)
        db_response.raise_for_status()
        databases = db_response.json()
        
        sample_db = next((db for db in databases.get('data', []) if db['name'] == 'Sample Database'), None)
        if not sample_db:
            st.error("Sample Databaseが見つかりませんでした。")
            return {}
        
        db_id = sample_db['id']
        
        table_response = requests.get(f"{METABASE_API_URL}/api/database/{db_id}/metadata", headers=headers)
        table_response.raise_for_status()
        tables_metadata = table_response.json()
        
        accounts_table = next((tbl for tbl in tables_metadata.get('tables', []) if tbl['name'].upper() == 'ACCOUNTS'), None)
        if not accounts_table:
            st.error("ACCOUNTSテーブルが見つかりませんでした。")
            return {}
        
        country_field = next((fld for fld in accounts_table.get('fields', []) if fld['name'].upper() == 'COUNTRY'), None)
        plan_field = next((fld for fld in accounts_table.get('fields', []) if fld['name'].upper() == 'PLAN'), None)

        if not country_field or not plan_field:
            st.error("COUNTRYまたはPLANフィールドがACCOUNTSテーブルに見つかりませんでした。")
            return {}

        return {
            "db_id": db_id, 
            "table_id": accounts_table['id'],
            "country_field_id": country_field['id'],
            "plan_field_id": plan_field['id']
        }
        
    except requests.exceptions.RequestException as e:
        st.error(f"データベースまたはテーブル情報の取得に失敗しました: {e}")
        return {}

def create_card(session_id: str, card_payload: Dict[str, Any]) -> Optional[int]:
    """Metabaseに新しいカードを作成し、そのIDを返す"""
    api_url = f"{METABASE_API_URL}/api/card"
    headers = {"X-Metabase-Session": session_id}
    try:
        response = requests.post(api_url, headers=headers, json=card_payload)
        response.raise_for_status()
        st.success(f"カード「{card_payload['name']}」が正常に作成されました！")
        return response.json().get('id')
    except requests.exceptions.RequestException as e:
        st.error(f"カードの作成に失敗しました: {e}")
        st.error(f"Metabaseからの応答: {e.response.text}")
        return None

# ★★★ 修正箇所ここから ★★★
def add_card_to_dashboard(session_id: str, dashboard_id: str, card_id: int) -> bool:
    """
    指定されたカードをダッシュボードに追加する。
    PUT /api/dashboard/{id} がdashcardリストの全要素にidを要求するエラーに対応するため、
    新規カードに一時的なIDとして-1を付与してAPIの検証を通過させる。
    """
    dashboard_api_url = f"{METABASE_API_URL}/api/dashboard/{dashboard_id}"
    headers = {"X-Metabase-Session": session_id}

    try:
        # 1. ダッシュボードの現在の状態を取得
        get_response = requests.get(dashboard_api_url, headers=headers)
        get_response.raise_for_status()
        dashboard_data = get_response.json()

        # 2. 既存のカードリストを取得
        dashcards = dashboard_data.get('dashcards', [])
        
        # 3. 新規カードを配置する次の行を計算
        max_row = 0
        if dashcards:
            max_row = max((c.get('row', 0) or 0) + (c.get('size_y', 0) or 0) for c in dashcards)

        # 4. 新規カードの情報を作成し、必須の 'id' キーを追加
        new_dashcard = {
            "id": -1,  # APIの検証を通過させるための一時的なID
            "card_id": card_id,
            "row": max_row,
            "col": 0,
            "size_x": 6,
            "size_y": 4,
            "series": [],
            "visualization_settings": {}
        }
        dashcards.append(new_dashcard)
        
        # 5. PUTリクエスト用にペイロードを再構築
        update_payload = {
            "name": dashboard_data.get("name"),
            "description": dashboard_data.get("description"),
            "dashcards": dashcards
        }
        
        # 6. 更新されたペイロードでダッシュボードを更新
        put_response = requests.put(dashboard_api_url, headers=headers, json=update_payload)
        put_response.raise_for_status()
        
        return True

    except requests.exceptions.RequestException as e:
        st.error(f"カードのダッシュボードへの追加に失敗しました: {e}")
        if e.response:
            st.error(f"Metabaseからの応答: {e.response.text}")
        return False
# ★★★ 修正箇所ここまで ★★★

# --- RotatEモデル用関数 ---
@st.cache_resource
def load_kge_model_and_data():
    print(f"--- モデル '{MODEL_DIR}' とデータを読み込んでいます ---")
    try:
        model = torch.load(os.path.join(MODEL_DIR, 'trained_model.pkl'), weights_only=False)
        model.eval()
        factory_path = os.path.join(MODEL_DIR, 'training_triples.ptf')
        training_factory = TriplesFactory.from_path_binary(factory_path)
        df = pd.read_csv(TRIPLES_FILE, header=None, names=['subject', 'predicate', 'object'])
        df = df.astype(str)
        df['subject'] = df['subject'].str.strip()
        df['object'] = df['object'].str.strip()
        relation_mask = df['predicate'].str.contains(RELATION_PATTERN, na=False)
        relation_df = df[relation_mask].copy()
        swapped_rows_mask = relation_df['subject'].str.contains("dashboard", case=False, na=False)
        relation_df.loc[swapped_rows_mask, ['subject', 'object']] = \
            relation_df.loc[swapped_rows_mask, ['object', 'subject']].values
        relation_df['predicate'] = CANONICAL_RELATION_NAME
        print("モデルとデータの読み込みが完了しました。")
        return model, training_factory, relation_df
    except FileNotFoundError:
        st.error(f"モデルディレクトリ '{MODEL_DIR}' または '{TRIPLES_FILE}' が見つかりません。")
        return None, None, None

def get_recommendations_from_kge(context_views: List[str], top_k: int = 10) -> List[str]:
    kge_model = st.session_state.kge_model
    training_factory = st.session_state.training_factory
    relation_df = st.session_state.relation_df

    if kge_model is None or training_factory is None:
        st.error("KGEモデルがロードされていません。")
        return []

    entity_to_id = training_factory.entity_to_id
    entity_embeddings = kge_model.entity_representations[0](indices=None).detach().cpu().numpy()
    relation_embeddings = kge_model.relation_representations[0](indices=None).detach().cpu().numpy()
    relation_id = training_factory.relation_to_id[CANONICAL_RELATION_NAME]
    relation_embedding = relation_embeddings[relation_id]
    
    inferred_t_vectors = []
    for view in context_views:
        if view in entity_to_id:
            view_embedding = entity_embeddings[entity_to_id[view]]
            inferred_t_vectors.append(view_embedding * relation_embedding)

    if not inferred_t_vectors:
        st.warning("コンテキストビューがモデルの語彙に一つも見つかりませんでした。")
        return []
    
    inferred_dashboard_embedding = np.mean(inferred_t_vectors, axis=0)
    
    all_entities = set(entity_to_id.keys())
    all_dashboards = set(relation_df['object'].unique())
    candidate_views = [e for e in (all_entities - all_dashboards) if e.startswith(VIEW_PREFIX)]
    
    scores = []
    for view in candidate_views:
        if view in context_views or view not in entity_to_id:
            continue
        view_embedding = entity_embeddings[entity_to_id[view]]
        h_r = view_embedding * relation_embedding
        score = np.linalg.norm(h_r - inferred_dashboard_embedding)
        scores.append({'view': view, 'score': float(score)})
        
    scores.sort(key=lambda x: x['score'])
    
    return [item['view'] for item in scores[:top_k]]

# --- Streamlit UI & メインロジック ---
def display_credentials_form():
    st.header("Metabase 認証情報")
    with st.form("credentials_form"):
        username = st.text_input("Metabase Username")
        password = st.text_input("Metabase Password", type="password")
        dashboard_id = st.text_input("Dashboard ID")
        secret_key = st.text_input("Metabase Secret Key", type="password")
        
        submitted = st.form_submit_button("接続")
        if submitted:
            session_id = get_metabase_session(username, password)
            if session_id:
                st.session_state.metabase_session_id = session_id
                st.session_state.dashboard_id = dashboard_id
                st.session_state.secret_key = secret_key
                st.rerun()

def embed_dashboard():
    secret_key = st.session_state.secret_key
    dashboard_id = normalize_id(st.session_state.dashboard_id)
    
    if not secret_key or not dashboard_id: return

    payload = {"resource": {"dashboard": int(dashboard_id)}, "params": {}, "exp": round(time.time()) + (60 * 10)}
    token = jwt.encode(payload, secret_key, algorithm="HS256")
    iframe_url = f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"
    st.components.v1.iframe(iframe_url, height=800, scrolling=True)

def main():
    st.set_page_config(layout="wide")
    st.title("ダッシュボードビュー推薦システム (RotatE版)")

    if 'metabase_session_id' not in st.session_state: st.session_state.metabase_session_id = None
    if 'dashboard_id' not in st.session_state: st.session_state.dashboard_id = ""
    if 'secret_key' not in st.session_state: st.session_state.secret_key = ""
    if 'kge_model' not in st.session_state:
        st.session_state.kge_model, st.session_state.training_factory, st.session_state.relation_df = load_kge_model_and_data()

    if st.session_state.metabase_session_id is None:
        display_credentials_form()
    else:
        embed_dashboard()
        
        st.header("ビュー推薦")
        
        dashboard_id = normalize_id(st.session_state.dashboard_id)
        
        if dashboard_id:
            dashboard_details = get_dashboard_details(st.session_state.metabase_session_id, dashboard_id)
            current_views = []
            if dashboard_details:
                dashcards = dashboard_details.get("dashcards", [])
                st.write("現在のダッシュボードに含まれるビュータイプ:")
                card_views = [dashcard.get("card", {}).get("display") for dashcard in dashcards if dashcard.get("card")]
                current_views = [view for view in [CARD_DISPLAY_TYPE_MAPPING.get(v) for v in card_views] if view is not None]
                st.json(current_views)

            if st.button("このダッシュボードにおすすめのビューを生成"):
                if current_views:
                    with st.spinner("RotatEモデルで推薦を生成中..."):
                        recommendations = get_recommendations_from_kge(context_views=current_views, top_k=10)
                    
                    if recommendations:
                        st.success("おすすめのビューが見つかりました！")
                        st.write(recommendations)
                    else:
                        st.info("推薦できるビューはありませんでした。")
                else:
                    st.warning("推薦の基となるビューがダッシュボードにありません。")

        st.header("サンプルチャート作成＆ダッシュボードに追加")
        st.write("`Sample Database`の`ACCOUNTS`テーブルからサンプルチャートを作成し、現在表示中のダッシュボードに追加します。")
        
        ids = get_db_and_table_ids(st.session_state.metabase_session_id)
        if ids:
            col1, col2 = st.columns(2)
            with col1:
                if st.button("棒グラフを作成＆追加"):
                    bar_chart_payload = {
                        "name": f"Sample Bar Chart (Accounts by Country) - {int(time.time())}",
                        "display": "bar",
                        "dataset_query": {
                            "type": "query", "database": ids['db_id'],
                            "query": {
                                "source-table": ids['table_id'],
                                "aggregation": [["count"]],
                                "breakout": [["field", ids['country_field_id'], None]]
                            }
                        },
                        "visualization_settings": {}
                    }
                    with st.spinner("棒グラフを作成中..."):
                        card_id = create_card(st.session_state.metabase_session_id, bar_chart_payload)
                    if card_id:
                        with st.spinner("ダッシュボードに追加中..."):
                            success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id)
                        if success:
                            st.success("ダッシュボードに追加しました！ページを更新します。")
                            time.sleep(2)
                            st.rerun()

            with col2:
                if st.button("円グラフを作成＆追加"):
                    pie_chart_payload = {
                        "name": f"Sample Pie Chart (Accounts by Plan) - {int(time.time())}",
                        "display": "pie",
                        "dataset_query": {
                            "type": "query", "database": ids['db_id'],
                            "query": {
                                "source-table": ids['table_id'],
                                "aggregation": [["count"]],
                                "breakout": [["field", ids['plan_field_id'], None]]
                            }
                        },
                        "visualization_settings": {}
                    }
                    with st.spinner("円グラフを作成中..."):
                        card_id = create_card(st.session_state.metabase_session_id, pie_chart_payload)
                    if card_id:
                        with st.spinner("ダッシュボードに追加中..."):
                            success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id)
                        if success:
                            st.success("ダッシュボードに追加しました！ページを更新します。")
                            time.sleep(2)
                            st.rerun()

if __name__ == '__main__':
    main()
