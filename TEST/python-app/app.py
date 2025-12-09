import streamlit as st
import jwt
import time
import requests
import pandas as pd
import os
import torch
import numpy as np
from pykeen.triples import TriplesFactory
from typing import List, Dict, Any, Optional, Tuple
import plotly.express as px
from datetime import datetime
import json
from streamlit_session_browser_storage import SessionStorage

import plotly.graph_objects as go

# --- Metabase & App 設定 ---
METABASE_SITE_URL = "http://localhost:3000"
METABASE_API_URL = "http://metabase:3000"

# 分析用DBの接続情報
DATA_DB_CONFIG = {
    "name": "Analytics DB", 
    "engine": "postgres",
    "details": {
        "host": os.getenv("DATA_DB_HOST", "data_db"),
        "port": int(os.getenv("DATA_DB_PORT", 5432)),
        "dbname": os.getenv("DATA_DB_NAME", "data_db"),
        "user": os.getenv("DATA_DB_USER", "data_user"),
        "password": os.getenv("DATA_DB_PASS", "data_password"),
        "let-user-control-scheduling": False
    }
}

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
    "object": "visual-object",
    "scalar": "visual-scalar"
}
REVERSE_CARD_DISPLAY_TYPE_MAPPING = {v: k for k, v in CARD_DISPLAY_TYPE_MAPPING.items()}
# KGEモデルの出力(visual-*)との互換性を確保
REVERSE_CARD_DISPLAY_TYPE_MAPPING.update({
    "visual-areaChart": "area",
    "visual-barChart": "bar",
    "visual-donutChart": "pie",
    "visual-lineChart": "line",
    "visual-pieChart": "pie",
    "visual-pivotTable": "pivot-table",
    "visual-map": "map",
    "visual-scatterChart": "scatter",
    "visual-table": "table",
    "visual-funnel": "funnel",
    "visual-gauge": "gauge",
    "visual-rowChart": "row",
    "visual-waterfallChart": "waterfall",
    "visual-comboChart": "combo",
    "visual-scalar": "scalar",
    "visual-progress": "progress",
    "visual-sankey": "sankey",
    "visual-object": "object",
    "donut": "pie"
})
CHART_ICONS = {
    "bar": "📊", "line": "📈", "area": "📉", "pie": "🥧", 
    "scatter": "✨", "pivot-table": "🧮", "table": "📋",
    "funnel": "🏺", "gauge": "⏱️", "row": "📊", "waterfall": "🌊",
    "scalar": "🔢", "donut": "🍩", "map": "🗺️"
}

SIZE_MAPPING = {
    'S (幅1/3)': {'width': 8, 'height': 5},
    'M (幅1/2)': {'width': 12, 'height': 10},
    'L (幅2/3)': {'width': 16, 'height': 10}
}
JOIN_STRATEGY_MAP = {
    "左外部結合 (Left Join)": "left-join",
    "内部結合 (Inner Join)": "inner-join",
    "右外部結合 (Right Join)": "right-join"
}
JOIN_STRATEGY_DISPLAY_MAP = {v: k for k, v in JOIN_STRATEGY_MAP.items()}
CHART_TYPE_MAP = {
    "棒グラフ": "bar",
    "折れ線グラフ": "line",
    "エリアグラフ": "area",
    "円グラフ": "pie",
    "数値": "scalar",
    "ゲージ": "gauge",
    "散布図": "scatter",
    "ピボットテーブル": "pivot-table",
    "地図": "map",
    "ファンネル": "funnel",
    "ウォーターフォール": "waterfall",
    "テーブル": "table",
}
REVERSE_CHART_TYPE_MAP = {v: k for k, v in CHART_TYPE_MAP.items()}


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

def find_empty_space(dashcards: List[Dict], card_width: int, card_height: int, grid_columns: int = 24) -> Tuple[int, int]:
    if not dashcards:
        return (0, 0)
    max_row_so_far = max((c.get('row', 0) + c.get('size_y', 0)) for c in dashcards) if dashcards else 0
    grid_height = max_row_so_far + card_height
    grid_map = np.zeros((grid_height, grid_columns), dtype=int)
    for card in dashcards:
        col, row, width, height = card.get('col', 0), card.get('row', 0), card.get('size_x', 6), card.get('size_y', 4)
        grid_map[row:row+height, col:col+width] = 1
    for r in range(grid_height - card_height + 1):
        for c in range(grid_columns - card_width + 1):
            if np.sum(grid_map[r:r+card_height, c:c+card_width]) == 0:
                return (r, c)
    return (max_row_so_far, 0)

def _deduplicate_columns(column_names: List[str]) -> List[str]:
    new_names = []
    counts = {}
    for name in column_names:
        if name in counts:
            counts[name] += 1
            new_names.append(f"{name}_{counts[name]}")
        else:
            counts[name] = 1
            new_names.append(name)
    return new_names

def add_log_entry(action: str, details: Dict):
    ss = SessionStorage()
    log = ss.getItem('operation_log') 
    if log is None:
        log = []
    
    # 基本情報の収集
    timestamp = datetime.now().isoformat()
    user_id = st.session_state.get("username", "unknown")
    use_rec = st.session_state.get("use_recommendation", True)
    dashboard_id = st.session_state.get("dashboard_id", "")
    
    entry = {
        "timestamp": timestamp,
        "user_id": user_id,
        "dashboard_id": dashboard_id,
        "recommendation_enabled": use_rec,
        "action": action,
        **details
    }
    
    # Session Storage (UI表示用)
    log.append(entry)
    ss.setItem('operation_log', log)
    
    # Server-side File Logging (分析用)
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, "app_log.jsonl")
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Failed to write log to file: {e}")

    print(f"LOG: {entry}")

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
            st.error(f"ID '{dashboard_id}' のダッシュボードが見つかりません。")
            return None
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"ダッシュボード情報の取得に失敗しました: {e}")
        return None

@st.cache_data(ttl=60) # キャッシュ時間を短く設定
def get_all_tables_metadata(_session_id: str) -> Tuple[Optional[int], Optional[List[Dict]]]:
    """
    Sample Database以外の最初のデータベースを取得し、その中の全てのテーブルを返す。
    隠しテーブルも含める設定 (include_hidden=true) を追加。
    """
    headers = {"X-Metabase-Session": _session_id}
    try:
        # 1. 全データベース取得
        db_response = requests.get(f"{METABASE_API_URL}/api/database", headers=headers)
        db_response.raise_for_status()
        databases = db_response.json().get('data', [])
        
        # Sample Database 以外を取得
        target_db = next((db for db in databases if db['name'] != 'Sample Database'), None)
        
        if not target_db:
            return None, None

        db_id = target_db['id']
        
        # 2. ターゲットDBの全テーブルメタデータを取得 (include_hidden=true を追加)
        # APIドキュメント: GET /api/database/{id}/metadata
        meta_url = f"{METABASE_API_URL}/api/database/{db_id}/metadata?include_hidden=true"
        meta_response = requests.get(meta_url, headers=headers)
        meta_response.raise_for_status()
        
        tables = meta_response.json().get('tables', [])
        return db_id, tables
        
    except requests.exceptions.RequestException as e:
        st.error(f"メタデータの取得に失敗しました: {e}")
        return None, None

# 古い関数 ensure_and_get_analytics_db_id は削除し、get_all_tables_metadata に統合しました

def create_card(session_id: str, card_payload: Dict[str, Any]) -> Optional[int]:
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

def add_card_to_dashboard(session_id: str, dashboard_id: str, card_id: int, size_x: int, size_y: int) -> bool:
    dashboard_api_url = f"{METABASE_API_URL}/api/dashboard/{dashboard_id}"
    headers = {"X-Metabase-Session": session_id}
    try:
        get_response = requests.get(dashboard_api_url, headers=headers)
        get_response.raise_for_status()
        dashboard_data = get_response.json()
        has_tabs = "tabs" in dashboard_data and isinstance(dashboard_data.get("tabs"), list) and len(dashboard_data["tabs"]) > 0
        if has_tabs:
            target_tab = dashboard_data["tabs"][0]
            dashcards = target_tab.get("dashcards", [])
        else:
            dashcards = dashboard_data.get('dashcards', [])
        new_row, new_col = find_empty_space(dashcards, size_x, size_y)
        new_dashcard = {"id": -1, "card_id": card_id, "col": new_col, "row": new_row, "size_x": size_x, "size_y": size_y, "series": [], "visualization_settings": {}}
        dashcards.append(new_dashcard)
        update_payload = {"name": dashboard_data.get("name"), "description": dashboard_data.get("description")}
        if has_tabs:
            target_tab["dashcards"] = dashcards
            update_payload["tabs"] = dashboard_data["tabs"]
        else:
            update_payload["dashcards"] = dashcards
        put_response = requests.put(dashboard_api_url, headers=headers, json=update_payload)
        put_response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"カードのダッシュボードへの追加に失敗しました: {e}")
        if e.response: st.error(f"Metabaseからの応答: {e.response.text}")
        return False

def remove_card_from_dashboard(session_id: str, dashboard_id: str, dashcard_id_to_remove: int) -> bool:
    dashboard_api_url = f"{METABASE_API_URL}/api/dashboard/{dashboard_id}"
    headers = {"X-Metabase-Session": session_id}
    try:
        get_response = requests.get(dashboard_api_url, headers=headers)
        get_response.raise_for_status()
        dashboard_data = get_response.json()
        has_tabs = "tabs" in dashboard_data and isinstance(dashboard_data.get("tabs"), list) and len(dashboard_data["tabs"]) > 0
        if has_tabs:
            target_tab = dashboard_data["tabs"][0]
            dashcards_list = target_tab.get("dashcards", [])
        else:
            dashcards_list = dashboard_data.get('dashcards', [])
        original_count = len(dashcards_list)
        new_dashcards_list = [card for card in dashcards_list if card.get("id") != dashcard_id_to_remove]
        if len(new_dashcards_list) == original_count:
            st.warning(f"ID {dashcard_id_to_remove} のカードがダッシュボード上に見つかりません。")
            return False
        update_payload = {"name": dashboard_data.get("name"), "description": dashboard_data.get("description")}
        if has_tabs:
            target_tab["dashcards"] = new_dashcards_list
            update_payload["tabs"] = dashboard_data["tabs"]
        else:
            update_payload["dashcards"] = new_dashcards_list
        put_response = requests.put(dashboard_api_url, headers=headers, json=update_payload)
        put_response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"カードのダッシュボードからの削除に失敗しました: {e}")
        if e.response: st.error(f"Metabaseからの応答: {e.response.text}")
        return False

def execute_query(session_id: str, dataset_query: Dict[str, Any]) -> Optional[Dict]:
    api_url = f"{METABASE_API_URL}/api/dataset"
    headers = {"X-Metabase-Session": session_id}
    try:
        response = requests.post(api_url, headers=headers, json=dataset_query)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"クエリの実行に失敗しました: {e}")
        if e.response:
            st.error(f"Metabaseからの応答: {e.response.text}")
        return None

# --- RotatEモデル用関数 ---
@st.cache_resource
def load_kge_model_and_data():
    if not os.path.exists(MODEL_DIR) or not os.path.exists(TRIPLES_FILE):
        st.error(f"モデルディレクトリ '{MODEL_DIR}' または '{TRIPLES_FILE}' が見つかりません。")
        return None, None, None
    print(f"--- モデル '{MODEL_DIR}' とデータを読み込んでいます ---")
    model = torch.load(os.path.join(MODEL_DIR, 'trained_model.pkl'), weights_only=False)
    model.eval()
    factory_path = os.path.join(MODEL_DIR, 'training_triples.ptf')
    training_factory = TriplesFactory.from_path_binary(factory_path)
    df = pd.read_csv(TRIPLES_FILE, header=None, names=['subject', 'predicate', 'object'])
    df = df.astype(str).apply(lambda x: x.str.strip())
    relation_mask = df['predicate'].str.contains(RELATION_PATTERN, na=False)
    relation_df = df[relation_mask].copy()
    swapped_rows_mask = relation_df['subject'].str.contains("dashboard", case=False, na=False)
    relation_df.loc[swapped_rows_mask, ['subject', 'object']] = relation_df.loc[swapped_rows_mask, ['object', 'subject']].values
    relation_df['predicate'] = CANONICAL_RELATION_NAME
    print("モデルとデータの読み込みが完了しました。")
    return model, training_factory, relation_df

def get_recommendations_from_kge(context_views: List[str], top_k: int = 10) -> List[str]:
    kge_model, training_factory = st.session_state.kge_model, st.session_state.training_factory
    if kge_model is None or training_factory is None: return []
    entity_to_id = training_factory.entity_to_id
    entity_embeddings = kge_model.entity_representations[0](indices=None).detach().cpu().numpy()
    relation_embeddings = kge_model.relation_representations[0](indices=None).detach().cpu().numpy()
    relation_id = training_factory.relation_to_id.get(CANONICAL_RELATION_NAME)
    if relation_id is None: return []
    relation_embedding = relation_embeddings[relation_id]
    inferred_t_vectors = [entity_embeddings[entity_to_id[view]] * relation_embedding for view in context_views if view in entity_to_id]
    if not inferred_t_vectors: return list(CARD_DISPLAY_TYPE_MAPPING.values())[:top_k]
    inferred_dashboard_embedding = np.mean(inferred_t_vectors, axis=0)
    candidate_views = [view for view in CARD_DISPLAY_TYPE_MAPPING.values() if view not in context_views and view in entity_to_id]
    scores = [{'view': view, 'score': float(np.linalg.norm((entity_embeddings[entity_to_id[view]] * relation_embedding) - inferred_dashboard_embedding))} for view in candidate_views]
    scores.sort(key=lambda x: x['score'])
    
    # 重複排除ロジック: マッピング後の表示タイプでユニークにする
    unique_recommendations = []
    seen_display_types = set()
    
    # 候補を多めに取得してフィルタリング (top_k * 3)
    for item in scores:
        view_name = item['view']
        # マッピング後のタイプを取得 (例: visual-donutChart -> pie)
        display_type = REVERSE_CARD_DISPLAY_TYPE_MAPPING.get(view_name, view_name)
        
        if display_type not in seen_display_types:
            unique_recommendations.append(view_name)
            seen_display_types.add(display_type)
            
        if len(unique_recommendations) >= top_k:
            break
            
    return unique_recommendations

# --- クエリビルダー関連ロジック ---

def get_all_available_fields(selections: Dict) -> List[Dict]:
    all_fields = []
    for field in selections.get("available_fields", []):
        field_copy = field.copy()
        field_copy['mbql_ref'] = ["field", field['id'], None]
        field_copy['display_name_with_table'] = f"{selections.get('table_name', '')} -> {field['display_name']}"
        all_fields.append(field_copy)
    for join in selections.get("joins", []):
        join_alias = join["join_alias"]
        target_table = next((tbl for tbl in st.session_state.tables_metadata if tbl['id'] == join['target_table_id']), None)
        if target_table:
            for field in target_table.get("fields", []):
                field_copy = field.copy()
                field_copy['mbql_ref'] = ["field", field['id'], {"join-alias": join_alias}]
                field_copy['display_name_with_table'] = f"{target_table['display_name']} ({join_alias}) -> {field['display_name']}"
                all_fields.append(field_copy)
    return all_fields

def handle_table_selection(selections: Dict, key_prefix: str):
    selected_table_name = st.session_state.get(f"{key_prefix}selected_table_name_key")
    if selected_table_name:
        table_options = {tbl['display_name']: tbl for tbl in st.session_state.tables_metadata}
        selected_table = table_options[selected_table_name]
        selections.update({
            "table_id": selected_table['id'], "table_name": selected_table_name,
            "available_fields": selected_table.get('fields', []),
            "joins": [], "filters": [], "aggregation": [], "breakout_id": None
        })
    else:
        selections.update({"table_id": None, "table_name": None, "available_fields": [], "filters": [], "joins": []})

def handle_custom_chart_submission(payload: Dict[str, Any], size_key: str):
    dashboard_id = normalize_id(st.session_state.dashboard_id)
    card_size_choice = st.session_state.get(size_key)
    card_size = SIZE_MAPPING.get(card_size_choice)
    with st.spinner("グラフを作成中..."):
        card_id = create_card(st.session_state.metabase_session_id, payload)
    if card_id:
        with st.spinner("ダッシュボードに追加中..."):
            success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
        if success:
            st.success("ダッシュボードに追加しました！")
            task_duration = time.time() - st.session_state.task_start_time if st.session_state.task_start_time else None
            log_details = {
                "card_name": payload['name'],
                "card_type": payload['display'],
                "task_duration_sec": task_duration
            }
            if st.session_state.pending_recommendation:
                log_details["recommendation_source"] = "recommendation"
                log_details.update(st.session_state.pending_recommendation)
            else:
                log_details["recommendation_source"] = "custom"
            add_log_entry("create_view", log_details)
            st.session_state.show_builder_dialog = False
            # テーブル選択情報を保持しつつ、他の設定をリセット
            current_selections = st.session_state.custom_builder_selections
            st.session_state.custom_builder_selections = {
                "table_id": current_selections.get("table_id"),
                "table_name": current_selections.get("table_name"),
                "available_fields": current_selections.get("available_fields", []),
                "chart_display_name": None,
                "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None
            }
            st.session_state.preview_data = None
            st.session_state.task_start_time = None
            st.session_state.pending_recommendation = None
            if 'recommendations' in st.session_state:
                del st.session_state.recommendations
            time.sleep(2)
            st.rerun()

def display_existing_filters(selections: Dict, key_prefix: str = ""):
    # フィルターリストのコピーを作成してイテレート（削除操作などで安全のため）
    filters = selections["filters"]
    
    for i, f in enumerate(filters):
        # 1. フィルター情報の表示
        value_str = f"`{f['value1']}`" + (f" と `{f['value2']}`" if f.get('value2') is not None else "")
        cols = st.columns([4, 3, 3, 1])
        cols[0].info(f"`{f['field_name']}`")
        cols[1].info(f"{f['operator_name']}")
        cols[2].info(value_str)
        
        if cols[3].button("🗑️", key=f"{key_prefix}del_filter_{i}", help="このフィルターを削除"):
            selections["filters"].pop(i)
            st.rerun()

        # 2. 次の条件との結合（最後の要素以外）
        if i < len(filters) - 1:
            # 次のフィルターの logical_operator を操作する
            next_f = filters[i+1]
            current_logic = next_f.get("logical_operator", "and")
            
            logic_key = f"{key_prefix}filter_logic_next_{i}"
            selected_logic = st.radio(
                f"↓ 次の条件との結合", 
                ["AND", "OR"], 
                index=0 if current_logic == "and" else 1, 
                key=logic_key,
                horizontal=True
            )
            next_f["logical_operator"] = selected_logic.lower()

def display_add_filter_form(selections: Dict, all_fields: List[Dict] = None, key_prefix: str = ""):
    with st.expander("＋ フィルターを追加する"):
        if all_fields is None: all_fields = get_all_available_fields(selections)
        field_options = {f['display_name_with_table']: f for f in all_fields}
        cols = st.columns(2)
        new_filter_field_display_name = cols[0].selectbox("列", field_options.keys(), index=None, key=f"{key_prefix}new_filter_field")
        operator_map = {"である": "=", "ではない": "!=", "より大きい": ">", "より小さい": "<", "以上": ">=", "以下": "<=", "範囲": "between", "空": "is-null", "空ではない": "not-null"}
        new_filter_op_name = cols[1].selectbox("条件", operator_map.keys(), index=None, key=f"{key_prefix}new_filter_op")
        new_filter_value1, new_filter_value2 = None, None
        if new_filter_op_name and operator_map[new_filter_op_name] not in ["is-null", "not-null"]:
            if operator_map[new_filter_op_name] == "between":
                val_cols = st.columns(2)
                new_filter_value1 = val_cols[0].text_input("開始値", key=f"{key_prefix}new_filter_value1")
                new_filter_value2 = val_cols[1].text_input("終了値", key=f"{key_prefix}new_filter_value2")
            else:
                new_filter_value1 = st.text_input("値", key=f"{key_prefix}new_filter_value1")
        
        # 既にフィルターがある場合、結合条件を選択させる
        new_filter_logic = "and"
        if selections["filters"]:
            logic_label = st.radio("前の条件との結合", ["AND (すべてに一致)", "OR (いずれかに一致)"], key=f"{key_prefix}new_filter_logic", horizontal=True)
            new_filter_logic = "and" if "AND" in logic_label else "or"

        if st.button("フィルターを追加", key=f"{key_prefix}add_filter_button"):
            if new_filter_field_display_name and new_filter_op_name:
                selected_field = field_options[new_filter_field_display_name]
                new_filter = {
                    "field_ref": selected_field['mbql_ref'], "field_name": selected_field['display_name_with_table'], 
                    "operator": operator_map[new_filter_op_name], "operator_name": new_filter_op_name, 
                    "value1": new_filter_value1, "value2": new_filter_value2,
                    "logical_operator": new_filter_logic
                }
                selections["filters"].append(new_filter)
                st.rerun()

def display_existing_joins(selections: Dict, key_prefix: str = ""):
    for i, join in enumerate(selections["joins"]):
        with st.container(border=True):
            cols = st.columns([0.9, 0.1])
            base_field = next((f for f in selections['available_fields'] if f['id'] == join['condition'][1][1]), None)
            target_table = next((t for t in st.session_state.tables_metadata if t['id'] == join['target_table_id']), None)
            if target_table:
                target_field = next((f for f in target_table['fields'] if f['id'] == join['condition'][2][1]), None)
                if base_field and target_field:
                    join_type_display = JOIN_STRATEGY_DISPLAY_MAP.get(join['strategy'], join['strategy'])
                    join_str = (f"**{selections['table_name']}** に **{join_type_display}** で **{join['target_table_name']}** を結合"
                                f"<br>条件: `{base_field['name']}` = `{target_field['name']}`")
                    cols[0].markdown(join_str, unsafe_allow_html=True)
            if cols[1].button("🗑️", key=f"{key_prefix}del_join_{i}", help="この結合を削除"):
                selections["joins"].pop(i); st.rerun()

def display_join_builder(selections: Dict, key_prefix: str = ""):
    with st.expander("＋ 結合を追加する"):
        joinable_tables = {tbl['display_name']: tbl for tbl in st.session_state.tables_metadata if tbl['id'] != selections.get('table_id')}
        target_table_name = st.selectbox("結合するテーブル", joinable_tables.keys(), index=None, key=f"{key_prefix}join_target_table", placeholder="テーブルを選択...")
        if target_table_name:
            target_table = joinable_tables[target_table_name]
            join_type_display_name = st.selectbox("結合方法", JOIN_STRATEGY_MAP.keys(), key=f"{key_prefix}join_strategy")
            st.write("結合条件:")
            cols = st.columns([5, 1, 5])
            base_fields = {f['display_name']: f['id'] for f in selections['available_fields']}
            base_field_name = cols[0].selectbox(f"{selections['table_name']} の列", base_fields.keys(), index=None, key=f"{key_prefix}join_base_field")
            cols[1].markdown("<p style='text-align: center; font-size: 24px; margin-top: 25px'>=</p>", unsafe_allow_html=True)
            target_fields = {f['display_name']: f['id'] for f in target_table['fields']}
            target_field_name = cols[2].selectbox(f"{target_table_name} の列", target_fields.keys(), index=None, key=f"{key_prefix}join_target_field")
            if st.button("結合を追加", key=f"{key_prefix}add_join_button"):
                if base_field_name and target_field_name and join_type_display_name:
                    join_count = len(selections.get("joins", []))
                    join_alias = f"_join_{join_count + 1}"
                    new_join = {"target_table_id": target_table['id'], "target_table_name": target_table_name,
                                "join_alias": join_alias, "strategy": JOIN_STRATEGY_MAP[join_type_display_name],
                                "condition": ["=", ["field", base_fields[base_field_name], None], ["field", target_fields[target_field_name], {"join-alias": join_alias}]]}
                    selections["joins"].append(new_join); st.rerun()

def display_aggregation_breakout_form(selections: Dict, all_fields: List[Dict] = None, show_breakout: bool = True, key_prefix: str = "", chart_type: str = None) -> Tuple[Optional[str], Optional[Any], Optional[Any], Optional[str]]:
    if all_fields is None: all_fields = get_all_available_fields(selections)
    cols = st.columns(2) if show_breakout else [st.container()]
    agg_container, breakout_container = cols[0], (cols[1] if show_breakout else None)
    
    # ラベルの決定
    is_axis_chart = chart_type in ["棒グラフ", "折れ線グラフ", "エリアグラフ"]
    agg_label = "Y軸 (集計値)" if is_axis_chart else "集約方法"
    breakout_label = "X軸 (グループ化)" if is_axis_chart else "グループ化する列"

    agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
    agg_type_name = agg_container.selectbox(agg_label, agg_map.keys(), key=f"{key_prefix}agg_type_name")
    agg_field_ref = None
    field_required_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
    if agg_map[agg_type_name] in field_required_aggs:
        numeric_fields = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields if any(t in f.get('base_type', '').lower() for t in ['integer', 'float', 'double', 'decimal']) and f.get('semantic_type') not in ['type/PK', 'type/FK']}
        agg_field_display_name = agg_container.selectbox("集計対象の列", numeric_fields.keys(), key=f"{key_prefix}agg_field_name", index=None)
        if agg_field_display_name: agg_field_ref = numeric_fields[agg_field_display_name]
    
    breakout_field_ref = None
    granularity = None
    
    if show_breakout and breakout_container:
        field_options = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
        breakout_field_display_name = breakout_container.selectbox(breakout_label, field_options.keys(), index=None, key=f"{key_prefix}breakout_field_name")
        breakout_field_ref = field_options.get(breakout_field_display_name)
        
        # 日付型のフィールドが選択された場合、粒度を選択できるようにする
        if breakout_field_display_name:
            selected_field_info = next((f for f in all_fields if f['display_name_with_table'] == breakout_field_display_name), None)
            if selected_field_info and any(t in selected_field_info.get('base_type', '').lower() for t in ['date', 'time', 'timestamp']):
                granularity_map = {
                    "年": "year",
                    "四半期": "quarter",
                    "月": "month",
                    "週": "week",
                    "日": "day",
                    "時間": "hour",
                    "分": "minute",
                    "デフォルト": None
                }
                granularity_name = breakout_container.selectbox("時間粒度", granularity_map.keys(), index=4, key=f"{key_prefix}granularity") # デフォルトは日
                granularity = granularity_map.get(granularity_name)

    return agg_type_name, agg_field_ref, breakout_field_ref, granularity

def display_scatter_plot_form(selections: Dict, all_fields: List[Dict] = None, key_prefix: str = "") -> Tuple[Optional[Dict], Optional[Any]]:
    st.info("散布図は、2つの指標（数値）の関係性を可視化します。オプションでカテゴリによる色分けも可能です。")
    if all_fields is None: all_fields = get_all_available_fields(selections)
    numeric_fields = {
        f['display_name_with_table']: f['mbql_ref'] 
        for f in all_fields 
        if any(t in f.get('base_type', '').lower() for t in ['integer', 'float', 'double', 'decimal']) 
        and f.get('semantic_type') not in ['type/PK', 'type/FK']
    }
    st.markdown("##### Y軸の指標")
    y_field_display_name = st.selectbox("Y軸の列", numeric_fields.keys(), key=f"{key_prefix}y_axis_field", index=None)
    st.markdown("##### X軸の指標")
    x_field_display_name = st.selectbox("X軸の列", numeric_fields.keys(), key=f"{key_prefix}x_axis_field", index=None)
    st.markdown("##### グループ化する列（オプション）")
    field_options = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
    breakout_field_display_name = st.selectbox("グループ化する列（点の色分け）", field_options.keys(), index=None, key=f"{key_prefix}scatter_breakout_field_name")
    y_axis_ref = numeric_fields.get(y_field_display_name)
    x_axis_ref = numeric_fields.get(x_field_display_name)
    breakout_field_ref = field_options.get(breakout_field_display_name)
    return {"y_axis": y_axis_ref, "x_axis": x_axis_ref}, breakout_field_ref

def display_pivot_table_form(selections: Dict, all_fields: List[Dict] = None, key_prefix: str = ""):
    st.info("ピボットテーブルは、データをクロス集計して表示します。行、列、集計したい値をそれぞれ指定してください。")
    if all_fields is None: all_fields = get_all_available_fields(selections)
    field_options = [f['display_name_with_table'] for f in all_fields]
    numeric_fields = [f['display_name_with_table'] for f in all_fields if any(t in f.get('base_type', '').lower() for t in ['integer', 'float', 'double', 'decimal']) and f.get('semantic_type') not in ['type/PK', 'type/FK']]
    
    selections['pivot_rows'] = st.multiselect("行", field_options, key=f"{key_prefix}pivot_rows_multiselect")
    selections['pivot_cols'] = st.multiselect("列", field_options, key=f"{key_prefix}pivot_cols_multiselect")
    
    selected_val = st.selectbox("値", numeric_fields, key=f"{key_prefix}pivot_vals_selectbox", index=None, placeholder="値を選択...")
    selections['pivot_vals'] = [selected_val] if selected_val else []

    pivot_agg_options = {
        "合計": "sum",
        "平均": "avg",
        "中央値": "median",
        "標準偏差": "stddev"
    }
    selections['pivot_agg_func_display'] = st.selectbox("集計方法", pivot_agg_options.keys(), key=f"{key_prefix}pivot_agg_selectbox")
    selections['pivot_agg_func'] = pivot_agg_options[selections['pivot_agg_func_display']]

def display_map_form(selections: Dict, all_fields: List[Dict], key_prefix: str = "") -> Optional[Dict]:
    st.info("地図は、地理的なデータを可視化します。ピン（緯度経度）またはリージョン（国、州など）を選択してください。")
    if all_fields is None: all_fields = get_all_available_fields(selections)
    map_type = st.radio("マップタイプ", ["ピンマップ (緯度・経度)", "リージョンマップ (地域)"], horizontal=True, key=f"{key_prefix}map_type_radio")
    
    # all_fields = get_all_available_fields(selections) # This line is redundant after the change
    field_options = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
    numeric_fields = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields if any(t in f.get('base_type', '').lower() for t in ['integer', 'float', 'double', 'decimal']) and f.get('semantic_type') not in ['type/PK', 'type/FK']}
    
    map_config = {}
    
    if "ピンマップ" in map_type:
        map_config["type"] = "pin"
        col1, col2 = st.columns(2)
        lat_field_name = col1.selectbox("緯度の列 (Latitude)", field_options.keys(), index=None, key=f"{key_prefix}map_lat_field")
        lon_field_name = col2.selectbox("経度の列 (Longitude)", field_options.keys(), index=None, key=f"{key_prefix}map_lon_field")
        
        if lat_field_name: map_config["latitude"] = field_options[lat_field_name]
        if lon_field_name: map_config["longitude"] = field_options[lon_field_name]
        
        # ピンマップでもメトリックを表示する場合がある（バブルの大きさなど）が、基本はLat/Lon
        # ここではオプションとして「指標」を追加できるようにする（将来拡張）
        
    else: # リージョンマップ
        map_config["type"] = "region"
        col1, col2 = st.columns(2)
        
        # リージョンマップの種類を選択 (World vs US)
        region_map_type = col1.radio("リージョンマップの種類", ["世界地図 (World)", "米国 (United States)"], index=0, key=f"{key_prefix}region_map_type")
        map_config["region_map_type"] = "world_countries" if "世界" in region_map_type else "us_states"
        
        region_field_name = col1.selectbox("地域の列 (都道府県/国)", field_options.keys(), index=None, key=f"{key_prefix}map_region_field")
        
        agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
        agg_type_name = col2.selectbox("集計方法", agg_map.keys(), key=f"{key_prefix}map_agg_type")
        
        if region_field_name: map_config["region"] = field_options[region_field_name]
        map_config["agg_type_name"] = agg_type_name
        map_config["agg_type"] = agg_map.get(agg_type_name)
        
        agg_field_ref = None
        field_required_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
        if map_config["agg_type"] in field_required_aggs:
             agg_field_display_name = col2.selectbox("集計対象の列", numeric_fields.keys(), key=f"{key_prefix}map_agg_field", index=None)
             if agg_field_display_name: 
                 agg_field_ref = numeric_fields[agg_field_display_name]
                 map_config["agg_field"] = agg_field_ref

    return map_config

@st.dialog("カスタムグラフ作成", width="large")
def display_custom_chart_form():
    selections = st.session_state.custom_builder_selections
    key_prefix = "custom_"
    chart_type_options = list(CHART_TYPE_MAP.keys())
    current_chart_display_name = selections.get('chart_display_name')
    current_chart_index = chart_type_options.index(current_chart_display_name) if current_chart_display_name in chart_type_options else None
    
    chart_display_name = st.selectbox(
        "グラフの種類を選択", 
        options=chart_type_options, 
        index=current_chart_index,
        key=f"{key_prefix}chart_type_selection"
    )
    
    # 選択されたグラフの種類を保存
    if chart_display_name:
        st.session_state.custom_builder_selections['chart_display_name'] = chart_display_name

    if selections.get("chart_display_name"):
        # 選択済みテーブル情報を表示
        st.info(f"使用中のテーブル: **{st.session_state.custom_builder_selections.get('table_name')}**")

        if selections.get("table_id"):
            # 共通のフィールドリストを取得（キャッシュ的に利用）
            all_fields = get_all_available_fields(selections)

            # st.markdown("---"); st.markdown("**テーブル結合**"); display_existing_joins(selections, key_prefix=key_prefix); display_join_builder(selections, key_prefix=key_prefix)
            st.markdown("---"); st.markdown("**フィルター**"); display_existing_filters(selections, key_prefix=key_prefix); display_add_filter_form(selections, all_fields=all_fields, key_prefix=key_prefix)
            st.markdown("---"); st.markdown("**データ定義**")
            scatter_axes, breakout_field_ref, agg_type_name, agg_field_ref = None, None, None, None
            map_config = None
            granularity = None
            
            if chart_display_name == "散布図":
                scatter_axes, breakout_field_ref = display_scatter_plot_form(selections, all_fields=all_fields, key_prefix=key_prefix)
            elif chart_display_name == "ピボットテーブル":
                display_pivot_table_form(selections, all_fields=all_fields, key_prefix=key_prefix)
            elif chart_display_name == "地図":
                map_config = display_map_form(selections, all_fields=all_fields, key_prefix=key_prefix)
            else:
                charts_without_breakout = ["数値", "ゲージ", "scalar"]
                show_breakout = chart_display_name not in charts_without_breakout
                agg_type_name, agg_field_ref, breakout_field_ref, granularity = display_aggregation_breakout_form(selections, all_fields=all_fields, show_breakout=show_breakout, key_prefix=key_prefix, chart_type=chart_display_name)
                
                if chart_display_name == "ゲージ":
                    st.markdown("##### ゲージ設定")
                    
                    # Metabase Default Colors
                    METABASE_COLORS_MAP = {
                        "Blue": "#509EE3",
                        "Green": "#9CC177",
                        "Purple": "#A989C5",
                        "Red": "#EF8C8C",
                        "Yellow": "#F9D45C",
                        "Orange": "#F2A86F",
                        "Teal": "#98D9D9",
                        "Indigo": "#7172AD",
                    }
                    METABASE_COLOR_NAMES = list(METABASE_COLORS_MAP.keys())

                    # データに基づく最小値・最大値の取得
                    data_min = 0.0
                    data_max = 100.0 # デフォルト
                    
                    if selections.get('table_id'):
                        # 集計対象のフィールドを取得
                        target_field_id = None
                        if agg_field_ref:
                             # agg_field_ref is ["field", id, options]
                             target_field_id = agg_field_ref[1]
                        
                        # クエリ構築
                        stats_query = {
                            "type": "query",
                            "database": next((t['db_id'] for t in st.session_state.tables_metadata if t['id'] == selections['table_id']), None),
                            "query": {
                                "source-table": selections['table_id'],
                                "aggregation": []
                            }
                        }
                        
                        if agg_type_name == "行のカウント":
                            # Countの場合: Min=0, Max=Count(*)
                            stats_query["query"]["aggregation"] = [["count"]]
                            # Countの結果がMaxになる
                        elif target_field_id:
                            # フィールド集計の場合: Min=min(field), Max=max(field)
                            # ただし、Sumの場合は合計値がMaxになるべきだが、ここでは「データの範囲」として
                            # カラム自体のMin/Maxを取得する（ユーザーの要望「集計対象データの最小値、最大値」）
                            # もし「売上合計」のゲージなら、0〜売上合計 が適切かもしれないが、
                            # ここではカラムの統計値を取得する。
                            stats_query["query"]["aggregation"] = [["min", agg_field_ref], ["max", agg_field_ref]]
                        
                        # 実行 (キャッシュなどを考慮しつつ)
                        # ここでは簡易的に毎回実行（軽量なはず）
                        try:
                            with st.spinner("データの統計情報を取得中..."):
                                stats_result = execute_query(st.session_state.metabase_session_id, stats_query)
                                if stats_result and stats_result.get('status') == 'completed' and stats_result['data']['rows']:
                                    row = stats_result['data']['rows'][0]
                                    if agg_type_name == "行のカウント":
                                        data_min = 0.0
                                        data_max = float(row[0])
                                    else:
                                        # Min/Max
                                        if len(row) >= 2:
                                            data_min = float(row[0]) if row[0] is not None else 0.0
                                            data_max = float(row[1]) if row[1] is not None else 100.0
                        except Exception as e:
                            # エラー時はデフォルト
                            pass

                    # マージンを持たせる
                    if data_max == data_min: data_max += 100
                    
                    use_segments = st.checkbox("範囲（カラーゾーン）を設定する", value=selections.get('use_segments', False), key=f"{key_prefix}use_segments")
                    selections['use_segments'] = use_segments
                    
                    if use_segments:
                        num_segments = st.number_input("範囲の数", min_value=1, max_value=8, value=selections.get('num_segments', 1), key=f"{key_prefix}num_segments")
                        selections['num_segments'] = num_segments
                        segments = []
                        
                        st.caption(f"データの範囲: {data_min} 〜 {data_max}")

                        # コールバック関数の定義 (クロージャとして定義)
                        def sync_slider_to_inputs(k_slider, k_min, k_max):
                            val = st.session_state[k_slider]
                            st.session_state[k_min] = val[0]
                            st.session_state[k_max] = val[1]

                        def sync_inputs_to_slider(k_slider, k_min, k_max):
                            # 入力値が逆転しないように制御
                            mn = st.session_state[k_min]
                            mx = st.session_state[k_max]
                            if mn > mx: mn = mx 
                            st.session_state[k_slider] = (mn, mx)

                        def set_prev_max(i, key_prefix):
                            prev_max_key = f"{key_prefix}in_max_{i-1}"
                            curr_min_key = f"{key_prefix}in_min_{i}"
                            curr_max_key = f"{key_prefix}in_max_{i}"
                            slider_key = f"{key_prefix}seg_slider_{i}"
                            
                            if prev_max_key in st.session_state:
                                val = st.session_state[prev_max_key]
                                st.session_state[curr_min_key] = val
                                # MaxがMinより小さい場合はMaxも更新
                                if st.session_state[curr_max_key] < val:
                                     st.session_state[curr_max_key] = val
                                st.session_state[slider_key] = (st.session_state[curr_min_key], st.session_state[curr_max_key])

                        for i in range(num_segments):
                            st.markdown(f"**範囲 {i+1}**")
                            c1, c2 = st.columns([3, 1])
                            
                            # 色選択 (Visual)
                            with c2:
                                s_color_name = st.selectbox(f"色", METABASE_COLOR_NAMES, index=i % len(METABASE_COLOR_NAMES), key=f"{key_prefix}seg_color_name_{i}", label_visibility="collapsed")
                                s_color_hex = METABASE_COLORS_MAP[s_color_name]
                                st.color_picker("色プレビュー", value=s_color_hex, key=f"{key_prefix}seg_color_disp_{i}", disabled=True, label_visibility="collapsed")

                            with c1:
                                s_label = st.text_input(f"ラベル", value=f"範囲 {i+1}", key=f"{key_prefix}seg_label_{i}")
                                
                                # Session State Keys
                                k_min = f"{key_prefix}in_min_{i}"
                                k_max = f"{key_prefix}in_max_{i}"
                                k_slider = f"{key_prefix}seg_slider_{i}"
                                
                                # 初期値設定
                                if k_min not in st.session_state: st.session_state[k_min] = data_min
                                if k_max not in st.session_state: st.session_state[k_max] = data_max
                                if k_slider not in st.session_state: st.session_state[k_slider] = (data_min, data_max)

                                # 入力欄
                                sc1, sc2, sc3 = st.columns([2, 2, 1])
                                val_min = sc1.number_input("最小", key=k_min, on_change=sync_inputs_to_slider, args=(k_slider, k_min, k_max))
                                val_max = sc2.number_input("最大", key=k_max, on_change=sync_inputs_to_slider, args=(k_slider, k_min, k_max))
                                
                                # 自動入力ボタン (2つ目以降)
                                if i > 0:
                                    sc3.button("直前の最大値を適用", key=f"{key_prefix}btn_auto_{i}", on_click=set_prev_max, args=(i, key_prefix))
                                
                                # スライダー
                                # 範囲はデータのMin/Maxより少し広く取る
                                slider_bound_min = min(data_min, val_min)
                                slider_bound_max = max(data_max, val_max)
                                if slider_bound_max == slider_bound_min: slider_bound_max += 100 # マージン

                                s_range = st.slider(
                                    "範囲",
                                    min_value=float(slider_bound_min),
                                    max_value=float(slider_bound_max),
                                    key=k_slider,
                                    on_change=sync_slider_to_inputs,
                                    args=(k_slider, k_min, k_max)
                                )
                                s_min, s_max = s_range
                            
                            segments.append({"min": s_min, "max": s_max, "label": s_label, "color": s_color_hex})
                        selections['gauge_segments'] = segments
            st.markdown("---")
            st.selectbox('カードサイズを選択', list(SIZE_MAPPING.keys()), key=f'{key_prefix}card_size_selection')
            col1, col2 = st.columns(2)
            if col1.button("プレビュー", key=f"{key_prefix}preview_button"):
                table_id = selections['table_id']
                selected_table = next((tbl for tbl in st.session_state.tables_metadata if tbl['id'] == table_id), None)
                all_fields = get_all_available_fields(selections)
                query = {"source-table": table_id}
                if selections["joins"]: query["joins"] = [{ "alias": join["join_alias"], "source-table": join["target_table_id"], "condition": join["condition"], "strategy": join["strategy"], "fields": "all" } for join in selections["joins"]]
                if selections["filters"]:
                    filter_clauses = []
                    for f in selections["filters"]:
                        op, field_clause = f["operator"], f["field_ref"]
                        if op in ["is-null", "not-null"]: clause = [op, field_clause]
                        elif op == "between":
                            try: v1, v2 = float(f["value1"]), float(f["value2"])
                            except (ValueError, TypeError): st.error(f"フィルター「{f['field_name']}」の範囲指定の値が無効です。"); return
                            clause = [op, field_clause, v1, v2]
                        else:
                            try: value = float(f["value1"])
                            except (ValueError, TypeError): value = f["value1"]
                            clause = [op, field_clause, value]
                        filter_clauses.append(clause)
                    
                    if filter_clauses:
                        # 逐次的にクエリを構築 (Left-to-Right)
                        # 初期値は最初のフィルター
                        current_filter_query = filter_clauses[0]
                        
                        # 2つ目以降を順次結合
                        for i in range(1, len(filter_clauses)):
                            # 対応するフィルター定義から結合条件を取得
                            # filter_clauses[i] は selections["filters"][i] に対応
                            logic = selections["filters"][i].get("logical_operator", "and")
                            next_clause = filter_clauses[i]
                            
                            current_filter_query = [logic, current_filter_query, next_clause]
                        
                        query["filter"] = current_filter_query
                
                agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
                agg_type = agg_map.get(agg_type_name) if agg_type_name else None
                preview_extras = {}
                
                if chart_display_name == "散布図":
                    x_ref, y_ref = scatter_axes["x_axis"], scatter_axes["y_axis"]
                    if not x_ref or not y_ref: st.error("散布図にはX軸とY軸の両方を選択してください。"); return
                    query["fields"] = [x_ref, y_ref]
                    if breakout_field_ref: query["fields"].append(breakout_field_ref)
                
                elif chart_display_name == "ウォーターフォール":
                    # フォームは既に上で表示されているため、変数をそのまま使用
                    if not agg_type_name or not breakout_field_ref:
                        st.warning("集計方法とグループ化する列を選択してください。")
                        return
                    
                    agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
                    agg_type = agg_map.get(agg_type_name)
                    
                    aggregation = [agg_type, agg_field_ref] if agg_field_ref else [agg_type]
                    query["aggregation"] = [aggregation]
                    
                    # 粒度がある場合は適用
                    final_breakout = breakout_field_ref
                    if granularity:
                        # breakout_field_ref は ["field", id, options] または ["field", id, None]
                        # optionsにtemporal-unitを追加する
                        field_id = breakout_field_ref[1]
                        options = breakout_field_ref[2] if len(breakout_field_ref) > 2 else None
                        if options is None: options = {}
                        options["temporal-unit"] = granularity
                        final_breakout = ["field", field_id, options]

                    query["breakout"] = [final_breakout]
                    
                    selections["aggregation"] = [aggregation]
                    selections["breakout_id"] = breakout_field_ref[1]

                elif chart_display_name == "テーブル":
                    st.info("テーブルビューは、データを表形式で表示します。表示する列を選択してください（指定しない場合は全ての列が表示されます）。")
                    field_options = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
                    selected_columns = st.multiselect("表示する列", field_options.keys(), key=f"{key_prefix}table_columns")
                    
                    if selected_columns:
                        query["fields"] = [field_options[col] for col in selected_columns]
                        # 選択された列を保存（プレビューなどで使用）
                        selections["table_columns"] = selected_columns

                elif chart_display_name == "ピボットテーブル":
                    pivot_rows_names = selections.get('pivot_rows', [])
                    pivot_cols_names = selections.get('pivot_cols', [])
                    pivot_vals_names = selections.get('pivot_vals', [])
                    if not pivot_rows_names or not pivot_vals_names:
                        st.error("ピボットテーブルには少なくとも「行」と「値」が必要です。"); return
                    field_name_map = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
                    row_refs = [field_name_map[name] for name in pivot_rows_names]
                    col_refs = [field_name_map[name] for name in pivot_cols_names]
                    val_refs = [field_name_map[name] for name in pivot_vals_names]
                    mbql_agg_func = selections.get('pivot_agg_func', 'sum')
                    query["breakout"] = row_refs + col_refs
                    query["aggregation"] = [[mbql_agg_func, ref] for ref in val_refs]
                    preview_extras['pivot_agg_func'] = mbql_agg_func
                elif chart_display_name == "地図":
                    if map_config["type"] == "pin":
                        if not map_config.get("latitude") or not map_config.get("longitude"):
                            st.error("ピンマップを作成するには、緯度と経度の列を選択してください。"); return
                        # ピンマップは通常、RawデータまたはLat/LonでのGroup By
                        # ここではシンプルにLat/Lon列を選択して表示する（Unaggregated）
                        query["fields"] = [map_config["latitude"], map_config["longitude"]]
                    else: # region
                        if not map_config.get("region"):
                            st.error("リージョンマップを作成するには、地域の列を選択してください。"); return
                        query["breakout"] = [map_config["region"]]
                        agg_t = map_config.get("agg_type")
                        agg_f = map_config.get("agg_field")
                        if agg_t:
                            if agg_f: query["aggregation"] = [[agg_t, agg_f]]
                            else: query["aggregation"] = [[agg_t]]
                        else:
                             st.error("集計方法を選択してください。"); return
                else:
                    if agg_type:
                        field_req_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
                        if agg_type in field_req_aggs: 
                            if not agg_field_ref: st.error("この集約方法には集計対象の列が必要です。"); return
                            query["aggregation"] = [[agg_type, agg_field_ref]]
                        else: query["aggregation"] = [[agg_type]]
                
                if breakout_field_ref and chart_display_name not in ["散布図", "ピボットテーブル", "地図", "ウォーターフォール"]: 
                    # 粒度がある場合は適用
                    final_breakout = breakout_field_ref
                    if granularity:
                        # breakout_field_ref は ["field", id, options] または ["field", id, None]
                        # optionsにtemporal-unitを追加する
                        field_id = breakout_field_ref[1]
                        options = breakout_field_ref[2] if len(breakout_field_ref) > 2 else None
                        if options is None: options = {}
                        options["temporal-unit"] = granularity
                        final_breakout = ["field", field_id, options]
                    
                    
                    query["breakout"] = [final_breakout]

                if chart_display_name == "ファンネル":
                    # ファンネルチャートは降順でソートする
                    query["order-by"] = [["desc", ["aggregation", 0]]]

                if chart_display_name == "ファンネル":
                    # ファンネルチャートは降順でソートする
                    query["order-by"] = [["desc", ["aggregation", 0]]]

                dataset_query = {"type": "query", "database": selected_table['db_id'], "query": query}
                with st.spinner("プレビューデータを取得中..."):
                    result = execute_query(st.session_state.metabase_session_id, dataset_query)
                if result and result.get('status') == 'completed':
                    result_cols = result['data']['cols']
                    display_names = [c['display_name'] for c in result_cols]
                    internal_names = [c['name'] for c in result_cols]
                    unique_display_names = _deduplicate_columns(display_names)
                    # Altair/Streamlit fails if column names contain colons (interpreted as type encoding)
                    # Sanitize column names by replacing colons with underscores
                    unique_display_names = [col.replace(':', '_') for col in unique_display_names]
                    df = pd.DataFrame(result['data']['rows'], columns=unique_display_names)
                    
                    if chart_display_name == "ピボットテーブル":
                        num_rows = len(selections.get('pivot_rows', []))
                        num_cols = len(selections.get('pivot_cols', []))
                        preview_extras['pivot_row_names'] = list(df.columns[:num_rows])
                        preview_extras['pivot_col_names'] = list(df.columns[num_rows : num_rows + num_cols])
                        preview_extras['pivot_val_names'] = list(df.columns[num_rows + num_cols :])
                    
                    viz_settings = {}
                    card_name = ""
                    
                    if chart_display_name == "散布図":
                        x_field = next((f for f in all_fields if f['mbql_ref'] == scatter_axes["x_axis"]), None)
                        y_field = next((f for f in all_fields if f['mbql_ref'] == scatter_axes["y_axis"]), None)
                        breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), None) if breakout_field_ref else None
                        
                        if len(internal_names) >= 2:
                            if breakout_field and len(internal_names) >= 3:
                                # X軸と色分け(Breakout)をdimensionに、Y軸をmetricに設定
                                viz_settings = {"graph.dimensions": [internal_names[0], internal_names[2]], "graph.metrics": [internal_names[1]]}
                            else:
                                viz_settings = {"graph.dimensions": [internal_names[0]], "graph.metrics": [internal_names[1]]}

                        if x_field and y_field:
                            x_name, y_name = x_field['display_name'], y_field['display_name']
                            breakout_name = f" ({breakout_field['display_name']}別)" if breakout_field else ""
                            card_name = f"散布図: {y_name} vs {x_name}{breakout_name}"
                    elif chart_display_name == "ピボットテーブル":
                        rows_str = ", ".join(selections.get('pivot_rows', []))
                        vals_str = ", ".join(selections.get('pivot_vals', []))
                        agg_str = selections.get('pivot_agg_func_display', '合計')
                        card_name = f"ピボット: {rows_str} 別 {vals_str}の{agg_str}"
                        display_to_internal_map = {unique_name: internal_name for unique_name, internal_name in zip(unique_display_names, internal_names)}
                        viz_settings = {
                            "pivot_table": {
                                "columns": [display_to_internal_map.get(name) for name in preview_extras.get('pivot_col_names', [])],
                                "rows": [display_to_internal_map.get(name) for name in preview_extras.get('pivot_row_names', [])],
                                "values": [display_to_internal_map.get(name) for name in preview_extras.get('pivot_val_names', [])]
                            }
                        }
                    elif chart_display_name == "地図":
                        if map_config["type"] == "pin":
                            card_name = "ピンマップ"
                            viz_settings["map.type"] = "pin"
                            # Metabaseの仕様に合わせてLat/Lonカラムを指定
                            # 実際のカラム名を特定する必要がある
                            # internal_namesから推測するか、mbql_refからIDを取得して...
                            # ここではシンプルに、Metabaseが自動検出することを期待しつつ、
                            # 明示的に指定できるなら指定する。
                            # API的には `map.latitude_column` と `map.longitude_column` にフィールド参照(name or id)を入れる
                            
                            # map_config["latitude"] は ["field", id, ...] の形式
                            lat_field_id = map_config["latitude"][1]
                            lon_field_id = map_config["longitude"][1]
                            
                            # カラム名を取得
                            lat_field_info = next((f for f in all_fields if f['id'] == lat_field_id), None)
                            lon_field_info = next((f for f in all_fields if f['id'] == lon_field_id), None)
                            
                            if lat_field_info and lon_field_info:
                                viz_settings["map.latitude_column"] = lat_field_info['name']
                                viz_settings["map.longitude_column"] = lon_field_info['name']
                                card_name = f"ピンマップ: {lat_field_info['display_name']} / {lon_field_info['display_name']}"

                        else: # region
                            card_name = f"リージョンマップ: {map_config['agg_type_name']}"
                            viz_settings["map.type"] = "region"
                            viz_settings["map.region"] = map_config.get("region_map_type", "world_countries")
                            # region_columnを指定
                            region_field_id = map_config["region"][1]
                            region_field_info = next((f for f in all_fields if f['id'] == region_field_id), None)
                            if region_field_info:
                                viz_settings["map.region_column"] = region_field_info['name']
                                viz_settings["map.region_column"] = region_field_info['name']
                                card_name += f" ({region_field_info['display_name']})"

                    elif chart_display_name == "ゲージ":
                        agg_field = next((f for f in all_fields if f['mbql_ref'] == agg_field_ref), None) if agg_field_ref else None
                        agg_str = f"の{agg_field['display_name_with_table']}" if agg_field else ""
                        card_name = f"ゲージ: {agg_type_name}{agg_str}"
                        
                        card_name = f"ゲージ: {agg_type_name}{agg_str}"
                        
                        # ゲージの最小・最大はセグメント全体から自動算出、またはデフォルト
                        # ここでは明示的な gauge.min/max は設定せず、セグメントに任せるか、
                        # セグメントがある場合はその最小・最大を適用する
                        if selections.get('use_segments'):
                            segs = selections.get('gauge_segments', [])
                            viz_settings["gauge.segments"] = segs
                            if segs:
                                all_mins = [s['min'] for s in segs]
                                all_maxs = [s['max'] for s in segs]
                                viz_settings["gauge.min"] = min(all_mins) if all_mins else 0
                                viz_settings["gauge.max"] = max(all_maxs) if all_maxs else 100

                    else:
                        agg_field = next((f for f in all_fields if f['mbql_ref'] == agg_field_ref), None) if agg_field_ref else None
                        agg_str = f"の{agg_field['display_name_with_table']}" if agg_field else ""
                        if breakout_field_ref:
                            breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), None)
                            card_name = f"{chart_display_name}: {breakout_field['display_name_with_table']}別 {agg_type_name}{agg_str}"
                        else:
                            card_name = f"{chart_display_name}: {agg_type_name}{agg_str}"
                    
                    if selections["filters"]:
                        filter_summary = ""
                        for i, f in enumerate(selections["filters"]):
                            val_str = f"{f['value1']}" + (f" ~ {f['value2']}" if f.get('value2') else "")
                            op_name = f['operator_name']
                            current_str = f"{f['field_name']} {op_name} {val_str}"
                            
                            if i == 0:
                                filter_summary += current_str
                            else:
                                logic = f.get("logical_operator", "and").upper()
                                filter_summary += f" {logic} {current_str}"
                        
                        card_name += f" ({filter_summary})"

                    final_payload = {
                        "name": card_name,
                        "display": CHART_TYPE_MAP.get(chart_display_name),
                        "dataset_query": dataset_query,
                        "visualization_settings": viz_settings
                    }
                    st.session_state.preview_data = {'df': df, 'chart_type': CHART_TYPE_MAP.get(chart_display_name), 'final_payload': final_payload, **preview_extras}
                    st.rerun()
                else:
                    st.error("プレビューデータの取得に失敗しました。")
            if col2.button("閉じる", key=f"{key_prefix}close_builder_main"):
                st.session_state.show_builder_dialog = False
                st.session_state.custom_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": [], 'chart_display_name': None}
                st.session_state.preview_data = None
                st.rerun()
    if st.session_state.preview_data:
        st.divider()
        st.subheader("プレビュー")
        preview_data = st.session_state.preview_data
        df = preview_data['df']
        chart_type = preview_data['chart_type']
        if not df.empty:
            if len(df.columns) < 1:
                    st.warning("プレビュー対象の列がありません。")
            else:
                try:
                    if chart_type in ["bar", "line", "area"]:
                        if len(df.columns) < 2:
                            st.warning("グラフを描画するには少なくとも2つの列が必要です。")
                            st.dataframe(df)
                        else:
                            x_col = df.columns[0]
                            y_cols = list(df.columns[1:])
                            if chart_type == "bar": st.bar_chart(df, x=x_col, y=y_cols)
                            elif chart_type == "line": st.line_chart(df, x=x_col, y=y_cols)
                            elif chart_type == "area": st.area_chart(df, x=x_col, y=y_cols)
                    elif chart_type == "pie":
                        if len(df.columns) == 2:
                            fig = px.pie(df, names=df.columns[0], values=df.columns[1], title="円グラフプレビュー")
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("円グラフには、ラベルと値の2つの列が必要です。")
                            st.dataframe(df)
                    elif chart_type == "scatter":
                        if len(df.columns) < 2:
                            st.warning("散布図を描画するには少なくとも2つの列が必要です。")
                            st.dataframe(df)
                        else:
                            x_col = df.columns[0]
                            y_col = df.columns[1]
                            st.scatter_chart(df, x=x_col, y=y_col)
                            st.scatter_chart(df, x=x_col, y=y_col)
                    elif chart_type == "waterfall":
                        if len(df.columns) >= 2:
                            try:
                                x_col = df.columns[0]
                                y_col = df.columns[1]
                                fig = go.Figure(go.Waterfall(
                                    name = "20", orientation = "v",
                                    measure = ["relative"] * len(df),
                                    x = df[x_col],
                                    textposition = "outside",
                                    text = df[y_col],
                                    y = df[y_col],
                                    connector = {"line":{"color":"rgb(63, 63, 63)"}},
                                ))
                                fig.update_layout(title=f"Waterfall: {x_col} vs {y_col}", showlegend=True)
                                st.plotly_chart(fig, use_container_width=True)
                            except Exception as e:
                                st.error(f"プレビュー生成エラー: {e}")
                                st.dataframe(df)
                        else:
                            st.warning("ウォーターフォールチャートには少なくとも2つの列が必要です。")
                            st.dataframe(df)
                    elif chart_type == "table":
                        st.dataframe(df)
                    elif chart_type == "pivot-table":
                        st.info("ピボットテーブルプレビュー")
                        try:
                            pivoted_df = pd.pivot_table(
                                df,
                                index=preview_data.get('pivot_row_names', []),
                                columns=preview_data.get('pivot_col_names', []),
                                values=preview_data.get('pivot_val_names', [])
                            )
                            st.dataframe(pivoted_df)
                        except Exception as e:
                            st.error(f"ピボットテーブルの作成に失敗しました: {e}")
                            st.write("変換前のデータ:")
                            st.dataframe(df)
                    elif chart_type == "funnel":
                        if len(df.columns) < 2:
                            st.warning("ファンネルチャートには少なくとも2つの列（ステップと値）が必要です。")
                            st.dataframe(df)
                        else:
                            st.info("ファンネルチャートプレビュー")
                            fig = px.funnel(df, x=df.columns[1], y=df.columns[0])
                            st.plotly_chart(fig, use_container_width=True)
                    elif chart_type == "map":
                        st.info("地図プレビュー (データテーブル)")
                        st.dataframe(df)
                    else:
                        st.info("このグラフ種別のプレビューは現在サポートされていません。データテーブルを表示します。")
                        st.dataframe(df)
                except Exception as e:
                    st.error(f"グラフの描画に失敗しました: {e}")
                    st.dataframe(df)
        else:
            st.info("クエリは成功しましたが、結果は0件でした。")
        st.markdown("---")
        if st.button("作成してダッシュボードに追加", type="primary", key=f"{key_prefix}add_to_dashboard_button"):
            add_log_entry("click_create_view", {
                "dashboard_id": st.session_state.dashboard_id,
                "chart_type": selections.get('chart_display_name'),
                "table_name": selections.get('table_name')
            })
            handle_custom_chart_submission(st.session_state.preview_data['final_payload'], size_key=f"{key_prefix}card_size_selection")

def display_credentials_form():
    st.header("Metabase 認証情報")
    with st.form("credentials_form"):
        username, password = st.text_input("Username"), st.text_input("Password", type="password")
        dashboard_id, secret_key = st.text_input("Dashboard ID"), st.text_input("Secret Key", type="password")
        use_recommendation = st.checkbox("推薦機能を使用する", value=True)
        if st.form_submit_button("接続"):
            session_id = get_metabase_session(username, password)
            if session_id:
                st.session_state.update(metabase_session_id=session_id, dashboard_id=dashboard_id, secret_key=secret_key, use_recommendation=use_recommendation, username=username)
                add_log_entry("login", {"dashboard_id": dashboard_id})
                st.rerun()

def display_table_selection_form():
    st.header("テーブル選択")
    st.write("分析に使用するテーブルを選択してください。")
    
    if st.session_state.tables_metadata:
        table_options = {tbl['display_name']: tbl for tbl in st.session_state.tables_metadata}
        
        # 現在の選択状態を反映
        current_selection = st.session_state.custom_builder_selections.get('table_name')
        index = list(table_options.keys()).index(current_selection) if current_selection in table_options else 0
        
        selected_table_name = st.selectbox(
            "テーブル", 
            table_options.keys(), 
            index=index,
            key="global_table_selection"
        )
        
        if st.button("決定", type="primary"):
            selected_table = table_options[selected_table_name]
            # 選択されたテーブル情報をセッションステートに保存
            st.session_state.custom_builder_selections.update({
                "table_id": selected_table['id'], 
                "table_name": selected_table_name,
                "available_fields": selected_table.get('fields', []),
                "joins": [], "filters": [], "aggregation": [], "breakout_id": None
            })
            st.session_state.table_selected = True
            
            # ログ記録
            add_log_entry("select_table", {
                "table_name": selected_table_name,
                "table_id": selected_table['id']
            })
            
            st.rerun()
    else:
        st.error("テーブル情報が取得できませんでした。")

def embed_dashboard():
    secret_key, dashboard_id = st.session_state.secret_key, normalize_id(st.session_state.dashboard_id)
    if not secret_key or not dashboard_id: return
    
    # Secret Keyの空白除去
    secret_key = secret_key.strip()
    
    try:
        payload = {"resource": {"dashboard": int(dashboard_id)}, "params": {}, "exp": round(time.time()) + (60 * 10)}
        token = jwt.encode(payload, secret_key, algorithm="HS256")
        
        # PyJWTのバージョンによってbytesが返る場合があるため、文字列に変換
        if isinstance(token, bytes):
            token = token.decode("utf-8")
            
        iframe_url = f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"
        st.components.v1.iframe(iframe_url, height=800, scrolling=True)
    except ValueError:
        st.error(f"ダッシュボードID '{dashboard_id}' は有効な数値ではありません。")
    except Exception as e:
        st.error(f"埋め込みURLの生成に失敗しました: {e}")

def main():
    st.set_page_config(layout="wide"); st.title("ダッシュボードビュー推薦システム (RotatE版)")
    # ダイアログの「☓」ボタンを非表示にするCSS（グローバルに適用）
    st.markdown("""
        <style>
        div[data-testid="stDialog"] button[aria-label="Close"] {
            display: none !important;
        }
        div[data-testid="stModal"] button[aria-label="Close"] {
            display: none !important;
        }
        </style>
    """, unsafe_allow_html=True)
    if 'metabase_session_id' not in st.session_state: st.session_state.metabase_session_id = None
    if 'dashboard_id' not in st.session_state: st.session_state.dashboard_id = ""
    if 'secret_key' not in st.session_state: st.session_state.secret_key = ""
    if 'kge_model' not in st.session_state:
        st.session_state.kge_model, st.session_state.training_factory, st.session_state.relation_df = load_kge_model_and_data()
    if 'show_builder_dialog' not in st.session_state: st.session_state.show_builder_dialog = False
    if 'tables_metadata' not in st.session_state: st.session_state.tables_metadata = None
    if 'custom_builder_selections' not in st.session_state:
        st.session_state.custom_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": [], 'chart_display_name': None}
    if 'preview_data' not in st.session_state: st.session_state.preview_data = None
    if 'recommendations' not in st.session_state: st.session_state.recommendations = None
    if 'task_start_time' not in st.session_state: st.session_state.task_start_time = None
    if 'pending_recommendation' not in st.session_state: st.session_state.pending_recommendation = None
    if 'table_selected' not in st.session_state: st.session_state.table_selected = False
    if 'use_recommendation' not in st.session_state: st.session_state.use_recommendation = True

    if st.session_state.metabase_session_id is None: 
        display_credentials_form()
    else:
        # --- メタデータ読み込み (修正版) ---
        if st.session_state.tables_metadata is None:
            with st.spinner(f"分析用データベースのテーブル情報を読み込み中..."):
                db_id, tables = get_all_tables_metadata(st.session_state.metabase_session_id)
                
                if tables:
                     st.session_state.tables_metadata = tables
                     # 取得したテーブル情報を全列挙 (デバッグ用)
                     st.success(f"DB ID: {db_id} から {len(tables)} 個のテーブルを取得しました。")
                else:
                     st.warning("分析用データベース(Sample Database以外)が見つからないか、テーブルが存在しません。")

        # サイドバーでテーブル選択を行う
        with st.sidebar:
            st.header("設定")
            
            # テーブル選択フォーム (サイドバー内に移動)
            if st.session_state.tables_metadata:
                table_options = {tbl['display_name']: tbl for tbl in st.session_state.tables_metadata}
                current_selection = st.session_state.custom_builder_selections.get('table_name')
                index = list(table_options.keys()).index(current_selection) if current_selection in table_options else 0
                
                selected_table_name = st.selectbox(
                    "分析対象テーブル", 
                    table_options.keys(), 
                    index=index,
                    key="sidebar_table_selection"
                )
                
                if selected_table_name != current_selection:
                    selected_table = table_options[selected_table_name]
                    st.session_state.custom_builder_selections.update({
                        "table_id": selected_table['id'], 
                        "table_name": selected_table_name,
                        "available_fields": selected_table.get('fields', []),
                        "joins": [], "filters": [], "aggregation": [], "breakout_id": None
                    })
                    st.session_state.table_selected = True
                    add_log_entry("select_table", {"table_name": selected_table_name, "table_id": selected_table['id']})
                    st.rerun()
            else:
                st.error("テーブル情報がありません")

            st.divider()
            st.header("Debug Info")
            if st.button("ログアウト"):
                add_log_entry("logout", {})
                st.session_state.metabase_session_id = None
                st.session_state.table_selected = False
                st.rerun()
            if st.checkbox("取得済みテーブル一覧を表示"):
                if st.session_state.tables_metadata:
                    st.json([{"id": t['id'], "name": t['name'], "display_name": t['display_name']} for t in st.session_state.tables_metadata])
                else:
                    st.write("テーブルデータなし")

        if not st.session_state.table_selected and st.session_state.tables_metadata:
             # 初回ロード時などでテーブル未選択の場合、デフォルトで先頭を選択済みにする処理を入れるか、
             # あるいはサイドバーで選択を促すメッセージを出す。
             # ここではサイドバーのselectboxがデフォルトで先頭を選ぶため、
             # 明示的にセットされていない場合は先頭をセットする処理を走らせる。
             first_table = st.session_state.tables_metadata[0]
             st.session_state.custom_builder_selections.update({
                "table_id": first_table['id'], 
                "table_name": first_table['display_name'],
                "available_fields": first_table.get('fields', []),
                "joins": [], "filters": [], "aggregation": [], "breakout_id": None
            })
             st.session_state.table_selected = True
             st.rerun()

        embed_dashboard()
        
        with st.container(border=True):
            st.header("ビュー推薦")
            st.markdown("---")
            dashboard_id = normalize_id(st.session_state.dashboard_id)
            if dashboard_id:
                dashboard_details = get_dashboard_details(st.session_state.metabase_session_id, dashboard_id)
                if dashboard_details:
                    top_level_dashcards = dashboard_details.get("dashcards", [])
                    tabs = dashboard_details.get("tabs", [])
                    tab_dashcards = []
                    if tabs:
                        tab_dashcards = tabs[0].get("dashcards", [])
                    dashcards = top_level_dashcards + tab_dashcards
                    current_views_types = list(set([view for view in [CARD_DISPLAY_TYPE_MAPPING.get(d.get("card", {}).get("display")) for d in dashcards if d.get("card")] if view is not None]))
                    st.write("**現在のダッシュボードのビュー:**")
                    valid_dashcards = [dc for dc in dashcards if dc.get("card")]
                    if not valid_dashcards:
                            st.text("（ビューはありません）")
                    else:
                        delete_cols = st.columns(3) 
                        col_index = 0
                        for dashcard in valid_dashcards:
                            card_name = dashcard.get("card", {}).get("name", "名称未設定")
                            dashcard_id = dashcard.get("id")
                            with delete_cols[col_index % 3]:
                                with st.container(border=True):
                                    st.markdown(f"**{card_name}**")
                                    if st.button("🗑️ 削除", key=f"delete_dashcard_{dashcard_id}", use_container_width=True):
                                        log_details = {"card_name": card_name, "dashcard_id": dashcard_id}
                                        task_start = time.time()
                                        with st.spinner("カードを削除中..."):
                                            success = remove_card_from_dashboard(
                                                st.session_state.metabase_session_id, 
                                                dashboard_id, 
                                                dashcard_id
                                            )
                                        task_duration = time.time() - task_start
                                        if success:
                                            log_details["task_duration_sec"] = task_duration
                                            add_log_entry("delete_view", log_details)
                                            st.success("カードを削除しました。")
                                            if 'recommendations' in st.session_state:
                                                del st.session_state.recommendations
                                            time.sleep(1) 
                                            st.rerun() 
                                        else:
                                            st.error("カードの削除に失敗しました。")
                            col_index += 1
                    
                    if st.session_state.recommendations is None:
                        if current_views_types and st.session_state.get('use_recommendation', True): 
                            log_details = {"current_views": current_views_types} 
                            task_start = time.time() 
                            with st.spinner("RotatEモデルで推薦を生成中..."):
                                recommendations = get_recommendations_from_kge(context_views=current_views_types, top_k=5)
                            task_duration = time.time() - task_start 
                            log_details["recommendations"] = recommendations
                            log_details["task_duration_sec"] = task_duration
                            add_log_entry("generate_recommendations", log_details)
                            if recommendations:
                                st.session_state.recommendations = recommendations
                                st.rerun() 
                            else:
                                st.info("推薦できるビューはありませんでした。")
                                st.session_state.recommendations = [] 
                        else:
                            st.warning("推薦の基となるビューがダッシュボードにありません。")
                            st.session_state.recommendations = [] 
                    
                    st.write("**グラフ作成:**")
                    if st.session_state.recommendations and st.session_state.get('use_recommendation', True):
                        rec_cols = len(st.session_state.recommendations)
                        cols = st.columns(rec_cols + 1)
                        for i, rec_view in enumerate(st.session_state.recommendations):
                            with cols[i]:
                                with st.container(border=True):
                                    display_type = REVERSE_CARD_DISPLAY_TYPE_MAPPING.get(rec_view, "")
                                    icon = CHART_ICONS.get(display_type, "❓")
                                    # 日本語名に変換
                                    japanese_name = REVERSE_CHART_TYPE_MAP.get(display_type, rec_view)
                                    
                                    # ランキング表示
                                    rank_color = "#FFD700" if i == 0 else "#C0C0C0" if i == 1 else "#CD7F32" if i == 2 else "#f0f2f6"
                                    st.markdown(f"""
                                        <div style="background-color: {rank_color}; color: black; padding: 2px 8px; border-radius: 4px; text-align: center; font-weight: bold; margin-bottom: 5px;">
                                            {i+1}位
                                        </div>
                                    """, unsafe_allow_html=True)
                                    
                                    st.markdown(f"<h3 style='text-align: center;'>{icon}</h3>", unsafe_allow_html=True)
                                    st.markdown(f"<p style='text-align: center; font-weight: bold;'>{japanese_name}</p>", unsafe_allow_html=True)
                                    if st.button("作成", key=f"rec_{rec_view}", use_container_width=True):
                                        # 設定をリセット（テーブル選択もクリア）
                                        target_chart_type = REVERSE_CHART_TYPE_MAP.get(display_type)
                                        st.session_state.custom_builder_selections = {
                                            "table_id": None,
                                            "table_name": None,
                                            "available_fields": [],
                                            "chart_display_name": target_chart_type,
                                            "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None
                                        }
                                        # ウィジェットの状態を強制的に更新（上書き）
                                        if target_chart_type:
                                            st.session_state["custom_chart_type_selection"] = target_chart_type
                                        else:
                                            st.session_state.pop("custom_chart_type_selection", None)
                                        
                                        st.session_state.preview_data = None
                                        st.session_state.show_builder_dialog = True
                                        st.session_state.task_start_time = time.time()
                                        st.session_state.pending_recommendation = {
                                            "rank": i + 1, 
                                            "view_name": rec_view,
                                            "recommendation_list": st.session_state.recommendations
                                        }
                                        st.rerun()
                        with cols[rec_cols]:
                            with st.container(border=True):
                                st.markdown("<h3 style='text-align: center;'>➕</h3>", unsafe_allow_html=True)
                                st.markdown("<p style='text-align: center; font-weight: bold;'>新しいグラフを作成</p>", unsafe_allow_html=True)
                                if st.button("作成", key="custom_create_new", use_container_width=True):
                                    # 設定をリセット（テーブル選択もクリア）
                                    st.session_state.custom_builder_selections = {
                                        "table_id": None,
                                        "table_name": None,
                                        "available_fields": [],
                                        "chart_display_name": None,
                                        "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None
                                    }
                                    # ウィジェットの状態をクリア
                                    st.session_state.pop("custom_chart_type_selection", None)

                                    st.session_state.preview_data = None
                                    st.session_state.show_builder_dialog = True
                                    st.session_state.task_start_time = time.time()
                                    st.session_state.pending_recommendation = None 
                                    st.rerun()
                    else:
                        if st.button("📊 新しいグラフを対話的に作成する"):
                            # 設定をリセット（テーブル選択もクリア）
                            st.session_state.custom_builder_selections = {
                                "table_id": None,
                                "table_name": None,
                                "available_fields": [],
                                "chart_display_name": None,
                                "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None
                            }
                            # ウィジェットの状態をクリア
                            st.session_state.pop("custom_chart_type_selection", None)

                            st.session_state.preview_data = None
                            st.session_state.show_builder_dialog = True
                            st.session_state.task_start_time = time.time()
                            st.session_state.pending_recommendation = None 
                            st.rerun()

            st.markdown("---")
            st.subheader("📊 操作ログ")
            ss = SessionStorage()
            current_log = ss.getItem('operation_log')
            if current_log:
                try:
                    log_data_json = json.dumps(current_log, indent=2, ensure_ascii=False)
                    st.download_button(
                        label="操作ログをダウンロード (.json)",
                        data=log_data_json,
                        file_name=f"metabase_app_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True,
                        on_click=add_log_entry,
                        args=("finish_dashboard_creation", {})
                    )
                    with st.expander("最新のログを表示 (最新5件)"):
                        st.json(current_log[-5:])
                except Exception as e:
                    st.error(f"ログのシリアル化に失敗しました: {e}")
            else:
                st.info("まだ操作ログはありません。")

        if st.session_state.get('show_builder_dialog', False):
            if st.session_state.tables_metadata:
                display_custom_chart_form()
            else:
                 st.warning("データベースメタデータを読み込めませんでした。推薦機能は利用できません。")

if __name__ == '__main__':
    main()