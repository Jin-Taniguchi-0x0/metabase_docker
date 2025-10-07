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
# 逆引き用マッピングを追加
REVERSE_CARD_DISPLAY_TYPE_MAPPING = {v: k for k, v in CARD_DISPLAY_TYPE_MAPPING.items()}
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
    """重複した列名に接尾辞を追加して一意にする"""
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

@st.cache_data
def get_db_and_table_ids(_session_id: str) -> Dict[str, Any]:
    headers = {"X-Metabase-Session": _session_id}
    try:
        db_response = requests.get(f"{METABASE_API_URL}/api/database", headers=headers)
        db_response.raise_for_status()
        databases = db_response.json()
        sample_db = next((db for db in databases.get('data', []) if db['name'] == 'Sample Database'), None)
        if not sample_db: return {}
        db_id = sample_db['id']
        table_response = requests.get(f"{METABASE_API_URL}/api/database/{db_id}/metadata", headers=headers)
        table_response.raise_for_status()
        tables_metadata = table_response.json()
        accounts_table = next((tbl for tbl in tables_metadata.get('tables', []) if tbl['name'].upper() == 'ACCOUNTS'), None)
        if not accounts_table: return {}
        country_field = next((fld for fld in accounts_table.get('fields', []) if fld['name'].upper() == 'COUNTRY'), None)
        plan_field = next((fld for fld in accounts_table.get('fields', []) if fld['name'].upper() == 'PLAN'), None)
        if not country_field or not plan_field: return {}
        return {"db_id": db_id, "table_id": accounts_table['id'], "country_field_id": country_field['id'], "plan_field_id": plan_field['id']}
    except requests.exceptions.RequestException as e:
        st.error(f"データベースまたはテーブル情報の取得に失敗しました: {e}")
        return {}

@st.cache_data
def get_all_tables_metadata(_session_id: str, db_id: int) -> Optional[List[Dict]]:
    api_url = f"{METABASE_API_URL}/api/database/{db_id}/metadata"
    headers = {"X-Metabase-Session": _session_id}
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        return response.json().get('tables')
    except requests.exceptions.RequestException as e:
        st.error(f"テーブルメタデータの取得に失敗しました: {e}")
        return None

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

def execute_query(session_id: str, dataset_query: Dict[str, Any]) -> Optional[Dict]:
    """
    カードを保存せずに、指定された dataset_query を実行して結果を取得する (プレビュー用)
    """
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
    return [item['view'] for item in scores][:top_k]

# --- クエリビルダー関連ロジック ---

def get_all_available_fields(selections: Dict) -> List[Dict]:
    """ベーステーブルと結合テーブルの全フィールドをMBQL形式で返す"""
    all_fields = []
    # ベーステーブルのフィールドを追加
    for field in selections.get("available_fields", []):
        field_copy = field.copy()
        field_copy['mbql_ref'] = ["field", field['id'], None]
        field_copy['display_name_with_table'] = f"{selections.get('table_name', '')} -> {field['display_name']}"
        all_fields.append(field_copy)

    # 結合テーブルのフィールドを追加
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
    """テーブル選択が変更されたときに呼び出されるコールバック関数"""
    selected_table_name = st.session_state.get(f"{key_prefix}selected_table_name_key")
    
    if selected_table_name:
        table_options = {tbl['display_name']: tbl for tbl in st.session_state.tables_metadata}
        selected_table = table_options[selected_table_name]
        # テーブル変更時は結合情報とフィルターをリセット
        selections.update({
            "table_id": selected_table['id'], "table_name": selected_table_name,
            "available_fields": selected_table.get('fields', []),
            "joins": [], "filters": [], "aggregation": [], "breakout_id": None
        })
    else:
        selections.update({"table_id": None, "table_name": None, "available_fields": [], "filters": [], "joins": []})


def handle_custom_chart_submission(payload: Dict[str, Any], size_key: str):
    """
    渡されたペイロードを元に、カード作成とダッシュボードへの追加を実行する。
    """
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
            # 状態をリセット
            st.session_state.show_custom_chart_form = False
            st.session_state.custom_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": [], 'chart_display_name': None}
            st.session_state.preview_data = None
            time.sleep(2)
            st.rerun()

# --- クエリビルダーUIの分割された関数 ---

def display_existing_filters(selections: Dict, key_prefix: str = ""):
    for i, f in enumerate(selections["filters"]):
        value_str = f"`{f['value1']}`" + (f" と `{f['value2']}`" if f.get('value2') is not None else "")
        cols = st.columns([4, 3, 3, 1])
        cols[0].info(f"`{f['field_name']}`"); cols[1].info(f"{f['operator_name']}"); cols[2].info(value_str)
        if cols[3].button("🗑️", key=f"{key_prefix}del_filter_{i}", help="このフィルターを削除"):
            selections["filters"].pop(i); st.rerun()

def display_add_filter_form(selections: Dict, key_prefix: str = ""):
    with st.expander("＋ フィルターを追加する"):
        all_fields = get_all_available_fields(selections)
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
        if st.button("フィルターを追加", key=f"{key_prefix}add_filter_button"):
            if new_filter_field_display_name and new_filter_op_name:
                selected_field = field_options[new_filter_field_display_name]
                selections["filters"].append({
                    "field_ref": selected_field['mbql_ref'], "field_name": selected_field['display_name_with_table'], 
                    "operator": operator_map[new_filter_op_name], "operator_name": new_filter_op_name, 
                    "value1": new_filter_value1, "value2": new_filter_value2
                }); st.rerun()

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

def display_aggregation_breakout_form(selections: Dict, show_breakout: bool = True, key_prefix: str = "") -> Tuple[Optional[str], Optional[Any], Optional[Any]]:
    all_fields = get_all_available_fields(selections)
    cols = st.columns(2) if show_breakout else [st.container()]
    agg_container, breakout_container = cols[0], (cols[1] if show_breakout else None)
    agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
    agg_type_name = agg_container.selectbox("集約方法", agg_map.keys(), key=f"{key_prefix}agg_type_name")
    agg_field_ref = None
    field_required_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
    if agg_map[agg_type_name] in field_required_aggs:
        numeric_fields = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields if any(t in f.get('base_type', '').lower() for t in ['integer', 'float', 'double', 'decimal']) and f.get('semantic_type') not in ['type/PK', 'type/FK']}
        agg_field_display_name = agg_container.selectbox("集計対象の列", numeric_fields.keys(), key=f"{key_prefix}agg_field_name", index=None)
        if agg_field_display_name: agg_field_ref = numeric_fields[agg_field_display_name]
    breakout_field_ref = None
    if show_breakout and breakout_container:
        field_options = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
        breakout_field_display_name = breakout_container.selectbox("グループ化する列", field_options.keys(), index=None, key=f"{key_prefix}breakout_field_name")
        breakout_field_ref = field_options.get(breakout_field_display_name)
    return agg_type_name, agg_field_ref, breakout_field_ref

def display_scatter_plot_form(selections: Dict, key_prefix: str = "") -> Tuple[Optional[Dict], Optional[Any]]:
    st.info("散布図は、2つの指標（数値）の関係性を可視化します。オプションでカテゴリによる色分けも可能です。")
    all_fields = get_all_available_fields(selections)
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

def display_pivot_table_form(selections: Dict, key_prefix: str = ""):
    st.info("ピボットテーブルは、データをクロス集計して表示します。行、列、集計したい値をそれぞれ指定してください。")
    all_fields = get_all_available_fields(selections)
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


def display_custom_chart_form():
    """高機能クエリビルダーとプレビューダイアログのUIを表示・管理する"""
    selections = st.session_state.custom_builder_selections
    key_prefix = "custom_"

    if st.session_state.get('show_preview_dialog', False):
        @st.dialog("グラフプレビュー")
        def show_preview():
            # ... (dialog content remains the same)
            preview_data = st.session_state.preview_data
            if not preview_data:
                st.error("プレビューデータの読み込みに失敗しました。")
                return

            df = preview_data['df']
            chart_type = preview_data['chart_type']
            
            st.subheader("プレビュー")
            
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
                        else:
                            st.info("このグラフ種別のプレビューは現在サポートされていません。データテーブルを表示します。")
                            st.dataframe(df)
                    except Exception as e:
                        st.error(f"グラフの描画に失敗しました: {e}")
                        st.dataframe(df)
            else:
                st.info("クエリは成功しましたが、結果は0件でした。")

            st.markdown("---")
            st.write("このグラフをダッシュボードに追加しますか？")
            
            col1, col2, _ = st.columns([1, 1, 2])
            with col1:
                if st.button("はい、追加します", type="primary", use_container_width=True):
                    st.session_state.show_preview_dialog = False
                    handle_custom_chart_submission(st.session_state.preview_data['final_payload'], size_key=f"{key_prefix}card_size_selection")
                    st.rerun() 
            with col2:
                if st.button("いいえ、戻ります", use_container_width=True):
                    st.session_state.show_preview_dialog = False
                    st.rerun()
        show_preview()


    with st.container(border=True):
        st.subheader("クエリビルダー")
        
        # --- FLOW CHANGE: Step 1 - Select Graph Type ---
        chart_type_options = list(CHART_TYPE_MAP.keys())
        # Set a default value for chart_display_name to avoid errors if it's not set
        current_chart_display_name = selections.get('chart_display_name')
        current_chart_index = chart_type_options.index(current_chart_display_name) if current_chart_display_name in chart_type_options else None
        
        def on_chart_type_change():
            st.session_state.custom_builder_selections['chart_display_name'] = st.session_state[f"{key_prefix}chart_type_selection"]

        chart_display_name = st.selectbox(
            "1. グラフの種類を選択", 
            chart_type_options, 
            key=f"{key_prefix}chart_type_selection", 
            index=current_chart_index,
            on_change=on_chart_type_change,
            placeholder="グラフの種類を選択..."
        )
        
        # --- FLOW CHANGE: Step 2 - Select Table (if graph type is chosen) ---
        if selections.get("chart_display_name"):
            table_options = {tbl['display_name']: tbl['id'] for tbl in st.session_state.tables_metadata}
            st.selectbox("2. ベースとなるテーブルを選択", table_options.keys(), 
                index=list(table_options.keys()).index(selections["table_name"]) if selections.get("table_name") else None, 
                on_change=handle_table_selection, 
                args=(selections, key_prefix),
                key=f"{key_prefix}selected_table_name_key", 
                placeholder="テーブルを選択...")

            # --- FLOW CHANGE: Step 3 - Show rest of the builder (if table is chosen) ---
            if selections.get("table_id"):
                st.markdown("---"); st.markdown("**テーブル結合**"); display_existing_joins(selections, key_prefix=key_prefix); display_join_builder(selections, key_prefix=key_prefix)
                st.markdown("---"); st.markdown("**フィルター**"); display_existing_filters(selections, key_prefix=key_prefix); display_add_filter_form(selections, key_prefix=key_prefix)
                st.markdown("---"); st.markdown("**データ定義**")

                scatter_axes, breakout_field_ref, agg_type_name, agg_field_ref = None, None, None, None
                if chart_display_name == "散布図":
                    scatter_axes, breakout_field_ref = display_scatter_plot_form(selections, key_prefix=key_prefix)
                elif chart_display_name == "ピボットテーブル":
                    display_pivot_table_form(selections, key_prefix=key_prefix)
                else:
                    charts_without_breakout = ["数値", "ゲージ"]
                    show_breakout = chart_display_name not in charts_without_breakout
                    agg_type_name, agg_field_ref, breakout_field_ref = display_aggregation_breakout_form(selections, show_breakout=show_breakout, key_prefix=key_prefix)

                st.markdown("---")
                
                st.selectbox('カードサイズを選択', list(SIZE_MAPPING.keys()), key=f'{key_prefix}card_size_selection')

                if st.button("プレビューして作成...", type="primary", key=f"{key_prefix}preview_button"):
                    # ... (Button logic remains the same)
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
                        if len(filter_clauses) > 1: query["filter"] = ["and"] + filter_clauses
                        elif filter_clauses: query["filter"] = filter_clauses[0]
                    
                    agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
                    agg_type = agg_map.get(agg_type_name) if agg_type_name else None
                    
                    preview_extras = {}

                    if chart_display_name == "散布図":
                        x_ref, y_ref = scatter_axes["x_axis"], scatter_axes["y_axis"]
                        if not x_ref or not y_ref: st.error("散布図にはX軸とY軸の両方を選択してください。"); return
                        query["fields"] = [x_ref, y_ref]
                        if breakout_field_ref: query["fields"].append(breakout_field_ref)
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

                    else:
                        if agg_type:
                            field_req_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
                            if agg_type in field_req_aggs: 
                                if not agg_field_ref: st.error("この集約方法には集計対象の列が必要です。"); return
                                query["aggregation"] = [[agg_type, agg_field_ref]]
                            else: query["aggregation"] = [[agg_type]]
                    if breakout_field_ref and chart_display_name not in ["散布図", "ピボットテーブル"]: 
                        query["breakout"] = [breakout_field_ref]
                    
                    dataset_query = {"type": "query", "database": selected_table['db_id'], "query": query}
                    
                    with st.spinner("プレビューデータを取得中..."):
                        result = execute_query(st.session_state.metabase_session_id, dataset_query)
                    
                    if result and result.get('status') == 'completed':
                        result_cols = result['data']['cols']
                        display_names = [c['display_name'] for c in result_cols]
                        internal_names = [c['name'] for c in result_cols]
                        unique_display_names = _deduplicate_columns(display_names)
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
                            if len(internal_names) >= 2: viz_settings = {"graph.dimensions": [internal_names[0]], "graph.metrics": [internal_names[1]]}
                            x_field = next((f for f in all_fields if f['mbql_ref'] == scatter_axes["x_axis"]), None)
                            y_field = next((f for f in all_fields if f['mbql_ref'] == scatter_axes["y_axis"]), None)
                            breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), None) if breakout_field_ref else None
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
                                    "columns": [display_to_internal_map[name] for name in preview_extras.get('pivot_col_names', [])],
                                    "rows": [display_to_internal_map[name] for name in preview_extras.get('pivot_row_names', [])],
                                    "values": [display_to_internal_map[name] for name in preview_extras.get('pivot_val_names', [])]
                                }
                            }
                        else:
                            agg_field = next((f for f in all_fields if f['mbql_ref'] == agg_field_ref), None) if agg_field_ref else None
                            agg_str = f"の{agg_field['display_name_with_table']}" if agg_field else ""
                            if breakout_field_ref:
                                breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), None)
                                card_name = f"{chart_display_name}: {breakout_field['display_name_with_table']}別 {agg_type_name}{agg_str}"
                            else:
                                card_name = f"{chart_display_name}: {agg_type_name}{agg_str}"

                        final_payload = {
                            "name": card_name,
                            "display": CHART_TYPE_MAP.get(chart_display_name),
                            "dataset_query": dataset_query,
                            "visualization_settings": viz_settings
                        }
                        
                        st.session_state.preview_data = {'df': df, 'chart_type': CHART_TYPE_MAP.get(chart_display_name), 'final_payload': final_payload, **preview_extras}
                        st.session_state.show_preview_dialog = True
                        st.rerun()
                    else:
                        st.error("プレビューデータの取得に失敗しました。")

    if st.button("ビルダーを閉じる", key=f"{key_prefix}close_builder"):
        st.session_state.show_custom_chart_form = False
        st.session_state.custom_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": [], 'chart_display_name': None}
        st.session_state.preview_data = None
        st.rerun()

def display_credentials_form():
    st.header("Metabase 認証情報")
    with st.form("credentials_form"):
        username, password = st.text_input("Username"), st.text_input("Password", type="password")
        dashboard_id, secret_key = st.text_input("Dashboard ID"), st.text_input("Secret Key", type="password")
        if st.form_submit_button("接続"):
            session_id = get_metabase_session(username, password)
            if session_id:
                st.session_state.update(metabase_session_id=session_id, dashboard_id=dashboard_id, secret_key=secret_key)
                st.rerun()

def embed_dashboard():
    secret_key, dashboard_id = st.session_state.secret_key, normalize_id(st.session_state.dashboard_id)
    if not secret_key or not dashboard_id: return
    try:
        payload = {"resource": {"dashboard": int(dashboard_id)}, "params": {}, "exp": round(time.time()) + (60 * 10)}
        token = jwt.encode(payload, secret_key, algorithm="HS256")
        iframe_url = f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"
        st.components.v1.iframe(iframe_url, height=800, scrolling=True)
    except ValueError:
        st.error(f"ダッシュボードID '{dashboard_id}' は有効な数値ではありません。")


# --- NEW: Recommendation Integration ---
def display_recommendation_card_creator():
    """推薦されたビューをダッシュボードに追加するためのダイアログを表示する"""
    
    selected_view = st.session_state.selected_recommendation
    display_type = REVERSE_CARD_DISPLAY_TYPE_MAPPING.get(selected_view, "bar") # e.g. "pivot-table"
    clean_name = selected_view.replace('visual-', '').replace('Chart', ' Chart').title()
    key_prefix = "rec_"

    @st.dialog(f"推薦グラフ「{clean_name}」を作成")
    def card_creator_dialog():
        selections = st.session_state.recommendation_builder_selections
        
        if st.session_state.get('selected_recommendation') != selections.get('current_recommendation'):
             selections.clear()
             selections.update({
                "table_id": None, "table_name": None, "joins": [], "filters": [], 
                "aggregation": [], "breakout_id": None, "breakout_name": None, 
                "available_fields": [], 'current_recommendation': st.session_state.get('selected_recommendation')
             })

        st.info(f"ダッシュボードに「{clean_name}」を追加します。以下の項目を選択してください。")

        table_options = {tbl['display_name']: tbl['id'] for tbl in st.session_state.tables_metadata}
        st.selectbox(
            "1. ベースとなるテーブルを選択", 
            table_options.keys(), 
            index=list(table_options.keys()).index(selections["table_name"]) if selections.get("table_name") else None, 
            on_change=handle_table_selection, 
            args=(selections, key_prefix),
            key=f"{key_prefix}selected_table_name_key",
            placeholder="テーブルを選択..."
        )

        if selections.get("table_id"):
            selected_table = next((tbl for tbl in st.session_state.tables_metadata if tbl['id'] == selections['table_id']), None)
            st.markdown("---")
            st.markdown("2. データ定義")
            
            scatter_axes, breakout_field_ref, agg_type_name, agg_field_ref = None, None, None, None

            if display_type == "scatter":
                scatter_axes, breakout_field_ref = display_scatter_plot_form(selections, key_prefix=key_prefix)
            elif display_type == "pivot-table":
                display_pivot_table_form(selections, key_prefix=key_prefix)
            else:
                agg_type_name, agg_field_ref, breakout_field_ref = display_aggregation_breakout_form(selections, show_breakout=True, key_prefix=key_prefix)
            
            st.markdown("---")
            st.selectbox('3. カードサイズを選択', list(SIZE_MAPPING.keys()), key=f'{key_prefix}card_size_selection')

            if st.button("作成してダッシュボードに追加", type="primary"):
                all_fields = get_all_available_fields(selections)
                query = {"source-table": selections['table_id']}
                card_name = ""
                viz_settings = {}

                try:
                    if display_type == "scatter":
                        x_ref, y_ref = scatter_axes["x_axis"], scatter_axes["y_axis"]
                        if not x_ref or not y_ref:
                            st.error("散布図にはX軸とY軸の両方を選択してください。")
                            return
                        query["fields"] = [x_ref, y_ref]
                        if breakout_field_ref: query["fields"].append(breakout_field_ref)
                        
                        x_field = next((f for f in all_fields if f['mbql_ref'] == x_ref), {})
                        y_field = next((f for f in all_fields if f['mbql_ref'] == y_ref), {})
                        breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), {}) if breakout_field_ref else {}
                        x_name, y_name = x_field.get('display_name', ''), y_field.get('display_name', '')
                        breakout_name = f" ({breakout_field.get('display_name', '')}別)" if breakout_field else ""
                        card_name = f"推薦 散布図: {y_name} vs {x_name}{breakout_name}"
                        viz_settings = {} 

                    elif display_type == "pivot-table":
                        pivot_rows_names = selections.get('pivot_rows', [])
                        pivot_cols_names = selections.get('pivot_cols', [])
                        pivot_vals_names = selections.get('pivot_vals', [])
                        if not pivot_rows_names or not pivot_vals_names:
                            st.error("ピボットテーブルには少なくとも「行」と「値」が必要です。")
                            return
                        
                        field_name_map = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
                        row_refs = [field_name_map[name] for name in pivot_rows_names if name]
                        col_refs = [field_name_map[name] for name in pivot_cols_names if name]
                        val_refs = [field_name_map[name] for name in pivot_vals_names if name]
                        
                        mbql_agg_func = selections.get('pivot_agg_func', 'sum')
                        query["breakout"] = row_refs + col_refs
                        query["aggregation"] = [[mbql_agg_func, ref] for ref in val_refs]

                        rows_str = ", ".join(filter(None, pivot_rows_names))
                        vals_str = ", ".join(filter(None, pivot_vals_names))
                        agg_str = selections.get('pivot_agg_func_display', '合計')
                        card_name = f"推薦 ピボット: {rows_str} 別 {vals_str}の{agg_str}"
                        viz_settings = {} 

                    else:
                        if not breakout_field_ref:
                            st.error("このグラフには「グループ化する列」が必要です。")
                            return
                        
                        agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
                        agg_type = agg_map.get(agg_type_name)
                        if agg_type:
                            if agg_field_ref: query["aggregation"] = [[agg_type, agg_field_ref]]
                            else: query["aggregation"] = [["count"]]
                        query["breakout"] = [breakout_field_ref]

                        breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), {})
                        card_name = f"推薦: {clean_name} ({breakout_field.get('display_name_with_table','')})"
                        viz_settings = {}
                    
                    dataset_query = {"type": "query", "database": selected_table['db_id'], "query": query}
                    
                    if display_type == "pivot-table":
                        with st.spinner("メタデータを取得中..."):
                            result = execute_query(st.session_state.metabase_session_id, dataset_query)
                        if not (result and result.get('status') == 'completed'):
                            st.error("クエリの実行に失敗しました。設定を確認してください。")
                            return
                        
                        result_cols = result['data']['cols']
                        internal_names = [c['name'] for c in result_cols]
                        
                        num_rows = len([name for name in selections.get('pivot_rows', []) if name])
                        num_cols = len([name for name in selections.get('pivot_cols', []) if name])
                        
                        row_internal_names = internal_names[:num_rows]
                        col_internal_names = internal_names[num_rows : num_rows + num_cols]
                        val_internal_names = internal_names[num_rows + num_cols :]

                        viz_settings = {
                            "pivot_table": {
                                "columns": col_internal_names,
                                "rows": row_internal_names,
                                "values": val_internal_names
                            }
                        }

                    final_payload = {
                        "name": card_name,
                        "display": display_type,
                        "dataset_query": dataset_query,
                        "visualization_settings": viz_settings
                    }
                    
                    handle_custom_chart_submission(final_payload, size_key=f"{key_prefix}card_size_selection")
                    st.session_state.show_recommendation_dialog = False
                    st.session_state.recommendation_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": []}
                    st.rerun()
                except Exception as e:
                    st.error(f"カードの作成中にエラーが発生しました: {e}")

    card_creator_dialog()


def main():
    st.set_page_config(layout="wide"); st.title("ダッシュボードビュー推薦システム (RotatE版)")
    # --- Session State Initialization ---
    if 'metabase_session_id' not in st.session_state: st.session_state.metabase_session_id = None
    if 'dashboard_id' not in st.session_state: st.session_state.dashboard_id = ""
    if 'secret_key' not in st.session_state: st.session_state.secret_key = ""
    if 'kge_model' not in st.session_state:
        st.session_state.kge_model, st.session_state.training_factory, st.session_state.relation_df = load_kge_model_and_data()
    if 'show_custom_chart_form' not in st.session_state: st.session_state.show_custom_chart_form = False
    if 'tables_metadata' not in st.session_state: st.session_state.tables_metadata = None
    # Separate states for the two builders
    if 'custom_builder_selections' not in st.session_state:
        st.session_state.custom_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": [], 'chart_display_name': None}
    if 'recommendation_builder_selections' not in st.session_state:
        st.session_state.recommendation_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": []}

    if 'preview_data' not in st.session_state: st.session_state.preview_data = None
    if 'show_preview_dialog' not in st.session_state: st.session_state.show_preview_dialog = False
    # --- New Session State for Recommendations ---
    if 'recommendations' not in st.session_state: st.session_state.recommendations = None
    if 'selected_recommendation' not in st.session_state: st.session_state.selected_recommendation = None
    if 'show_recommendation_dialog' not in st.session_state: st.session_state.show_recommendation_dialog = False

    if st.session_state.metabase_session_id is None: 
        display_credentials_form()
    else:
        # --- Auto-load metadata on first run after login ---
        if st.session_state.tables_metadata is None:
            ids = get_db_and_table_ids(st.session_state.metabase_session_id)
            if ids and 'db_id' in ids:
                with st.spinner("データベースメタデータを読み込み中..."):
                    st.session_state.tables_metadata = get_all_tables_metadata(st.session_state.metabase_session_id, ids['db_id'])
                if not st.session_state.tables_metadata: 
                    st.warning("テーブルメタデータの読み込みに失敗しました。")
            else: 
                st.error("データベースIDの取得に失敗したため、メタデータを読み込めません。")
        
        if 'card_size_selection' not in st.session_state:
            st.session_state.card_size_selection = list(SIZE_MAPPING.keys())[0]

        embed_dashboard()
        
        # --- FLOW CHANGE: Recommendation Section First ---
        with st.container(border=True):
            st.header("ビュー推薦")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.info("💡 ダッシュボードにグラフを追加・削除した後は、「推薦を更新する」ボタンを押してください。")
            with col2:
                if st.button("🔄 推薦を更新する", use_container_width=True):
                    if 'recommendations' in st.session_state:
                        del st.session_state.recommendations
                    st.rerun()

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

                    current_views = list(set([view for view in [CARD_DISPLAY_TYPE_MAPPING.get(d.get("card", {}).get("display")) for d in dashcards if d.get("card")] if view is not None]))
                    
                    st.write("**現在のダッシュボードのビュー:**")
                    if current_views:
                        st.json(current_views)
                    else:
                        st.text("（ビューはありません）")
                    
                    if st.button("このダッシュボードにおすすめのビューを生成"):
                        if current_views:
                            with st.spinner("RotatEモデルで推薦を生成中..."):
                                recommendations = get_recommendations_from_kge(context_views=current_views, top_k=5)
                            if recommendations:
                                st.success("おすすめのビューが見つかりました！")
                                st.session_state.recommendations = recommendations
                                st.rerun()
                            else:
                                st.info("推薦できるビューはありませんでした。")
                        else:
                            st.warning("推薦の基となるビューがダッシュボードにありません。")
                    
                    if st.session_state.recommendations:
                        st.write("**おすすめのビュー:**")
                        cols = st.columns(len(st.session_state.recommendations))
                        for i, rec_view in enumerate(st.session_state.recommendations):
                            with cols[i]:
                                with st.container(border=True):
                                    display_type = REVERSE_CARD_DISPLAY_TYPE_MAPPING.get(rec_view, "")
                                    icon = CHART_ICONS.get(display_type, "❓")
                                    clean_name = rec_view.replace('visual-', '').replace('Chart', ' Chart').title()
                                    st.markdown(f"<h3 style='text-align: center;'>{icon}</h3>", unsafe_allow_html=True)
                                    st.markdown(f"<p style='text-align: center; font-weight: bold;'>{clean_name}</p>", unsafe_allow_html=True)
                                    if st.button("作成", key=f"rec_{rec_view}", use_container_width=True):
                                        st.session_state.selected_recommendation = rec_view
                                        st.session_state.show_recommendation_dialog = True
                                        st.rerun()
            st.markdown("---")

        if st.session_state.get('show_recommendation_dialog', False):
            if st.session_state.tables_metadata:
                display_recommendation_card_creator()
            else:
                 st.warning("データベースメタデータを読み込めませんでした。推薦機能は利用できません。")

        # --- FLOW CHANGE: Custom Graph Creator Second ---
        st.header("カスタムグラフ作成")
        if st.button("📊 新しいグラフを対話的に作成する"):
            st.session_state.show_custom_chart_form = True
            st.rerun()

        if st.session_state.show_custom_chart_form: 
            if st.session_state.tables_metadata:
                display_custom_chart_form()
            else:
                st.warning("データベースメタデータを読み込めませんでした。グラフ作成機能は利用できません。")

        st.header("サンプルチャート作成＆ダッシュボードに追加")
        
        st.selectbox('追加するカードのサイズを選択してください', list(SIZE_MAPPING.keys()), key='card_size_selection_sample')

        ids = get_db_and_table_ids(st.session_state.metabase_session_id)
        if ids:
            col1, col2 = st.columns(2)
            card_size = SIZE_MAPPING.get(st.session_state.card_size_selection_sample)
            if col1.button("棒グラフを作成＆追加"):
                payload = {"name": f"Sample Bar Chart - {int(time.time())}", "display": "bar", "dataset_query": {"type": "query", "database": ids['db_id'], "query": {"source-table": ids['table_id'], "aggregation": [["count"]], "breakout": [["field", ids['country_field_id'], None]]}}, "visualization_settings": {}}
                with st.spinner("作成中..."):
                    card_id = create_card(st.session_state.metabase_session_id, payload)
                    if card_id: 
                        success = add_card_to_dashboard(st.session_state.metabase_session_id, st.session_state.dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
                        if success: st.success("追加しました！"); time.sleep(2); st.rerun()
            if col2.button("円グラフを作成＆追加"):
                payload = {"name": f"Sample Pie Chart - {int(time.time())}", "display": "pie", "dataset_query": {"type": "query", "database": ids['db_id'], "query": {"source-table": ids['table_id'], "aggregation": [["count"]], "breakout": [["field", ids['plan_field_id'], None]]}}, "visualization_settings": {}}
                with st.spinner("作成中..."):
                    card_id = create_card(st.session_state.metabase_session_id, payload)
                    if card_id:
                        success = add_card_to_dashboard(st.session_state.metabase_session_id, st.session_state.dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
                        if success: st.success("追加しました！"); time.sleep(2); st.rerun()

if __name__ == '__main__':
    main()

