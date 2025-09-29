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
SIZE_MAPPING = {
    'S (幅1/3)': {'width': 8, 'height': 4},
    'M (幅1/2)': {'width': 12, 'height': 5},
    'L (幅2/3)': {'width': 16, 'height': 6}
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
        field_copy['display_name_with_table'] = f"{selections['table_name']} -> {field['display_name']}"
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

def handle_table_selection():
    """テーブル選択が変更されたときに呼び出されるコールバック関数"""
    selections = st.session_state.query_builder_selections
    selected_table_name = st.session_state.get("selected_table_name_key")
    
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

def handle_custom_chart_submission(chart_display_name: str, agg_type: Optional[str], breakout_field_ref=None, agg_field_ref=None, aggregations=None):
    """拡張されたフォーム情報からMBQLペイロードを構築し、APIを呼び出す"""
    selections = st.session_state.query_builder_selections
    table_id = selections['table_id']
    charts_with_breakout = ["棒グラフ", "折れ線グラフ", "エリアグラフ", "円グラフ"]

    # バリデーション
    if not table_id:
        st.error("ベースとなるテーブルを選択してください。")
        return
    if chart_display_name in charts_with_breakout and not breakout_field_ref:
        st.error("このグラフの種類には「グループ化する列」の選択が必要です。")
        return
    if chart_display_name == "散布図" and (not aggregations or len(aggregations) < 2):
        st.error("散布図にはX軸とY軸、両方の指標を設定してください。")
        return

    selected_table = next((tbl for tbl in st.session_state.tables_metadata if tbl['id'] == table_id), None)
    all_fields = get_all_available_fields(selections)
    
    # --- MBQLクエリ構築 ---
    query = {"source-table": table_id}

    if selections["joins"]:
        query["joins"] = [{
            "alias": join["join_alias"], "source-table": join["target_table_id"],
            "condition": join["condition"], "strategy": join["strategy"], "fields": "all"
        } for join in selections["joins"]]

    # Aggregation
    if chart_display_name == "散布図":
        query["aggregation"] = aggregations
    else:
        field_required_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
        if agg_type in field_required_aggs:
            if not agg_field_ref:
                st.error("この集約方法には集計対象の列が必要です。"); return
            query["aggregation"] = [[agg_type, agg_field_ref]]
        else:
            query["aggregation"] = [[agg_type]]

    if breakout_field_ref:
        query["breakout"] = [breakout_field_ref]

    # Filters
    if selections["filters"]:
        filter_clauses = []
        for f in selections["filters"]:
            op, field_clause = f["operator"], f["field_ref"]
            if op in ["is-null", "not-null"]:
                clause = [op, field_clause]
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

    # --- カード名生成 ---
    if chart_display_name == "散布図":
        y_agg_field = next((f for f in all_fields if f['mbql_ref'] == aggregations[0][1]), None)
        x_agg_field = next((f for f in all_fields if f['mbql_ref'] == aggregations[1][1]), None)
        breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), None) if breakout_field_ref else None
        
        y_agg_name = f"{y_agg_field['display_name']}の{aggregations[0][0]}" if y_agg_field else "Y軸指標"
        x_agg_name = f"{x_agg_field['display_name']}の{aggregations[1][0]}" if x_agg_field else "X軸指標"
        breakout_name = f"{breakout_field['display_name']}別 " if breakout_field else ""
        card_name = f"散布図: {breakout_name}{y_agg_name} vs {x_agg_name}"
    else:
        agg_field = next((f for f in all_fields if f['mbql_ref'] == agg_field_ref), None) if agg_field_ref else None
        agg_str = f"の{agg_field['display_name_with_table']}" if agg_field else ""
        if breakout_field_ref:
            breakout_field = next((f for f in all_fields if f['mbql_ref'] == breakout_field_ref), None)
            card_name = f"{chart_display_name}: {breakout_field['display_name_with_table']}別 {st.session_state.agg_type_name}{agg_str}"
        else:
            card_name = f"{chart_display_name}: {st.session_state.agg_type_name}{agg_str}"

    # --- APIペイロード作成と実行 ---
    payload = {
        "name": card_name, "display": CHART_TYPE_MAP[chart_display_name],
        "dataset_query": {"type": "query", "database": selected_table['db_id'], "query": query},
        "visualization_settings": {}
    }

    dashboard_id = normalize_id(st.session_state.dashboard_id)
    card_size = SIZE_MAPPING.get(st.session_state.card_size_selection)
    with st.spinner("グラフを作成中..."):
        card_id = create_card(st.session_state.metabase_session_id, payload)
    if card_id:
        with st.spinner("ダッシュボードに追加中..."):
            success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
        if success:
            st.success("ダッシュボードに追加しました！ページを更新します。")
            st.session_state.show_custom_chart_form = False
            st.session_state.query_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": []}
            time.sleep(2); st.rerun()

# --- クエリビルダーUIの分割された関数 ---

def display_existing_filters(selections: Dict):
    for i, f in enumerate(selections["filters"]):
        value_str = f"`{f['value1']}`" + (f" と `{f['value2']}`" if f.get('value2') is not None else "")
        cols = st.columns([4, 3, 3, 1])
        cols[0].info(f"`{f['field_name']}`")
        cols[1].info(f"{f['operator_name']}")
        cols[2].info(value_str)
        if cols[3].button("🗑️", key=f"del_filter_{i}", help="このフィルターを削除"):
            selections["filters"].pop(i); st.rerun()

def display_add_filter_form(selections: Dict):
    with st.expander("＋ フィルターを追加する"):
        all_fields = get_all_available_fields(selections)
        field_options = {f['display_name_with_table']: f for f in all_fields}
        cols = st.columns(2)
        new_filter_field_display_name = cols[0].selectbox("列", field_options.keys(), index=None, key="new_filter_field")
        operator_map = {"である": "=", "ではない": "!=", "より大きい": ">", "より小さい": "<", "以上": ">=", "以下": "<=", "範囲": "between", "空": "is-null", "空ではない": "not-null"}
        new_filter_op_name = cols[1].selectbox("条件", operator_map.keys(), index=None, key="new_filter_op")
        new_filter_value1, new_filter_value2 = None, None
        if new_filter_op_name and operator_map[new_filter_op_name] not in ["is-null", "not-null"]:
            if operator_map[new_filter_op_name] == "between":
                val_cols = st.columns(2)
                new_filter_value1 = val_cols[0].text_input("開始値", key="new_filter_value1")
                new_filter_value2 = val_cols[1].text_input("終了値", key="new_filter_value2")
            else:
                new_filter_value1 = st.text_input("値", key="new_filter_value1")
        if st.button("フィルターを追加"):
            if new_filter_field_display_name and new_filter_op_name:
                selected_field = field_options[new_filter_field_display_name]
                selections["filters"].append({
                    "field_ref": selected_field['mbql_ref'], "field_name": selected_field['display_name_with_table'], 
                    "operator": operator_map[new_filter_op_name], "operator_name": new_filter_op_name, 
                    "value1": new_filter_value1, "value2": new_filter_value2
                }); st.rerun()

def display_existing_joins(selections: Dict):
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
            if cols[1].button("🗑️", key=f"del_join_{i}", help="この結合を削除"):
                selections["joins"].pop(i); st.rerun()

def display_join_builder(selections: Dict):
    with st.expander("＋ 結合を追加する"):
        joinable_tables = {tbl['display_name']: tbl for tbl in st.session_state.tables_metadata if tbl['id'] != selections['table_id']}
        target_table_name = st.selectbox("結合するテーブル", joinable_tables.keys(), index=None, key="join_target_table", placeholder="テーブルを選択...")
        if target_table_name:
            target_table = joinable_tables[target_table_name]
            join_type_display_name = st.selectbox("結合方法", JOIN_STRATEGY_MAP.keys(), key="join_strategy")
            st.write("結合条件:")
            cols = st.columns([5, 1, 5])
            base_fields = {f['display_name']: f['id'] for f in selections['available_fields']}
            base_field_name = cols[0].selectbox(f"{selections['table_name']} の列", base_fields.keys(), index=None, key="join_base_field")
            cols[1].markdown("<p style='text-align: center; font-size: 24px; margin-top: 25px'>=</p>", unsafe_allow_html=True)
            target_fields = {f['display_name']: f['id'] for f in target_table['fields']}
            target_field_name = cols[2].selectbox(f"{target_table_name} の列", target_fields.keys(), index=None, key="join_target_field")
            if st.button("結合を追加"):
                if base_field_name and target_field_name and join_type_display_name:
                    join_count = len(selections.get("joins", []))
                    join_alias = f"_join_{join_count + 1}"
                    new_join = {"target_table_id": target_table['id'], "target_table_name": target_table_name,
                                "join_alias": join_alias, "strategy": JOIN_STRATEGY_MAP[join_type_display_name],
                                "condition": ["=", ["field", base_fields[base_field_name], None], ["field", target_fields[target_field_name], {"join-alias": join_alias}]]}
                    selections["joins"].append(new_join); st.rerun()

def display_aggregation_breakout_form(selections: Dict, show_breakout: bool = True) -> Tuple[Optional[str], Optional[Any], Optional[Any]]:
    all_fields = get_all_available_fields(selections)
    cols = st.columns(2) if show_breakout else [st.container()]
    agg_container, breakout_container = cols[0], (cols[1] if show_breakout else None)
    agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
    agg_type_name = agg_container.selectbox("集約方法", agg_map.keys(), key="agg_type_name")
    agg_field_ref = None
    field_required_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
    if agg_map[agg_type_name] in field_required_aggs:
        numeric_fields = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields if any(t in f['base_type'].lower() for t in ['integer', 'float', 'double', 'decimal']) and f.get('semantic_type') not in ['type/PK', 'type/FK']}
        agg_field_display_name = agg_container.selectbox("集計対象の列", numeric_fields.keys(), key="agg_field_name", index=None)
        if agg_field_display_name: agg_field_ref = numeric_fields[agg_field_display_name]
    breakout_field_ref = None
    if show_breakout and breakout_container:
        field_options = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
        breakout_field_display_name = breakout_container.selectbox("グループ化する列", field_options.keys(), index=None, key="breakout_field_name")
        breakout_field_ref = field_options.get(breakout_field_display_name)
    return agg_type_name, agg_field_ref, breakout_field_ref

def display_scatter_plot_form(selections: Dict) -> Tuple[Optional[List], Optional[Any]]:
    st.info("散布図は、2つの指標（数値）の関係性を可視化します。オプションでカテゴリによる色分けも可能です。")
    all_fields = get_all_available_fields(selections)
    numeric_fields = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields if any(t in f['base_type'].lower() for t in ['integer', 'float', 'double', 'decimal']) and f.get('semantic_type') not in ['type/PK', 'type/FK']}
    agg_map = {"合計": "sum", "平均": "avg", "異なる値の数": "distinct", "標準偏差": "stddev", "最小値": "min", "最大値": "max"}
    
    aggregations = []
    
    # Y-Axis
    st.markdown("##### Y軸の指標")
    cols_y = st.columns(2)
    y_agg_type_name = cols_y[0].selectbox("集約方法", agg_map.keys(), key="y_agg_type")
    y_field_display_name = cols_y[1].selectbox("集計対象の列", numeric_fields.keys(), key="y_agg_field", index=None)
    if y_agg_type_name and y_field_display_name:
        aggregations.append([agg_map[y_agg_type_name], numeric_fields[y_field_display_name]])

    # X-Axis
    st.markdown("##### X軸の指標")
    cols_x = st.columns(2)
    x_agg_type_name = cols_x[0].selectbox("集約方法", agg_map.keys(), key="x_agg_type")
    x_field_display_name = cols_x[1].selectbox("集計対象の列", numeric_fields.keys(), key="x_agg_field", index=None)
    if x_agg_type_name and x_field_display_name:
        aggregations.append([agg_map[x_agg_type_name], numeric_fields[x_field_display_name]])

    # Breakout
    st.markdown("##### グループ化する列（オプション）")
    field_options = {f['display_name_with_table']: f['mbql_ref'] for f in all_fields}
    breakout_field_display_name = st.selectbox("グループ化する列", field_options.keys(), index=None, key="scatter_breakout_field_name")
    breakout_field_ref = field_options.get(breakout_field_display_name)
    
    return aggregations, breakout_field_ref

def display_custom_chart_form():
    """高機能クエリビルダーのUIを表示する"""
    selections = st.session_state.query_builder_selections
    with st.container(border=True):
        st.subheader("クエリビルダー")
        table_options = {tbl['display_name']: tbl['id'] for tbl in st.session_state.tables_metadata}
        st.selectbox("1. ベースとなるテーブルを選択", table_options.keys(), index=list(table_options.keys()).index(selections["table_name"]) if selections["table_name"] else None, on_change=handle_table_selection, key="selected_table_name_key", placeholder="テーブルを選択...")

        if selections["table_id"]:
            chart_display_name = st.selectbox("2. グラフの種類を選択", CHART_TYPE_MAP.keys(), key="chart_type_selection")
            st.markdown("---")
            st.markdown("**テーブル結合**"); display_existing_joins(selections); display_join_builder(selections)
            st.markdown("---")
            st.markdown("**フィルター**"); display_existing_filters(selections); display_add_filter_form(selections)
            st.markdown("---")
            st.markdown("**データ定義**")

            if chart_display_name == "散布図":
                aggregations, breakout_field_ref = display_scatter_plot_form(selections)
                agg_type_name, agg_field_ref = None, None
            else:
                charts_without_breakout = ["数値", "ゲージ"]
                show_breakout = chart_display_name not in charts_without_breakout
                agg_type_name, agg_field_ref, breakout_field_ref = display_aggregation_breakout_form(selections, show_breakout=show_breakout)
                aggregations = None

            st.markdown("---")
            if st.button("作成してダッシュボードに追加", type="primary"):
                agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
                handle_custom_chart_submission(
                    chart_display_name=chart_display_name, agg_type=agg_map.get(agg_type_name) if agg_type_name else None,
                    agg_field_ref=agg_field_ref, breakout_field_ref=breakout_field_ref, aggregations=aggregations
                )

    if st.button("ビルダーを閉じる"):
        st.session_state.show_custom_chart_form = False
        st.session_state.query_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": []}
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
    payload = {"resource": {"dashboard": int(dashboard_id)}, "params": {}, "exp": round(time.time()) + (60 * 10)}
    token = jwt.encode(payload, secret_key, algorithm="HS256")
    iframe_url = f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"
    st.components.v1.iframe(iframe_url, height=800, scrolling=True)

def main():
    st.set_page_config(layout="wide"); st.title("ダッシュボードビュー推薦システム (RotatE版)")
    if 'metabase_session_id' not in st.session_state: st.session_state.metabase_session_id = None
    if 'dashboard_id' not in st.session_state: st.session_state.dashboard_id = ""
    if 'secret_key' not in st.session_state: st.session_state.secret_key = ""
    if 'kge_model' not in st.session_state:
        st.session_state.kge_model, st.session_state.training_factory, st.session_state.relation_df = load_kge_model_and_data()
    if 'show_custom_chart_form' not in st.session_state: st.session_state.show_custom_chart_form = False
    if 'tables_metadata' not in st.session_state: st.session_state.tables_metadata = None
    if 'query_builder_selections' not in st.session_state:
        st.session_state.query_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": []}

    if st.session_state.metabase_session_id is None: display_credentials_form()
    else:
        embed_dashboard()
        st.header("データベースメタデータ")
        if st.button("Sample Databaseのテーブル一覧とメタデータを取得"):
            ids = get_db_and_table_ids(st.session_state.metabase_session_id)
            if ids and 'db_id' in ids:
                with st.spinner("テーブル情報を取得中..."):
                    st.session_state.tables_metadata = get_all_tables_metadata(st.session_state.metabase_session_id, ids['db_id'])
                if st.session_state.tables_metadata: st.success(f"'{len(st.session_state.tables_metadata)}' 個のテーブルが見つかりました。")
                else: st.warning("テーブルが見つかりませんでした。")
            else: st.error("データベースIDの取得に失敗したため、処理を中断しました。")
        st.header("カスタムグラフ作成")
        if st.button("📊 新しいグラフを対話的に作成する"):
            if st.session_state.tables_metadata is None:
                ids = get_db_and_table_ids(st.session_state.metabase_session_id)
                if ids and 'db_id' in ids:
                    with st.spinner("テーブル情報を読み込んでいます..."):
                        st.session_state.tables_metadata = get_all_tables_metadata(st.session_state.metabase_session_id, ids['db_id'])
            st.session_state.show_custom_chart_form = True; st.rerun()
        if st.session_state.show_custom_chart_form: display_custom_chart_form()
        st.header("ビュー推薦")
        dashboard_id = normalize_id(st.session_state.dashboard_id)
        if dashboard_id:
            dashboard_details = get_dashboard_details(st.session_state.metabase_session_id, dashboard_id)
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
                        if recommendations: st.success("おすすめのビューが見つかりました！"); st.write(recommendations)
                        else: st.info("推薦できるビューはありませんでした。")
                    else: st.warning("推薦の基となるビューがダッシュボードにありません。")
        st.header("サンプルチャート作成＆ダッシュボードに追加")
        st.selectbox('追加するカードのサイズを選択してください', list(SIZE_MAPPING.keys()), key='card_size_selection')
        ids = get_db_and_table_ids(st.session_state.metabase_session_id)
        if ids:
            col1, col2 = st.columns(2)
            if col1.button("棒グラフを作成＆追加"):
                card_size = SIZE_MAPPING.get(st.session_state.card_size_selection)
                payload = {"name": f"Sample Bar Chart - {int(time.time())}", "display": "bar", "dataset_query": {"type": "query", "database": ids['db_id'], "query": {"source-table": ids['table_id'], "aggregation": [["count"]], "breakout": [["field", ids['country_field_id'], None]]}}, "visualization_settings": {}}
                with st.spinner("作成中..."):
                    card_id = create_card(st.session_state.metabase_session_id, payload)
                    if card_id: success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
                    if card_id and success: st.success("追加しました！"); time.sleep(2); st.rerun()
            if col2.button("円グラフを作成＆追加"):
                card_size = SIZE_MAPPING.get(st.session_state.card_size_selection)
                payload = {"name": f"Sample Pie Chart - {int(time.time())}", "display": "pie", "dataset_query": {"type": "query", "database": ids['db_id'], "query": {"source-table": ids['table_id'], "aggregation": [["count"]], "breakout": [["field", ids['plan_field_id'], None]]}}, "visualization_settings": {}}
                with st.spinner("作成中..."):
                    card_id = create_card(st.session_state.metabase_session_id, payload)
                    if card_id: success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
                    if card_id and success: st.success("追加しました！"); time.sleep(2); st.rerun()

if __name__ == '__main__':
    main()