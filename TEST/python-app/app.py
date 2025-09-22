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

# --- Streamlit UI & メインロジック ---

def handle_table_selection():
    """テーブル選択が変更されたときに呼び出されるコールバック関数"""
    selections = st.session_state.query_builder_selections
    selected_table_name = st.session_state.get("selected_table_name_key")
    
    if selected_table_name:
        table_options = {tbl['display_name']: tbl for tbl in st.session_state.tables_metadata}
        selected_table = table_options[selected_table_name]
        selections.update({
            "table_id": selected_table['id'],
            "table_name": selected_table_name,
            "available_fields": selected_table.get('fields', []),
            "joins": [], "filters": [], "aggregation": [], "breakout_id": None
        })
    else:
        selections.update({"table_id": None, "table_name": None, "available_fields": [], "filters": []})

def handle_custom_chart_submission(agg_type, breakout_field_id, agg_field_id=None):
    """拡張されたフォーム情報からMBQLペイロードを構築し、APIを呼び出す"""
    selections = st.session_state.query_builder_selections
    table_id = selections['table_id']

    if not table_id or not breakout_field_id:
        st.error("テーブルとグループ化する列を選択してください。")
        return

    selected_table = next((tbl for tbl in st.session_state.tables_metadata if tbl['id'] == table_id), None)
    breakout_field = next((fld for fld in selections['available_fields'] if fld['id'] == breakout_field_id), None)

    query = {"source-table": table_id}

    field_required_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
    if agg_type in field_required_aggs:
        if not agg_field_id:
            st.error("この集約方法には集計対象の列が必要です。"); return
        query["aggregation"] = [[agg_type, ["field", agg_field_id, None]]]
    else: # count, cum-count
        query["aggregation"] = [[agg_type]]

    query["breakout"] = [["field", breakout_field_id, None]]

    if selections["filters"]:
        filter_clauses = []
        for f in selections["filters"]:
            clause = []
            op = f["operator"]
            field_clause = ["field", f["field_id"], None]
            
            if op in ["is-null", "not-null"]:
                clause = [op, field_clause]
            elif op == "between":
                try:
                    v1, v2 = float(f["value1"]), float(f["value2"])
                    clause = [op, field_clause, v1, v2]
                except (ValueError, TypeError):
                    st.error(f"フィルター「{f['field_name']}」の範囲指定の値が無効です。数値を入力してください。"); return
            else:
                try: value = float(f["value1"])
                except (ValueError, TypeError): value = f["value1"]
                clause = [op, field_clause, value]
            
            filter_clauses.append(clause)

        if len(filter_clauses) > 1:
            query["filter"] = ["and"] + filter_clauses
        elif filter_clauses:
            query["filter"] = filter_clauses[0]
            
    agg_field = next((fld for fld in selections['available_fields'] if fld['id'] == agg_field_id), None) if agg_field_id else None
    agg_str = f"の{agg_field['display_name']}" if agg_field else ""
    
    card_name = f"棒グラフ: {selected_table['display_name']}の{breakout_field['display_name']}別 {st.session_state.agg_type_name}{agg_str}"
    payload = {
        "name": card_name, "display": "bar",
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

def display_custom_chart_form():
    """高機能クエリビルダーのUIを表示する"""
    selections = st.session_state.query_builder_selections
    
    with st.container(border=True):
        st.subheader("クエリビルダー")

        table_options = {tbl['display_name']: tbl['id'] for tbl in st.session_state.tables_metadata}
        st.selectbox("1. ベースとなるテーブルを選択", table_options.keys(), index=list(table_options.keys()).index(selections["table_name"]) if selections["table_name"] else None, on_change=handle_table_selection, key="selected_table_name_key", placeholder="テーブルを選択...")

        if selections["table_id"]:
            st.markdown("---")
            st.markdown("**フィルター**")
            for i, f in enumerate(selections["filters"]):
                value_str = f"`{f['value1']}`" + (f" と `{f['value2']}`" if f.get('value2') is not None else "")
                cols = st.columns([4, 3, 3, 1])
                cols[0].info(f"`{f['field_name']}`")
                cols[1].info(f"{f['operator_name']}")
                cols[2].info(value_str)
                if cols[3].button("🗑️", key=f"del_filter_{i}", help="このフィルターを削除"):
                    selections["filters"].pop(i); st.rerun()
            
            with st.expander("＋ フィルターを追加する"):
                field_options = {f['display_name']: f for f in selections["available_fields"]}
                
                cols = st.columns(2)
                new_filter_field_name = cols[0].selectbox("列", field_options.keys(), index=None, key="new_filter_field")
                
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
                    if new_filter_field_name and new_filter_op_name:
                        selections["filters"].append({
                            "field_id": field_options[new_filter_field_name]['id'], "field_name": new_filter_field_name, 
                            "operator": operator_map[new_filter_op_name], "operator_name": new_filter_op_name, 
                            "value1": new_filter_value1, "value2": new_filter_value2
                        })
                        st.rerun()

            st.markdown("---")
            st.markdown("**集約**")
            cols = st.columns(2)
            agg_map = {"行のカウント": "count", "..の合計": "sum", "..の平均": "avg", "..の異なる値の数": "distinct", "..の累積合計": "cum-sum", "行の累積カウント": "cum-count", "..の標準偏差": "stddev", "..の最小値": "min", "..の最大値": "max"}
            agg_type_name = cols[0].selectbox("集約方法", agg_map.keys(), key="agg_type_name")
            agg_field_name = None
            
            field_required_aggs = ["sum", "avg", "distinct", "cum-sum", "stddev", "min", "max"]
            if agg_map[agg_type_name] in field_required_aggs:
                # ★★★ 修正箇所: ID関連フィールドを除外 ★★★
                numeric_fields = {
                    f['display_name']: f['id'] 
                    for f in selections["available_fields"] 
                    if any(t in f['base_type'].lower() for t in ['integer', 'float', 'double', 'decimal']) 
                    and f.get('semantic_type') not in ['type/PK', 'type/FK']
                }
                agg_field_name = cols[0].selectbox("集計対象の列", numeric_fields.keys(), key="agg_field", index=None)
            
            breakout_field_name = cols[1].selectbox("グループ化する列", field_options.keys(), index=None, key="breakout_field")

            st.markdown("---")
            if st.button("作成してダッシュボードに追加", type="primary"):
                agg_field_id = numeric_fields.get(agg_field_name) if 'numeric_fields' in locals() and agg_field_name else None
                breakout_field_id = field_options[breakout_field_name]['id'] if breakout_field_name else None
                handle_custom_chart_submission(agg_type=agg_map[agg_type_name], agg_field_id=agg_field_id, breakout_field_id=breakout_field_id)

    if st.button("ビルダーを閉じる"):
        st.session_state.show_custom_chart_form = False
        st.session_state.query_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": []}
        st.rerun()

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
    if 'show_custom_chart_form' not in st.session_state: st.session_state.show_custom_chart_form = False
    if 'tables_metadata' not in st.session_state: st.session_state.tables_metadata = None
    if 'query_builder_selections' not in st.session_state:
        st.session_state.query_builder_selections = {"table_id": None, "table_name": None, "joins": [], "filters": [], "aggregation": [], "breakout_id": None, "breakout_name": None, "available_fields": []}

    if st.session_state.metabase_session_id is None:
        display_credentials_form()
    else:
        embed_dashboard()
        
        st.header("データベースメタデータ")
        st.info("このセクションは、今後の機能開発のためのデモです。")
        
        if st.button("Sample Databaseのテーブル一覧とメタデータを取得"):
            ids = get_db_and_table_ids(st.session_state.metabase_session_id)
            if ids and 'db_id' in ids:
                with st.spinner("テーブル情報を取得中..."):
                    st.session_state.tables_metadata = get_all_tables_metadata(st.session_state.metabase_session_id, ids['db_id'])
                if st.session_state.tables_metadata:
                    st.success(f"'{len(st.session_state.tables_metadata)}' 個のテーブルが見つかりました。")
                else:
                    st.warning("テーブルが見つかりませんでした。")
            else:
                st.error("データベースIDの取得に失敗したため、処理を中断しました。")

        st.header("カスタム棒グラフ作成")
        if st.button("📊 新しい棒グラフを対話的に作成する"):
            if st.session_state.tables_metadata is None:
                ids = get_db_and_table_ids(st.session_state.metabase_session_id)
                if ids and 'db_id' in ids:
                    with st.spinner("テーブル情報を読み込んでいます..."):
                        st.session_state.tables_metadata = get_all_tables_metadata(st.session_state.metabase_session_id, ids['db_id'])
            st.session_state.show_custom_chart_form = True
            st.rerun()

        if st.session_state.show_custom_chart_form:
            display_custom_chart_form()
        
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
                        st.success("おすすめのビューが見つかりました！"); st.write(recommendations)
                    else:
                        st.info("推薦できるビューはありませんでした。")
                else:
                    st.warning("推薦の基となるビューがダッシュボードにありません。")

        st.header("サンプルチャート作成＆ダッシュボードに追加")
        st.write("`Sample Database`の`ACCOUNTS`テーブルからサンプルチャートを作成し、現在表示中のダッシュボードに追加します。")
        
        st.selectbox('追加するカードのサイズを選択してください', list(SIZE_MAPPING.keys()), key='card_size_selection')

        ids = get_db_and_table_ids(st.session_state.metabase_session_id)
        if ids:
            col1, col2 = st.columns(2)
            if col1.button("棒グラフを作成＆追加"):
                card_size = SIZE_MAPPING.get(st.session_state.card_size_selection)
                payload = {"name": f"Sample Bar Chart (Accounts by Country) - {int(time.time())}", "display": "bar", "dataset_query": {"type": "query", "database": ids['db_id'], "query": {"source-table": ids['table_id'], "aggregation": [["count"]], "breakout": [["field", ids['country_field_id'], None]]}}, "visualization_settings": {}}
                with st.spinner("棒グラフを作成中..."):
                    card_id = create_card(st.session_state.metabase_session_id, payload)
                if card_id:
                    with st.spinner("ダッシュボードに追加中..."):
                        success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
                    if success: st.success("ダッシュボードに追加しました！ページを更新します。"); time.sleep(2); st.rerun()

            if col2.button("円グラフを作成＆追加"):
                card_size = SIZE_MAPPING.get(st.session_state.card_size_selection)
                payload = {"name": f"Sample Pie Chart (Accounts by Plan) - {int(time.time())}", "display": "pie", "dataset_query": {"type": "query", "database": ids['db_id'], "query": {"source-table": ids['table_id'], "aggregation": [["count"]], "breakout": [["field", ids['plan_field_id'], None]]}}, "visualization_settings": {}}
                with st.spinner("円グラフを作成中..."):
                    card_id = create_card(st.session_state.metabase_session_id, payload)
                if card_id:
                    with st.spinner("ダッシュボードに追加中..."):
                        success = add_card_to_dashboard(st.session_state.metabase_session_id, dashboard_id, card_id, size_x=card_size['width'], size_y=card_size['height'])
                    if success: st.success("ダッシュボードに追加しました！ページを更新します。"); time.sleep(2); st.rerun()

if __name__ == '__main__':
    main()