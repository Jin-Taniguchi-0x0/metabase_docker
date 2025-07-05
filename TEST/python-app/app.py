import streamlit as st
import jwt
import time
import requests
import pandas as pd
import os
import pickle # For loading the KGE model
import torch # PyTorchモデルの読み込みに必要
from typing import List, Tuple, Dict, Any, Optional


METABASE_SITE_URL = "http://localhost:3000"
METABASE_API_URL = "http://metabase:3000"

# ハードコードされたカード表示タイプのマッピング
# card.get("display") の値をキーとし、表示したいview_nameを値とする
CARD_DISPLAY_TYPE_MAPPING = {
    "area": "visual-areaChart",
    "bar": "visual-barChart",
    "donut": "visual-donutChart",
    "funnel": "-",
    "gauge": "-",
    "line": "visual-lineChart",
    "pie": "visual-pieChart",
    "pivot-table": "visual-pivotTable", # Metabase APIのcard.displayの値と一致させる
    "map": "visual-map",
    "sunburst": "-",
    "scatter": "visual-scatterChart",
    "progress": "-",
    "row": "-",
    "table": "visual-table",
    "detail": "-",
    "number": "-",
    "combo": "-",
    # 必要に応じて、Metabaseがサポートする他の表示タイプと
    # 対応するview_nameをここに追加してください。
    # "-" を値として設定すると、マッピングせずに元の表示タイプ名が使用されます。
}

# スクリプトの場所を基準にマッピングファイルの絶対パスを構築
# 想定されるプロジェクト構造:
# project_root/
#   ├── python-app/
#   │   └── app.py
#   └── rsc/
#       └── viewMap.csv
try:
    # app.py が置かれているディレクトリの絶対パスを取得
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # print(f"DEBUG: _SCRIPT_DIR is: {_SCRIPT_DIR}") # Uncomment for debugging path issues
    # _SCRIPT_DIR から一つ上の階層に上がり、'rsc' ディレクトリ内の 'viewMap.csv' を指すパスを構築
    _MAPPING_FILE_ABSOLUTE_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "rsc", "viewMap.csv"))
except NameError:
    # __file__ が定義されていない場合 (例: 一部の対話型環境やデプロイメントシナリオ)
    # print("DEBUG: NameError occurred for __file__.") # Uncomment for debugging
    st.warning("スクリプトパスの自動検出に失敗しました (__file__ が未定義)。相対パス '../rsc/viewMap.csv' を使用します。Docker環境では問題が発生する可能性があります。")
    _MAPPING_FILE_ABSOLUTE_PATH = "../rsc/viewMap.csv" # 元の相対パス (フォールバック)

MAPPING_DIR = _MAPPING_FILE_ABSOLUTE_PATH
MODEL_PATH = "models/KGE_TransE_model.pkl" # Path within the Docker container


@st.cache_data
def load_mapping_data(mapping_file_path: str) -> Dict[str, str]:
    """
    カードタイトル名マッピング用のCSVファイルを読み込みます。
    CSVファイルには 'original_name' (Metabase上のカード名) と 
    'mapped_name' (表示したいカード名) の列が必要です。
    """
    try:
        df = pd.read_csv(mapping_file_path)
        if 'original_name' not in df.columns or 'mapped_name' not in df.columns:
            st.warning(
                f"マッピングファイル '{mapping_file_path}' には 'original_name' および "
                f"'mapped_name' 列が必要です。カードタイトルのマッピングは行われません。"
            )
            return {}
        mapping_dict = pd.Series(df.mapped_name.values, index=df.original_name).to_dict()
        return mapping_dict
    except FileNotFoundError:
        st.info(f"カードタイトルマッピングファイルが見つかりません: {mapping_file_path}。カードタイトルのマッピングは行われません。")
        return {}
    except Exception as e:
        st.error(f"カードタイトルマッピングファイルの読み込みに失敗しました: {e}")
        return {}


@st.cache_resource # Use st.cache_resource for models and other large objects
def load_kge_model(model_path: str) -> Optional[Any]:
    """KGEモデルをファイルから読み込みます。"""
    absolute_model_path = os.path.join(_SCRIPT_DIR if '_SCRIPT_DIR' in globals() else '.', model_path)
    # print(f"DEBUG: Attempting to load KGE model from: {absolute_model_path}") # Uncomment for debugging
    try:
        # モデルがPyTorchオブジェクトとして保存されている場合 (PyKeenモデルなど) は、
        # pickle.load() の代わりに torch.load() を使用します。
        # map_location=torch.device('cpu') は、GPUで訓練・保存されたモデルをCPU環境で読み込む際に役立ちます。
        # PyTorch 2.6以降、weights_onlyのデフォルトがTrueに変更されたため、Falseを明示的に指定する必要があります。
        # これは、信頼できるソースからのモデルファイルであることを前提としています。
        model = torch.load(absolute_model_path, map_location=torch.device('cpu'), weights_only=False)
        st.success(f"KGE推薦モデルの読み込みに成功しました (using torch.load): {absolute_model_path}")
        return model
    except pickle.UnpicklingError as e:
        st.error(
            f"モデルファイルのデシリアライズに失敗しました (Pickle/PyTorch): {e}\n"
            f"torch.load() を weights_only=False で試みましたが、ファイルが期待されるPyTorch形式でないか、"
            f"pickleのより深い問題（例: カスタムクラスの欠落、環境の不一致など）が発生している可能性があります。\n"
            f"モデルの保存方法と、実行環境に必要なライブラリ（特にモデル作成に使用したライブラリのバージョン）が揃っているか確認してください。"
        )
        if "Unsupported operand 149" in str(e):
            st.warning("エラーメッセージに 'Unsupported operand 149' が含まれています。これは、モデルの保存に使用されたPyTorchのバージョンと、現在の実行環境のPyTorchのバージョン間に互換性の問題がある可能性を示唆しています。")
        return None
    except FileNotFoundError:
        st.error(f"KGEモデルファイルが見つかりません: {absolute_model_path}")
        return None
    except Exception as e:
        st.error(f"KGEモデルの読み込み中に予期せぬエラーが発生しました: {e}")
        st.exception(e) # 詳細なエラー情報を表示
        return None


def set_app_config():
    """Streamlitページの基本設定を行います。"""
    st.set_page_config(
        page_title="推薦システムインターフェース テスト",
        page_icon="🖥️",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get Help': 'https://www.extremelycoolapp.com/help',
            'Report a bug': "https://www.extremelycoolapp.com/bug",
            'About': "# This is a header. This is an *extremely* cool app!"
        }
    )


def test_title():
    """アプリケーションのヘッダーを表示します。"""
    st.header("推薦システム")


def test_button():
    """テスト用のボタン群を表示します。"""
    stylesheet = """ <style> button { height: auto; padding-top: 40px !important; padding-bottom: 40px !important; } </style> """
    st.markdown(stylesheet, unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.button(':material/Show_Chart:', use_container_width=True)
    with col2:
        st.button(':material/Pie_Chart:', use_container_width=True)
    with col3:
        st.button(':material/Stacked_Bar_Chart:', use_container_width=True)
    with col4:
        st.button(':material/Bar_Chart:', use_container_width=True)


def display_credentials_form():
    """Metabaseの認証情報とダッシュボードIDを入力するフォームを表示します。"""
    with st.form("secret_and_credentials_form"):
        secret = st.text_input("Metabase JWT秘密鍵", type="password")
        username = st.text_input("Metabase ユーザー名")
        password = st.text_input("Metabase パスワード", type="password")
        dashboard_id = st.number_input("ダッシュボードID", min_value=1, step=1, value=1)
        submitted = st.form_submit_button("保存する")

        if submitted and secret and username and password:
            # Save inputs to session state
            st.session_state.METABASE_SECRET_KEY = secret
            st.session_state.METABASE_USERNAME = username
            st.session_state.METABASE_PASSWORD = password
            st.session_state.METABASE_DASHBOARD_ID = int(dashboard_id)

            # Generate JWT token
            payload = {
                "resource": {"dashboard": int(dashboard_id)},
                "params": {},
                "exp": round(time.time()) + (60 * 60)  # 1 hour expiry
            }
            token = jwt.encode(payload, secret, algorithm="HS256")
            if isinstance(token, bytes):
                token = token.decode("utf-8")

            # Store iframe URL
            st.session_state.IFRAME_URL = f"{METABASE_SITE_URL}/embed/dashboard/{token}#bordered=true&titled=true"
            st.rerun()


def embed_dashboard():
    """Metabaseダッシュボードをiframeで埋め込み表示します。"""
    st.components.v1.iframe(st.session_state.IFRAME_URL, height=800)


def get_metabase_session(username: str, password: str) -> Optional[str]:
    url = f"{METABASE_API_URL}/api/session"
    # print(f"DEBUG: Getting session from {url} for user {username}") # Uncomment for debugging
    response = requests.post(url, json={"username": username, "password": password})
    if response.status_code == 200:
        return response.json()["id"]
    else:
        raise Exception(f"セッション取得に失敗しました: {response.status_code} - {response.text}")


def get_dashboard_card_info(session_id: str, dashboard_id: int, title_mapping_data: Dict[str, str]) -> List[Tuple[int, str, str]]:
    """
    ダッシュボードに含まれるカードの (id, name, display) を一覧取得

    Returns:
        List[Tuple[int, str, str]]
        title_mapping_data (dict): A dictionary for mapping original card titles to new titles.
    """
    url = f"{METABASE_API_URL}/api/dashboard/{dashboard_id}"
    headers = {"X-Metabase-Session": session_id}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        st.error(f"ダッシュボード取得に失敗しました（{response.status_code}）")
        return []

    data = response.json()
    cards = []

    for item in data.get("dashcards", []):
        card = item.get("card")
        if card and card.get("id") is not None:
            card_id = card["id"]
            original_card_name = card.get("name", "(No Name)")
            # Apply mapping: Use mapped name if exists, otherwise use original name
            card_name = title_mapping_data.get(original_card_name, original_card_name)

            original_card_display_type = card.get("display", "(Unknown)")

            # ハードコードされたマッピングで表示タイプを変換
            # マッピングが存在し、かつ値が"-"でない場合に変換後の値を採用
            mapped_display_type = CARD_DISPLAY_TYPE_MAPPING.get(original_card_display_type)

            if mapped_display_type and mapped_display_type != "-":
                final_card_display_type = mapped_display_type
            else: # mapped_display_type が None であるか、または "-" の場合
                if mapped_display_type == "-": # 表示タイプのマッピングが "-" の場合
                    final_card_display_type = f"##{original_card_display_type}##" # 元の表示タイプを##で囲む
                else: # マッピングが存在しない場合
                    final_card_display_type = original_card_display_type
            cards.append((card_id, card_name, final_card_display_type))

    return cards

def display_dashboard_cards_info():
    """現在のダッシュボードのカード情報を取得し表示します。"""
    # Load card title mapping data
    title_mapping_data = load_mapping_data(MAPPING_DIR)

    try:
        # Ensure necessary session state variables exist before proceeding
        if not all(st.session_state.get(key) for key in ['METABASE_USERNAME', 'METABASE_PASSWORD', 'METABASE_DASHBOARD_ID']):
            st.warning("Metabaseの認証情報またはダッシュボードIDが設定されていません。フォームから入力してください。")
            return

        session_id = get_metabase_session(
            st.session_state.METABASE_USERNAME,
            st.session_state.METABASE_PASSWORD
        )
        cards = get_dashboard_card_info(session_id, st.session_state.METABASE_DASHBOARD_ID, title_mapping_data)
        if cards:
            st.success(f"{len(cards)} 件のカードを取得しました：")
            for i, (cid, cname, ctype) in enumerate(cards, 1):
                st.write(f"{i}. ID: {cid} ｜タイトル: {cname} ｜タイプ: {ctype}")
        else:
            st.warning("カードが見つかりませんでした。")
    except Exception as e:
        st.error(f"カード情報の表示中にエラーが発生しました: {e}")


def display_view_recommendations(kge_model: Any, current_cards: List[Tuple[int, str, str]]):
    """KGEモデルを使ってビューの推薦を表示します。"""
    if kge_model is None:
        st.warning("推薦モデルがロードされていないため、ビューの推薦は利用できません。")
        return

    st.subheader("次に試すビューの推薦")

    if not current_cards:
        st.info("現在ダッシュボードにカードがありません。推薦の基準となるカードを選択してください。")
        # TODO: もしカードがない場合でも推薦できるロジックがあればここに追加
        return

    # --- ここからモデルの仕様に合わせた処理 ---
    # KGEモデル (特にPyKeenで訓練されたモデル) への入力形式を準備します。
    # 例: 現在のカードID (エンティティID) のリスト、または特定の関係タイプ。
    # モデルが期待する入力形式に合わせてこの部分を実装する必要があります。
    
    current_card_ids = [card[0] for card in current_cards]
    # current_card_names = [card[1] for card in current_cards] # 必要に応じて名前も使用

    try:
        # 例: PyKeenモデルの場合、以下のような予測方法が考えられます。
        # (これはあくまで一般的な例であり、実際のモデルのAPIに合わせてください)
        #
        # if hasattr(kge_model, 'predict_tails'):
        #     # head_ids = torch.tensor(current_card_ids)
        #     # relation_ids = torch.tensor([RELATION_ID_FOR_NEXT_VIEW]) # "次のビュー" に対応する関係ID
        #     # scores_df = kge_model.predict_tails(head_ids=head_ids, relation_ids=relation_ids, ...)
        #     # recommended_view_ids = scores_df.sort_values(by='score', ascending=False)['tail_id'].tolist()[:5]
        #     pass
        # elif hasattr(kge_model, 'get_recommendations'): # カスタムメソッドの場合
        #     # recommended_view_ids = kge_model.get_recommendations(current_card_ids, top_n=5)
        #     pass
        # else:
        #     st.warning("モデルに適切な予測メソッドが見つかりません。")
        #     return

        # --- ダミーの推薦ロジック (実際のモデル呼び出しに置き換えてください) ---
        st.markdown(" **以下の推薦はダミーです。実際のモデルロジックに置き換えてください。** ")
        if current_card_ids:
            # ダミー: 現在のカードIDに基づいて単純な推薦を生成
            dummy_recommendations = []
            for i, cid in enumerate(current_card_ids[:1]): # 最初のカードを基準にする
                 dummy_recommendations.extend([
                     (cid + 100 + i, f"推薦ビュー {cid + 100 + i} (ダミー)"),
                     (cid + 200 + i, f"推薦ビュー {cid + 200 + i} (ダミー)"),
                 ])
            recommended_views_info = [f"ID: {rec_id} - {rec_name}" for rec_id, rec_name in dummy_recommendations[:3]] # 上位3件
        else:
            recommended_views_info = ["推薦の基準となるカードがありません。"]
        # --- ダミーの推薦ロジックここまで ---

        if recommended_views_info:
            st.write("このダッシュボードに次に追加すると良いかもしれないビュー:")
            for rec_info in recommended_views_info:
                st.markdown(f"- {rec_info}")
        else:
            st.info("現時点では、このダッシュボードへの推薦はありません。")

    except Exception as e:
        st.error(f"推薦の生成中にエラーが発生しました: {e}")
        st.exception(e) # 詳細なエラー情報を表示


if __name__ == '__main__':
    # アプリケーションの初期設定（初回実行時のみ）
    if 'app_initialized' not in st.session_state:
        set_app_config()
        st.session_state.app_initialized = True

    # 必要な情報がセッションに保存されているか確認
    required_keys = ['IFRAME_URL', 'METABASE_USERNAME', 'METABASE_PASSWORD', 'METABASE_DASHBOARD_ID']
    if not all(st.session_state.get(key) for key in required_keys):
        display_credentials_form()
    else:
        # 認証情報とダッシュボードIDが設定されていればメインコンテンツを表示
        test_title()
        embed_dashboard()
        test_button()
        display_dashboard_cards_info()

        # KGEモデルをロード
        kge_model = load_kge_model(MODEL_PATH)
        
        if kge_model and st.session_state.get('METABASE_DASHBOARD_ID'):
            # 現在のカード情報を取得して推薦関数に渡す
            # 注意: display_dashboard_cards_info でもカード情報を取得しています。
            # パフォーマンス向上のため、結果を st.session_state に保存して再利用することを検討できます。
            # ここでは、明確性のために再度取得する形にしています。
            try:
                session_id_for_rec = get_metabase_session(
                    st.session_state.METABASE_USERNAME,
                    st.session_state.METABASE_PASSWORD
                )
                title_mapping_data_for_rec = load_mapping_data(MAPPING_DIR)
                current_dashboard_cards = get_dashboard_card_info(
                    session_id_for_rec,
                    st.session_state.METABASE_DASHBOARD_ID,
                    title_mapping_data_for_rec
                )
                display_view_recommendations(kge_model, current_dashboard_cards)
            except Exception as e:
                st.error(f"推薦表示の準備中にエラーが発生しました: {e}")
                st.exception(e)