import streamlit as st
import pandas as pd
import jwt
import time

def set_config():
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

def test_dashboard():
    st.map()

def test_title():
    st.header("推薦システム")


def test_button():
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

def set_IFRAME_URL():
    st.text_input("Input metabase key", key='IFRAME_URL')


def embed_dashboard():
    st.components.v1.iframe(st.session_state.IFRAME_URL, height=800)



# メイン処理

if __name__ == '__main__':
    if 'main_controller' not in st.session_state:
        set_config()
        st.session_state.main_controller = True

    if 'IFRAME_URL' not in st.session_state:
        set_IFRAME_URL()
    else:
        test_title()
        embed_dashboard()
        test_button()



    

# You'll need to install PyJWT via pip 'pip install PyJWT' or your project packages file




#APIはセッション認証すること！
# curl -X POST \
#   -H "Content-Type: application/json" \
#   -d '{"username": "tilmit9831@gmail.com", "password": "password"}' \
#   http://localhost:3000/api/session

# curl -H "X-Metabase-Session: 99f14989-918a-413e-b33a-76447a75c856" -X GET http://localhost:3000/api/permissions/group


#やること　
# 1. セッション認証を自動で取ってくるようにする。
# 2. metabase container内だけではなく、Python container内でAPIを利用できるようにポートを開設？する
# 3. 特定のDashboardのcardリストをAPIで取得する

#カードリスト