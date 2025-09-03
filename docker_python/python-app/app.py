import streamlit as st
import pandas as pd

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
    st.header("これはインターフェースのテストになります")


def test_button():
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.button('😃')
    with col2:
        st.button('🥶')
    with col3:
        st.button('🥵')
    with col4:
        st.button('😈')

# メイン処理

if __name__ == '__main__':
    if 'main_controller' not in st.session_state:
        set_config()
        st.session_state.main_controller = True

    test_title()
    test_dashboard()
    test_button()

