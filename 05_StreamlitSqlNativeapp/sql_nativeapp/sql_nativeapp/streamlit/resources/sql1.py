import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd


# UI
st.markdown("<h1 style='color:teal;'>WH設定</h1>",unsafe_allow_html = True)
st.write("")

st.markdown("### 🔳 確認内容")
st.markdown("- アカウント内のすべてのウェアハウスをリストします")  

st.markdown("### 🔳 クエリ実行後の確認観点")
st.markdown("- ウェアハウスのsize等が設計書通りであること")  

check_items = {
        "No": [1, 2, 3, 4],
    "確認項目": [
        "WHのサイズ",
        "WHのクラスタ数",
        "WHのオートスケール設定",
        "WHの自動停止設定（AUTO_SUSPEND）"
    ],
    "観点": [
        "設計書通りであること",
        "設計書通りであること",
        "意図通りに設定されているか",
        "無駄に稼働し続けないよう設定されているか"
    ]
}
st.dataframe(pd.DataFrame(check_items), hide_index=True)
st.write("")

# セッション取得
session = get_active_session()

def execute_proc():
    try:
        # Pythonストアドプロシージャを呼び出して結果をDataFrameで受け取る
        df = session.call("code_schema.show_warehouse_proc")
        st.dataframe(df)  # 横に広いので dataframe 推奨

        with st.expander("実行SQL", expanded=False):
            st.code("SHOW WAREHOUSES;")
    except Exception as e:
        st.error(f"エラーが発生しました: {e}")


# ボタンで実行
if st.button("クエリ実行"):
    execute_proc()
