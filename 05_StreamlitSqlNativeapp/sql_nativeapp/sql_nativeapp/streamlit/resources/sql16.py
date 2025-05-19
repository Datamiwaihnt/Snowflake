import streamlit as st
import pandas as pd
import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# ウェアハウス一覧を取得
@st.cache_data
def show_warehouses():
    query = "show warehouses"
    exe = session.sql(query).collect()
    return exe

# フィルター条件入力UI
def get_filter_inputs(warehouse_name, key_suffix):
    today = datetime.date.today()
    first_day = today.replace(day=1)
    next_month = (today.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    last_day = next_month - datetime.timedelta(days=1)

    col1, col2, col3 = st.columns(3)

    with col1:
        warehouse = st.selectbox(
            "ウェアハウスを選択",
            warehouse_name,
            key=f'warehouse_selectbox_sql_{key_suffix}'
        )
    with col2:
        begin_date = st.date_input("開始日", value=first_day, key=f"begin_date_{key_suffix}")
        begin_time = st.time_input("開始時刻", value=datetime.time(0, 0), key=f"begin_time_{key_suffix}")

    with col3:
        end_date = st.date_input("終了日", value=last_day, key=f"end_date_{key_suffix}")
        end_time = st.time_input("終了時刻", value=datetime.time(23, 59), key=f"end_time_{key_suffix}")

    begin_dt = datetime.datetime.combine(begin_date, begin_time)
    end_dt = datetime.datetime.combine(end_date, end_time)

    begin_str = begin_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    return warehouse, begin_str, end_str

# クエリ実行 sql16
def execute_query16(warehouse, begin_str, end_str):    

    df_query16 = session.call(
    "code_schema.localSpill15",
    warehouse,
    begin_str,
    end_str
)
    

    rows = df_query16.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)    
 

# Query Acceleration Service sql16
def main16():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab16")

    if st.button("クエリ実行", key="execute_button_tab16"):
        execute_query16(warehouse, begin_str, end_str)

# タイトル表示
st.markdown("<h1 style='color:teal;'>Query Acceleration Service</h1>",unsafe_allow_html = True)
main16()
