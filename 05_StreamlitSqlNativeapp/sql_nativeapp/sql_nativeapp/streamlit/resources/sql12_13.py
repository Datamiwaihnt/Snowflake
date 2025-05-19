import streamlit as st
import pandas as pd
import datetime
from snowflake.snowpark.context import get_active_session
import altair as alt

session = get_active_session()

# ウェアハウス一覧を取得
@st.cache_data(show_spinner=False)
def show_warehouses():
    # ストアドプロシージャの呼び出し
    df = session.call("code_schema.show_warehouse_proc")  # Snowpark DataFrame
    rows = df.collect()                         # ⬅️ クエリを実行して行を取得
    rows_as_dict = [row.as_dict() for row in rows]  # ⬅️ 各行をdictに変換
    return pd.DataFrame(rows_as_dict)  # Pandas DataFrame に変換


# フィルター条件　main13用
def input_scan_range():
    
    minNumber = st.number_input("スキャンサイズの下限(GB)", value=0, placeholder="Type a number...")
    maxNumber = st.number_input("スキャンサイズの上限(GB)", value=10, placeholder="Type a number...")
    return minNumber,maxNumber

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

def execute_query11(warehouse,begin_str, end_str):
    
    df_query11 = session.call(
        "code_schema.localSpill11",
        warehouse,
        begin_str,
        end_str
    )

    rows = df_query11.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)

    df['SQL_COUNT'] = df['PERCENT_SQL_COUNT'].str.rstrip('%').astype(float)

    bar_order = [
        "1: 0B < SCAN_SIZE <= 1GB",				
        "2: 1GB < SCAN_SIZE <= 20GB",					
        "3: 20GB < SCAN_SIZE <= 50GB",					
        "4: 50GB < SCAN_SIZE"	
    ]

    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('SCAN_SIZE_RANGE', sort=bar_order),
        x=alt.X('SQL_COUNT'),
        color='SCAN_SIZE_RANGE',
        tooltip=['SCAN_SIZE_RANGE', 'SQL_COUNT']
    ).properties(
        title="クエリスキャンサイズ範囲ごとのSQL数"
    )
    
    st.altair_chart(bar_chart, use_container_width=True)

    query_text_sql11 = """
        with sqlcnt_per_scansize as (					
            select * from					
                (					
                    select
                       warehouse_name,					
                       warehouse_size,					
                       COUNT(*) total_count_sql,					
                       COUNT(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN 1 ELSE NULL END) AS "1: 0B < SCAN_SIZE <= 1GB",					
                       COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN 1 ELSE NULL END) AS "2: 1GB < SCAN_SIZE <= 20GB",   					
                       COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN 1 ELSE NULL END) AS "3: 20GB < SCAN_SIZE <= 50GB",					
                       COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN 1 ELSE NULL END)                                           AS "4: 50GB < SCAN_SIZE"					
                    from					
                        snowflake.account_usage.query_history					
                    where					
                        execution_status = 'SUCCESS'					
                    and warehouse_name = '{warehouse}'				
                    and warehouse_size is not null					
                    and BYTES_SCANNED > 0
                    and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
                    group by all					
                )					
            unpivot (sql_count for scan_size_range in (					
                "1: 0B < SCAN_SIZE <= 1GB",					
                "2: 1GB < SCAN_SIZE <= 20GB", 					
                "3: 20GB < SCAN_SIZE <= 50GB",					
                "4: 50GB < SCAN_SIZE"					
            ))					
            )					
        select *, round(sql_count / total_count_sql * 100,1) ||'%' as "%SQL_COUNT" from sqlcnt_per_scansize;					
    """.format(warehouse=warehouse, begin_str=begin_str, end_str=end_str)

    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql11, language="sql")


def execute_query12(warehouse,begin_str, end_str,minNumber,maxNumber):

    df_query12 = session.call(
        "code_schema.localSpill12",
        warehouse,
        begin_str,
        end_str,
        minNumber,
        maxNumber
    ) 

    rows = df_query12.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)


    query_text_sql12 = """
        select 
            warehouse_name,
            warehouse_size,
            query_id,
            query_text,
            round(BYTES_SCANNED/1024/1024/1024,2) BYTES_SCANNED_GB,
            round(total_elapsed_time / 1000,2) total_elapsed_time_s
        from
            snowflake.account_usage.query_history
        where
            execution_status = 'SUCCESS'
            and warehouse_name = '{warehouse}'
            and warehouse_size is not null
            and BYTES_SCANNED_GB > '{minNumber}'
            and BYTES_SCANNED_GB <= '{maxNumber}'
            and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'       
        order by BYTES_SCANNED_GB desc  
    ;
""".format(warehouse=warehouse, minNumber=minNumber, maxNumber=maxNumber, begin_str=begin_str, end_str=end_str)


    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql12, language="sql")


def main12():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab12")
    
    if st.button("クエリ実行", key="execute_query12"):
        execute_query11(warehouse, begin_str, end_str)

def main13():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab13")
    minNumber,maxNumber = input_scan_range()   
    if st.button("クエリ実行", key="execute_query13"):
        execute_query12(warehouse, begin_str, end_str,minNumber,maxNumber)

# タイトル表示
st.markdown("<h1 style='color:teal;'>クエリスキャンサイズ</h1>",unsafe_allow_html = True)
# タブUI
tab8, tab9 = st.tabs(["クエリスキャンサイズ範囲ごとのSQL数", "クエリスキャンサイズが多いSQL"])
with tab8:
    st.markdown("### クエリスキャンサイズ範囲ごとのSQL数",unsafe_allow_html = True)
    main12()
with tab9:
    st.markdown("### クエリスキャンサイズが多いSQL",unsafe_allow_html = True)
    main13()
