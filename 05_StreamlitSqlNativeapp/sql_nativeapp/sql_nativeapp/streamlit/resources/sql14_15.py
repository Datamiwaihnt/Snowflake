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

def execute_query13(warehouse,begin_str, end_str):
    
    df_query13 = session.call(
        "code_schema.localSpill13",
        warehouse,
        begin_str,
        end_str
    )

    rows = df_query13.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)

    df['SQL_COUNT'] = df['PERCENT_SQL_COUNT'].str.rstrip('%').astype(float)

    bar_order = [
        "1: 0 < SCAN_P_RATIO <= 1%",					
        "2: 1 < SCAN_P_RATIO <= 10%",					
        "3: 10 < SCAN_P_RATIO <= 30%", 					
        "4: 30 < SCAN_P_RATIO <= 60%",					
        "5: 60 < SCAN_P_RATIO <= 90%",					
        "6: 90% < SCAN_P_RATIO"
    ]

    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('SCAN_PARTITION_RATO_RANGE', sort=bar_order),
        x=alt.X('SQL_COUNT'),
        color='SCAN_PARTITION_RATO_RANGE',
        tooltip=['SCAN_PARTITION_RATO_RANGE', 'SQL_COUNT']
    ).properties(
        title="スキャンパーティション割合の傾向"
    )
    
    st.altair_chart(bar_chart, use_container_width=True)

    query_text_sql13 = """
        with query as (					
            select * from					
                (					
                    select					
                        warehouse_name,					
                        warehouse_size,					
                        COUNT(*) total_count_sql,					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 0  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 1   THEN 1 ELSE NULL END) AS "1: 0 < SCAN_P_RATIO <= 1%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 1  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 10  THEN 1 ELSE NULL END) AS "2: 1 < SCAN_P_RATIO <= 10%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 10 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 30  THEN 1 ELSE NULL END) AS "3: 10 < SCAN_P_RATIO <= 30%",   					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 30 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 60  THEN 1 ELSE NULL END) AS "4: 30 < SCAN_P_RATIO <= 60%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 60 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 90  THEN 1 ELSE NULL END) AS "5: 60 < SCAN_P_RATIO <= 90%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 90                                                    THEN 1 ELSE NULL END) AS "6: 90% < SCAN_P_RATIO"					
                    from					
                        snowflake.account_usage.query_history					
                    where					
                        execution_status = 'SUCCESS'					
                    and warehouse_name = '{warehouse}'					
                    and warehouse_size is not null					
                    and PARTITIONS_TOTAL > 0
                    and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
                    group by all					
                )					
            unpivot (sql_cnt for SCAN_PARTITION_RATO_RANGE in (					
                "1: 0 < SCAN_P_RATIO <= 1%",					
                "2: 1 < SCAN_P_RATIO <= 10%",					
                "3: 10 < SCAN_P_RATIO <= 30%", 					
                "4: 30 < SCAN_P_RATIO <= 60%",					
                "5: 60 < SCAN_P_RATIO <= 90%",					
                "6: 90% < SCAN_P_RATIO"					
            ))					
        )					
        select *,round(sql_cnt / total_count_sql * 100,1) ||'%' as "%SCAN_PARTITION_RATIO" from query;				
    """.format(warehouse=warehouse, begin_str=begin_str, end_str=end_str)

    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql13, language="sql")


def execute_query14(warehouse,begin_str, end_str):

    df_query14 = session.call(
        "code_schema.localSpill14",
        warehouse,
        begin_str,
        end_str
    ) 

    rows = df_query14.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)


    query_text_sql14 = """
        select 			
           warehouse_name,			
           warehouse_size,			
           query_id,			
           query_text,			
           round(PARTITIONS_SCANNED/PARTITIONS_TOTAL*100,2) "%SCAN_PARTITION_RATIO" ,			
           PARTITIONS_SCANNED ,			
           PARTITIONS_TOTAL   ,			
           round(BYTES_SCANNED/1024/1024/1024,2) BYTES_SCANNED_GB,			
           round(total_elapsed_time / 1000,2) total_elapsed_time_s			
        from			
            snowflake.account_usage.query_history			
        where			
            execution_status = 'SUCCESS'			
        and warehouse_name = '{warehouse}'			
        and warehouse_size is not null
        and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
        and (PARTITIONS_TOTAL > 0 and "%SCAN_PARTITION_RATIO" >= 90 and "%SCAN_PARTITION_RATIO" <= 100)			
        order by "%SCAN_PARTITION_RATIO" desc, PARTITIONS_SCANNED desc			

""".format(warehouse=warehouse, begin_str=begin_str, end_str=end_str)


    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql14, language="sql")


def main14():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab14")
    
    if st.button("クエリ実行", key="execute_query14"):
        execute_query13(warehouse, begin_str, end_str)

def main15():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab15")
    if st.button("クエリ実行", key="execute_query15"):
        execute_query14(warehouse, begin_str, end_str)

# タイトル表示
st.markdown("<h1 style='color:teal;'>クエリスキャンサイズ</h1>",unsafe_allow_html = True)
# タブUI
tab8, tab9 = st.tabs(["クエリスキャンサイズ範囲ごとのSQL数", "クエリスキャンサイズが多いSQL"])
with tab8:
    st.markdown("### クエリスキャンサイズ範囲ごとのSQL数",unsafe_allow_html = True)
    main14()
with tab9:
    st.markdown("### クエリスキャンサイズが多いSQL",unsafe_allow_html = True)
    main15()
