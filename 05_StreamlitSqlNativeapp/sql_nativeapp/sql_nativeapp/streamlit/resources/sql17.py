import streamlit as st
import pandas as pd
import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# フィルター条件入力UI
def get_filter_inputs(key_suffix):
    today = datetime.date.today()
    first_day = today.replace(day=1)
    next_month = (today.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    last_day = next_month - datetime.timedelta(days=1)

    col2, col3 = st.columns(2)

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

    return begin_str, end_str

# クエリ実行 sql17
def execute_query17(begin_str, end_str):    

    df_query17 = session.call(
    "code_schema.localSpill16",
    begin_str,
    end_str
)
    

    rows = df_query17.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)

 
    query_text_sql17 = """

    with credits as (
    select 
        warehouse_name::varchar warehouse_name,
        round(sum(credits_used),1) as credits_used
    from snowflake.account_usage.warehouse_metering_history 
    where start_time between to_char(to_timestamp('{begin_str}'),'YYYY-MM-DD')::TIMESTAMP_LTZ  and to_char(to_timestamp('{end_str}'),'YYYY-MM-DD')::TIMESTAMP_LTZ
    group by warehouse_name
    ),
    query as(
    select
        warehouse_name,
        warehouse_size,
        AVG(CASE WHEN BYTES_SCANNED/1024/1024/1024 > 1 THEN bytes_scanned ELSE NULL END) AS avg_large ,
        COUNT(CASE WHEN BYTES_SCANNED/1024/1024/1024 > 1  THEN 1 ELSE NULL END) AS count_large ,
        COUNT(CASE WHEN BYTES_SCANNED/1024/1024/1024 <= 1  THEN 1 ELSE NULL END) AS count_small ,
        AVG(CASE WHEN BYTES_SCANNED/1024/1024/1024 > 1 THEN total_elapsed_time / 1000 ELSE NULL END) AS avg_large_exe_time ,
        AVG(bytes_scanned) AS avg_bytes_scanned ,
        AVG(total_elapsed_time)/ 1000 AS avg_elapsed_time ,
        AVG(execution_time)/ 1000 AS avg_execution_time ,
        COUNT(*) AS count_queries
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
    and BYTES_SCANNED > 0
    and total_elapsed_time > 0
    group by all
    )
    select 
        q.warehouse_name ,
        q.warehouse_size ,
        ROUND(count_large / count_queries * 100, 0) || '%' AS percent_large ,
        ROUND(count_small / count_queries * 100, 0) || '%' AS percent_small ,
        CASE
            WHEN avg_large >= POWER(2, 40) THEN to_char(ROUND(avg_large / POWER(2, 40), 1)) || ' TB'
            WHEN avg_large >= POWER(2, 30) THEN to_char(ROUND(avg_large / POWER(2, 30), 1)) || ' GB'
            WHEN avg_large >= POWER(2, 20) THEN to_char(ROUND(avg_large / POWER(2, 20), 1)) || ' MB'
            WHEN avg_large >= POWER(2, 10) THEN to_char(ROUND(avg_large / POWER(2, 10), 1)) || ' KB'
            ELSE to_char(avg_large)
        END AS avg_bytes_large ,
        ROUND(avg_large_exe_time) AS  "AVG_LARGE_EXE_TIME(s)",
        ROUND(avg_execution_time) AS  "AVG_ALL_EXE_TIME(s)",
        count_queries,
        ROUND(c.credits_used) as credits_used,
        to_char(to_timestamp('{begin_str}'),'YYYY-MM-DD') || ' - ' || to_char(to_timestamp('{end_str}'),'YYYY-MM-DD') "DATE PERIOD",
    from 
        query q
    inner join
        credits c
    on c.warehouse_name = q.warehouse_name
    order by
        case warehouse_size
            when 'X-Small' then 1
            when 'Small'   then 2
            when 'Medium'  then 3
            when 'Large'   then 4
            when 'X-Large' then 5
            when '2X-Large' then 6
            when '3X-Large' then 7
            when '4X-Large' then 8
            else 9
        end desc,
        c.credits_used desc;
    """.format(begin_str=begin_str, end_str=end_str)

    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql17, language="sql")


# WH全体分析(簡易版) sql17
def main17():
    begin_str, end_str = get_filter_inputs(key_suffix="tab17")

    if st.button("クエリ実行", key="execute_button_tab17"):
        execute_query17(begin_str, end_str)

# タイトル表示
st.markdown("<h1 style='color:teal;'>WH全体分析(簡易版)</h1>",unsafe_allow_html = True)
main17()
