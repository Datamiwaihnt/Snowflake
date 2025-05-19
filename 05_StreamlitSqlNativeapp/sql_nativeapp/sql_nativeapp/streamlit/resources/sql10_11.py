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

def execute_query9(warehouse,begin_str, end_str):
    
    df_query9 = session.call(
        "code_schema.localSpill9",
        warehouse,
        begin_str,
        end_str
    )

    rows = df_query9.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)

    df['SQL_COUNT'] = df['PERCENT_SQL_COUNT'].str.rstrip('%').astype(float)

    bar_order = [
        "1: 0s < ELAPSED_TIME <= 1s",
        "2: 1s < ELAPSED_TIME <= 10s",
        "3: 10s < ELAPSED_TIME <= 60s",
        "4: 60s < ELAPSED_TIME <= 600s",
        "5: 600s < ELAPSED_TIME <= 3600s",
        "6: 3600s < ELAPSED_TIME"
    ]

    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('ELAPSED_TIME_RANGE', sort=bar_order),
        x=alt.X('SQL_COUNT'),
        color='ELAPSED_TIME_RANGE',
        tooltip=['ELAPSED_TIME_RANGE', 'SQL_COUNT']
    ).properties(
        title="TXブロック待ち発生状況"
    )
    
    st.altair_chart(bar_chart, use_container_width=True)

    query_text_sql9 = """
          with sqlcnt_per_range as (
            select * from (
                select
                    warehouse_name,
                    warehouse_size,
                    count(*) total_count_sql,
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 0   and (total_elapsed_time / 1000) <= 1 THEN 1 ELSE NULL END)      AS "1: 0s < ELAPSED_TIME <= 1s", 
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 1   and (total_elapsed_time / 1000) <= 10 THEN 1 ELSE NULL END)     AS "2: 1s < ELAPSED_TIME <= 10s", 
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 10  and (total_elapsed_time / 1000) <= 60 THEN 1 ELSE NULL END)     AS "3: 10s < ELAPSED_TIME <= 60s",
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 60  and (total_elapsed_time / 1000) <= 600 THEN 1 ELSE NULL END)    AS "4: 60s < ELAPSED_TIME <= 600s",
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 600 and (total_elapsed_time / 1000) <= 3600 THEN 1 ELSE NULL END)   AS "5: 600s < ELAPSED_TIME <= 3600s",
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 3600 THEN 1 ELSE NULL END) AS "6: 3600s < ELAPSED_TIME", 
                from
                    snowflake.account_usage.query_history
                where
                    execution_status = 'SUCCESS'
                and warehouse_name = '{warehouse}'
                and warehouse_size is not null
                and total_elapsed_time > 0
                and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_time}' AND '{end_time}'
                group by all
            
            )
            unpivot (sql_count for elapsed_time_range in (
                "1: 0s < ELAPSED_TIME <= 1s",
                "2: 1s < ELAPSED_TIME <= 10s",
                "3: 10s < ELAPSED_TIME <= 60s",
                "4: 60s < ELAPSED_TIME <= 600s",
                "5: 600s < ELAPSED_TIME <= 3600s",
                "6: 3600s < ELAPSED_TIME"
            ))
        )

        select 
            *,
            round(sql_count / total_count_sql*100,3) || '%' as "%SQL_COUNT"
        from 
            sqlcnt_per_range;  
    """.format(warehouse=warehouse, begin_str=begin_str, end_str=end_str)

    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql9, language="sql")


def execute_query10(warehouse,begin_str, end_str):

    df_query10 = session.call(
        "code_schema.localSpill10",
        warehouse,
        begin_str,
        end_str
    ) 

    rows = df_query10.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)


    query_text_sql10 = """
        select
            warehouse_name,				
            warehouse_size,				
            query_id,				
            query_text,				
            CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,				
            round(total_elapsed_time/1000,1) total_elapsed_time_s				
        from
            snowflake.account_usage.query_history				
        where				
            execution_status = 'SUCCESS'				
            and warehouse_name = '{warehouse}'				
            and warehouse_size is not null				
            and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
            and total_elapsed_time_s > 0				
        order by total_elapsed_time_s desc				
    ;
""".format(warehouse=warehouse, begin_str=begin_str, end_str=end_str)


    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql10, language="sql")

  

def main10():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab10")
    
    if st.button("クエリ実行", key="execute_query10"):
        execute_query9(warehouse, begin_str, end_str)

def main11():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab11")   
    if st.button("クエリ実行", key="execute_query11"):
        execute_query10(warehouse, begin_str, end_str)

# タイトル表示
st.markdown("<h1 style='color:teal;'>クエリ実行時間</h1>",unsafe_allow_html = True)
# タブUI
tab8, tab9 = st.tabs(["クエリ実行時間範囲ごとのSQL数", "クエリ実行時間が長いSQL"])
with tab8:
    st.markdown("### クエリ実行時間範囲ごとのSQL数",unsafe_allow_html = True)
    main10()
with tab9:
    st.markdown("### クエリ実行時間が長いSQL",unsafe_allow_html = True)
    main11()
