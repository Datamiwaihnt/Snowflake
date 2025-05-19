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

# クエリ実行 sql18
def execute_query18(begin_str, end_str):    

    df_query18 = session.call(
    "code_schema.localSpill17",
    begin_str,
    end_str
)

    rows = df_query18.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)


    query_text_sql18 = """
     with credits as (
    select 
        warehouse_name::varchar warehouse_name,
        round(sum(credits_used),1) as credits_used
    from snowflake.account_usage.warehouse_metering_history 
    where start_time between to_char(to_timestamp('{begin_str}'),'YYYY-MM-DD')::TIMESTAMP_LTZ  and to_char(to_timestamp('{end_str}'),'YYYY-MM-DD')::TIMESTAMP_LTZ
    group by warehouse_name
    ),
    qas as(
    SELECT 
        warehouse_name,
        warehouse_size,
        count(*) total_eligible_qas_sql_count,
        round(avg(eligible_query_acceleration_time),2)    AS "avg_eligible_query_acceleration_time(s)",
        round(median(eligible_query_acceleration_time),2) AS "mdn_eligible_query_acceleration_time(s)",
        sum(eligible_query_acceleration_time)    AS "sum_eligible_query_acceleration_time(s)",
        max(eligible_query_acceleration_time)    AS "max_eligible_query_acceleration_time(s)"
    FROM 
        snowflake.account_usage.QUERY_ACCELERATION_ELIGIBLE
    where 
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' and '{end_str}'
    group by all
    ),
    query as(
    select
        warehouse_name,
        warehouse_size,
        round(avg(BYTES_SCANNED/1024/1024/1024),2)                      AS "AVG_SCAN_SIZE(GB)",
        round(median(BYTES_SCANNED/1024/1024/1024),2)                   AS "MDN_SCAN_SIZE(GB)",
        round(sum(BYTES_SCANNED/1024/1024/1024),2)                      AS "SUM_SCAN_SIZE(GB)",
        round(max(BYTES_SCANNED/1024/1024/1024),2)                      AS "MAX_SCAN_SIZE(GB)",
        round(avg(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024),2)     AS "AVG_LOCAL_SPILLED_SIZE(GB)",
        round(median(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024),2)  AS "MDN_LOCAL_SPILLED_SIZE(GB)",
        round(sum(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024),2)     AS "SUM_LOCAL_SPILLED_SIZE(GB)",
        round(max(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024),2)     AS "MAX_LOCAL_SPILLED_SIZE(GB)",
        round(avg(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024),2)    AS "AVG_REMOTE_SPILLED_SIZE(GB)",
        round(median(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024),2) AS "MDN_REMOTE_SPILLED_SIZE(GB)",
        round(sum(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024),2)    AS "SUM_REMOTE_SPILLED_SIZE(GB)",
        round(max(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024),2)    AS "MAX_REMOTE_SPILLED_SIZE(GB)",
        round(avg(total_elapsed_time)/1000,2)                           AS "AVG_ELAPSED_TIME(s)",
        round(median(total_elapsed_time)/1000,2)                        AS "MDN_ELAPSED_TIME(s)",
        round(sum(total_elapsed_time)/1000,2)                           AS "SUM_ELAPSED_TIME(s)",
        round(sum(total_elapsed_time)/1000/3600,2)                      AS "SUM_ELAPSED_TIME(h)",
        round(max(total_elapsed_time)/1000,2)                           AS "MAX_ELAPSED_TIME(s)",
        round(avg(QUEUED_OVERLOAD_TIME)/1000,2)                         AS "AVG_QUEUED_TIME(s)",
        round(median(QUEUED_OVERLOAD_TIME)/1000,2)                      AS "MDN_QUEUED_TIME(s)",
        round(sum(QUEUED_OVERLOAD_TIME)/1000,2)                         AS "SUM_QUEUED_TIME(s)",
        round(max(QUEUED_OVERLOAD_TIME)/1000,2)                         AS "MAX_QUEUED_TIME(s)",
        round(avg(TRANSACTION_BLOCKED_TIME)/1000,2)                     AS "AVG_TXBLOCKED_TIME(s)",
        round(median(TRANSACTION_BLOCKED_TIME)/1000,2)                  AS "MDN_TXBLOCKED_TIME(s)",
        round(sum(TRANSACTION_BLOCKED_TIME)/1000,2)                     AS "SUM_TXBLOCKED_TIME(s)",
        round(max(TRANSACTION_BLOCKED_TIME)/1000,2)                     AS "MAX_TXBLOCKED_TIME(s)",
        round(avg(PARTITIONS_SCANNED/PARTITIONS_TOTAL*100),2)           AS "AVG_SCAN_PARTITION_RATIO(%)",
        round(median(PARTITIONS_SCANNED/PARTITIONS_TOTAL*100),2)        AS "MDN_SCAN_PARTITION_RATIO(%)",
        count(*) count_total_sql,
        -- SQL数：スキャンサイズレンジ
        COUNT(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN 1 ELSE NULL END) AS "CNTSQL: 0B < SCAN_SIZE <= 1GB",
        COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN 1 ELSE NULL END) AS "CNTSQL: 1GB < SCAN_SIZE <= 20GB",   
        COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN 1 ELSE NULL END) AS "CNTSQL: 20GB < SCAN_SIZE <= 50GB",
        COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN 1 ELSE NULL END)                                           AS "CNTSQL: 50GB < SCAN_SIZE",
        -- SQL数：ローカルスピルサイズレンジ
        COUNT(CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE)                = 0  THEN 1 ELSE NULL END)                                                                AS "CNTSQL: LOCAL_SPILLED_SIZE = 0B", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE)                > 0  and (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024)       <= 1 THEN 1 ELSE NULL END)      AS "CNTSQL: 0B < LOCAL_SPILLED_SIZE <= 1MB",   
        COUNT(CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024)      > 1  and (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024)  <= 1 THEN 1 ELSE NULL END)      AS "CNTSQL: 1MB < LOCAL_SPILLED_SIZE <= 1GB", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024) > 1  and (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024)  <= 10 THEN 1 ELSE NULL END)     AS "CNTSQL: 1GB < LOCAL_SPILLED_SIZE <= 10GB", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024) > 10  and (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024) <= 100 THEN 1 ELSE NULL END)    AS "CNTSQL: 10GB < LOCAL_SPILLED_SIZE <= 100GB", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024) > 100 and (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024/1024) <= 1 THEN 1 ELSE NULL END) AS "CNTSQL: 100GB < LOCAL_SPILLED_SIZE <= 1TB",
        COUNT(CASE WHEN (BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024/1024) > 1 THEN 1 ELSE NULL END)                                                            AS "CNTSQL: 1TB < LOCAL_SPILLED_SIZE",
        -- SQL数：リモートスピルサイズレンジ
        COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                = 0  THEN 1 ELSE NULL END)                                                                 AS "CNTSQL: REMOTE_SPILLED_SIZE = 0B", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                > 0  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)       <= 1 THEN 1 ELSE NULL END)      AS "CNTSQL: 0B < REMOTE_SPILLED_SIZE <= 1MB",   
        COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)      > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 1 THEN 1 ELSE NULL END)      AS "CNTSQL: 1MB < REMOTE_SPILLED_SIZE <= 1GB", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 10 THEN 1 ELSE NULL END)     AS "CNTSQL: 1GB < REMOTE_SPILLED_SIZE <= 10GB", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 10  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) <= 100 THEN 1 ELSE NULL END)    AS "CNTSQL: 10GB < REMOTE_SPILLED_SIZE <= 100GB", 
        COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 100 and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) <= 1 THEN 1 ELSE NULL END) AS "CNTSQL: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
        COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) > 1 THEN 1 ELSE NULL END)                                                             AS "CNTSQL: 1TB < REMOTE_SPILLED_SIZE",
        -- SQL数：クエリ実行時間レンジ
        COUNT(CASE WHEN (total_elapsed_time / 1000) > 0   and (total_elapsed_time / 1000) <= 1 THEN 1 ELSE NULL END)      AS "CNTSQL: 0s < ELAPSED_TIME <= 1s",  
        COUNT(CASE WHEN (total_elapsed_time / 1000) > 1   and (total_elapsed_time / 1000) <= 10 THEN 1 ELSE NULL END)     AS "CNTSQL: 1s < ELAPSED_TIME <= 10s", 
        COUNT(CASE WHEN (total_elapsed_time / 1000) > 10  and (total_elapsed_time / 1000) <= 60 THEN 1 ELSE NULL END)     AS "CNTSQL: 10s < ELAPSED_TIME <= 60s",   
        COUNT(CASE WHEN (total_elapsed_time / 1000) > 60  and (total_elapsed_time / 1000) <= 600 THEN 1 ELSE NULL END)    AS "CNTSQL: 60s < ELAPSED_TIME <= 600s",   
        COUNT(CASE WHEN (total_elapsed_time / 1000) > 600 and (total_elapsed_time / 1000) <= 3600 THEN 1 ELSE NULL END)   AS "CNTSQL: 600s < ELAPSED_TIME <= 3600s",  
        COUNT(CASE WHEN (total_elapsed_time / 1000) > 3600 THEN 1 ELSE NULL END)                                          AS "CNTSQL: 3600s < ELAPSED_TIME",
        -- SQL数：キュー待ち時間割合レンジ
        COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                               AS "CNTSQL: ELAPSED_TIME_QUEUED% = 0%",
        COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0      and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "CNTSQL: 0% < ELAPSED_TIME_QUEUED% <= 1%",
        COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.01   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "CNTSQL: 1% < ELAPSED_TIME_QUEUED% <= 5%",
        COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.05   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "CNTSQL: 5% < ELAPSED_TIME_QUEUED% <= 20%",
        COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.2    and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "CNTSQL: 20% < ELAPSED_TIME_QUEUED% <= 50%",
        COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                             AS "CNTSQL: 50% < ELAPSED_TIME_QUEUED%",
        -- SQL数：トランザクションブロック時間割合レンジ
        COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                                   AS "CNTSQL: ELAPSED_TIME_TXBLOCKED% = 0%",
        COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0      and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "CNTSQL: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
        COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.01   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "CNTSQL: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
        COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.05   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "CNTSQL: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
        COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.2    and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "CNTSQL: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
        COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                                 AS "CNTSQL: 50% < ELAPSED_TIME_TXBLOCKED%",
        -- SQL数：スキャンパーティション割合レンジ
        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 0  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 1   THEN 1 ELSE NULL END) AS "CNTSQL: 0 < SCAN_P_RATIO <= 1%",
        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 1  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 10  THEN 1 ELSE NULL END) AS "CNTSQL: 1 < SCAN_P_RATIO <= 10%",
        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 10 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 30  THEN 1 ELSE NULL END) AS "CNTSQL: 10 < SCAN_P_RATIO <= 30%",   
        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 30 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 60  THEN 1 ELSE NULL END) AS "CNTSQL: 30 < SCAN_P_RATIO <= 60%",
        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 60 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 90  THEN 1 ELSE NULL END) AS "CNTSQL: 60 < SCAN_P_RATIO <= 90%",
        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 90                                                    THEN 1 ELSE NULL END) AS "CNTSQL: 90% < SCAN_P_RATIO",
        -- 合計クエリ実行時間：スキャンサイズレンジ
        round(sum(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN total_elapsed_time ELSE 0 END)/1000/3600,1) AS "SUM_ELAPSED_TIME(h): 0B < SCAN_SIZE <= 1GB",
        round(sum(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN total_elapsed_time ELSE 0 END)/1000/3600,1) AS "SUM_ELAPSED_TIME(h): 1GB < SCAN_SIZE <= 20GB",  
        round(sum(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN total_elapsed_time ELSE 0 END)/1000/3600,1) AS "SUM_ELAPSED_TIME(h): 20GB < SCAN_SIZE <= 50GB",
        round(sum(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN total_elapsed_time ELSE 0 END)/1000/3600,1)                                           AS "SUM_ELAPSED_TIME(h): 50GB < SCAN_SIZE",
        -- 平均クエリ実行時間：スキャンサイズレンジ
        round(avg(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN total_elapsed_time ELSE NULL END)/1000,1) AS "AVG_ELAPSED_TIME(s): 0B < SCAN_SIZE <= 1GB",
        round(avg(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN total_elapsed_time ELSE NULL END)/1000,1) AS "AVG_ELAPSED_TIME(s): 1GB < SCAN_SIZE <= 20GB",  
        round(avg(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN total_elapsed_time ELSE NULL END)/1000,1) AS "AVG_ELAPSED_TIME(s): 20GB < SCAN_SIZE <= 50GB",
        round(avg(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN total_elapsed_time ELSE NULL END)/1000,1)                                           AS "AVG_ELAPSED_TIME(s): 50GB < SCAN_SIZE",
        -- 平均スキャンサイズ：スキャンサイズレンジ
        round(avg(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN BYTES_SCANNED ELSE NULL END)/1024/1024/1024,2) AS "AVG_SCAN_SIZE(GB): 0B < SCAN_SIZE <= 1GB",
        round(avg(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN BYTES_SCANNED ELSE NULL END)/1024/1024/1024,2) AS "AVG_SCAN_SIZE(GB): 1GB < SCAN_SIZE <= 20GB",  
        round(avg(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN BYTES_SCANNED ELSE NULL END)/1024/1024/1024,2) AS "AVG_SCAN_SIZE(GB): 20GB < SCAN_SIZE <= 50GB",
        round(avg(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN BYTES_SCANNED ELSE NULL END)/1024/1024/1024,2)                                           AS "AVG_SCAN_SIZE(GB): 50GB < SCAN_SIZE",
        -- 合計クエリ実行時間：クエリ実行時間レンジ
        round(sum(CASE WHEN (total_elapsed_time / 1000) > 0   and (total_elapsed_time / 1000) <= 1 THEN total_elapsed_time ELSE 0 END)/1000/3600,1)      AS "SUM_ELAPSED_TIME(h): 0s < ELAPSED_TIME <= 1s",  
        round(sum(CASE WHEN (total_elapsed_time / 1000) > 1   and (total_elapsed_time / 1000) <= 10 THEN total_elapsed_time ELSE 0 END)/1000/3600,1)     AS "SUM_ELAPSED_TIME(h): 1s < ELAPSED_TIME <= 10s", 
        round(sum(CASE WHEN (total_elapsed_time / 1000) > 10  and (total_elapsed_time / 1000) <= 60 THEN total_elapsed_time ELSE 0 END)/1000/3600,1)     AS "SUM_ELAPSED_TIME(h): 10s < ELAPSED_TIME <= 60s",   
        round(sum(CASE WHEN (total_elapsed_time / 1000) > 60  and (total_elapsed_time / 1000) <= 600 THEN total_elapsed_time ELSE 0 END)/1000/3600,1)    AS "SUM_ELAPSED_TIME(h): 60s < ELAPSED_TIME <= 600s",   
        round(sum(CASE WHEN (total_elapsed_time / 1000) > 600 and (total_elapsed_time / 1000) <= 3600 THEN total_elapsed_time ELSE 0 END)/1000/3600,1)   AS "SUM_ELAPSED_TIME(h): 600s < ELAPSED_TIME <= 3600s",  
        round(sum(CASE WHEN (total_elapsed_time / 1000) > 3600 THEN total_elapsed_time ELSE 0 END)/1000/3600,1)                                          AS "SUM_ELAPSED_TIME(h): 3600s < ELAPSED_TIME"
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' and '{end_str}'
    and BYTES_SCANNED > 0
    and total_elapsed_time > 0
    group by all
    )
    select 
        q.warehouse_name,
        q.warehouse_size,
        to_char(to_timestamp('{begin_str}'),'YYYY-MM-DD') || ' - ' || to_char(to_timestamp('{end_str}'),'YYYY-MM-DD') "DATE PERIOD",
        c.* exclude (warehouse_name),
        round("CNTSQL: 0B < SCAN_SIZE <= 1GB"    /count_total_sql*100,0) || '%' AS "%CNTSQL: 0B < SCAN_SIZE <= 1GB",
        round("CNTSQL: 1GB < SCAN_SIZE <= 20GB"  /count_total_sql*100,0) || '%' AS "%CNTSQL: 1GB < SCAN_SIZE <= 20GB",  
        round("CNTSQL: 20GB < SCAN_SIZE <= 50GB" /count_total_sql*100,0) || '%' AS "%CNTSQL: 20GB < SCAN_SIZE <= 50GB", 
        round("CNTSQL: 50GB < SCAN_SIZE"         /count_total_sql*100,0) || '%' AS "%CNTSQL: 50GB < SCAN_SIZE",
        q.* exclude (warehouse_name,warehouse_size),
        a.* exclude (warehouse_name,warehouse_size)
    from 
        query q
    inner join
        credits c
    on c.warehouse_name = q.warehouse_name
    left outer join
        qas a
    on  q.warehouse_name = a.warehouse_name
    and q.warehouse_size = a.warehouse_size
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
        c.credits_used desc
    ;
    """.format(begin_str=begin_str, end_str=end_str)

    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql18, language="sql")


# WH全体分析(詳細版) sql18
def main18():
    begin_str, end_str = get_filter_inputs(key_suffix="tab18")

    if st.button("クエリ実行", key="execute_button_tab18"):
        execute_query18(begin_str, end_str)

# タイトル表示
st.markdown("<h1 style='color:teal;'>WH全体分析(詳細版)</h1>",unsafe_allow_html = True)
main18()
