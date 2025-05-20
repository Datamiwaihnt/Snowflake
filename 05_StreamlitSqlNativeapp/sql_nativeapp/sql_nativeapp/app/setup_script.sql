CREATE APPLICATION ROLE IF NOT EXISTS sql_native_app;
CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE sql_native_app;
GRANT CREATE TABLE ON SCHEMA code_schema TO APPLICATION ROLE sql_native_app;

CREATE STREAMLIT IF NOT EXISTS code_schema.sql_native_streamlit
  FROM '/'
  MAIN_FILE = 'streamlit_app.py'
;
GRANT USAGE ON STREAMLIT code_schema.sql_native_streamlit TO APPLICATION ROLE sql_native_app;

//プロシージャの定義
CREATE OR REPLACE PROCEDURE code_schema.HELLO()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
  AS
  BEGIN
    RETURN 'Hello Snowflake!';
  END;

//プロシージャの権限付与
GRANT USAGE ON PROCEDURE code_schema.hello() TO APPLICATION ROLE sql_native_app;

//sql1プロシージャの定義
CREATE OR REPLACE PROCEDURE code_schema.show_warehouse_proc()
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
def main(session):
    # SHOW WAREHOUSES を実行
    session.sql("SHOW WAREHOUSES").collect()

    # クエリIDを明示的に取得
    query_id_row = session.sql("SELECT LAST_QUERY_ID()").collect()
    query_id = query_id_row[0][0]  # クエリIDを取り出す

    # クエリIDをもとに結果をSELECTする
    df = session.sql(f'SELECT * FROM TABLE(RESULT_SCAN(\'{query_id}\'))')
    return df
$$;

//プロシージャsshow_warehouseの権限付与
GRANT USAGE ON PROCEDURE code_schema.show_warehouse_proc() TO APPLICATION ROLE sql_native_app;

//プロシージャ(ローカルスピルサイズ範囲ごとのSQL数)
CREATE OR REPLACE PROCEDURE code_schema.localSpill1(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  LOCAL_SPILLED_SIZE_RANGE STRING,
  SQL_COUNT NUMBER,
  "%SQL_COUNT" STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_lspilled AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE = 0 THEN 1 END) AS "0: LOCAL_SPILLED_SIZE = 0B", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE > 0 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024 <= 1 THEN 1 END) AS "1: 0B < LOCAL_SPILLED_SIZE <= 1MB",
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024 > 1 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 <= 1 THEN 1 END) AS "2: 1MB < LOCAL_SPILLED_SIZE <= 1GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 > 1 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 <= 10 THEN 1 END) AS "3: 1GB < LOCAL_SPILLED_SIZE <= 10GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 > 10 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 <= 100 THEN 1 END) AS "4: 10GB < LOCAL_SPILLED_SIZE <= 100GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 > 100 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024/1024 <= 1 THEN 1 END) AS "5: 100GB < LOCAL_SPILLED_SIZE <= 1TB",
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024/1024 > 1 THEN 1 END) AS "6: 1TB < LOCAL_SPILLED_SIZE"
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND BYTES_SCANNED > 0
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR LOCAL_SPILLED_SIZE_RANGE IN (          
        "6: 1TB < LOCAL_SPILLED_SIZE",
        "5: 100GB < LOCAL_SPILLED_SIZE <= 1TB",
        "4: 10GB < LOCAL_SPILLED_SIZE <= 100GB",
        "3: 1GB < LOCAL_SPILLED_SIZE <= 10GB",
        "2: 1MB < LOCAL_SPILLED_SIZE <= 1GB",
        "1: 0B < LOCAL_SPILLED_SIZE <= 1MB",
        "0: LOCAL_SPILLED_SIZE = 0B"
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      LOCAL_SPILLED_SIZE_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as "%SQL_COUNT"
    FROM sqlcnt_per_lspilled
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill1(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;

//ローカルスピル発生量が多いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill2(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  BYTES_SPILLED_TO_LOCAL_STORAGE NUMBER,
  BYTES_SPILLED_TO_LOCAL_STORAGE_GB NUMBER(10,2),
  BYTES_SPILLED_TO_REMOTE_STORAGE NUMBER,
  BYTES_SPILLED_TO_REMOTE_STORAGE_GB NUMBER(10,2)
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        round(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_LOCAL_STORAGE_GB,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        round(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_REMOTE_STORAGE_GB
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and BYTES_SPILLED_TO_LOCAL_STORAGE > 0
    order by BYTES_SPILLED_TO_LOCAL_STORAGE desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill2(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;


//プロシージャリモートスピルサイズ発生状況
CREATE OR REPLACE PROCEDURE code_schema.localSpill3(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  REMOTE_SPILLED_SIZE_RANGE STRING,
  SQL_COUNT NUMBER,
  "%SQL_COUNT" STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_rspilled AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                = 0  THEN 1 ELSE NULL END)                                                                 AS "0: REMOTE_SPILLED_SIZE = 0B", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                > 0  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)       <= 1 THEN 1 ELSE NULL END)      AS "1: 0B < REMOTE_SPILLED_SIZE <= 1MB",   
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)      > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 1 THEN 1 ELSE NULL END)      AS "2: 1MB < REMOTE_SPILLED_SIZE <= 1GB", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 10 THEN 1 ELSE NULL END)     AS "3: 1GB < REMOTE_SPILLED_SIZE <= 10GB", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 10  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) <= 100 THEN 1 ELSE NULL END)    AS "4: 10GB < REMOTE_SPILLED_SIZE <= 100GB", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 100 and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) <= 1 THEN 1 ELSE NULL END) AS "5: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) > 1 THEN 1 ELSE NULL END)                                                             AS "6: 1TB < REMOTE_SPILLED_SIZE"
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND BYTES_SCANNED > 0
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR REMOTE_SPILLED_SIZE_RANGE IN (
        "6: 1TB < REMOTE_SPILLED_SIZE",
        "5: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
        "4: 10GB < REMOTE_SPILLED_SIZE <= 100GB",
        "3: 1GB < REMOTE_SPILLED_SIZE <= 10GB",
        "2: 1MB < REMOTE_SPILLED_SIZE <= 1GB",
        "1: 0B < REMOTE_SPILLED_SIZE <= 1MB",
        "0: REMOTE_SPILLED_SIZE = 0B"
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      REMOTE_SPILLED_SIZE_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as "%SQL_COUNT"
    FROM sqlcnt_per_rspilled
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill3(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;


//リモートスピル発生量が多いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill4(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  BYTES_SPILLED_TO_LOCAL_STORAGE NUMBER,
  BYTES_SPILLED_TO_LOCAL_STORAGE_GB NUMBER(10,2),
  BYTES_SPILLED_TO_REMOTE_STORAGE NUMBER,
  BYTES_SPILLED_TO_REMOTE_STORAGE_GB NUMBER(10,2)
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        round(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_LOCAL_STORAGE_GB,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        round(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_REMOTE_STORAGE_GB
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and BYTES_SPILLED_TO_REMOTE_STORAGE > 0
    order by BYTES_SPILLED_TO_REMOTE_STORAGE desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill4(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;

//キュー待ち発生状況
CREATE OR REPLACE PROCEDURE code_schema.localSpill5(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  QUEUED_PERCENT_RANGE STRING,
  SQL_COUNT NUMBER,
  PERCENT_SQL_COUNT STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_queued_percent AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                               AS "0: ELAPSED_TIME_QUEUED% = 0%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0      and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "1: 0% < ELAPSED_TIME_QUEUED% <= 1%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.01   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "2: 1% < ELAPSED_TIME_QUEUED% <= 5%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.05   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "3: 5% < ELAPSED_TIME_QUEUED% <= 20%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.2    and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "4: 20% < ELAPSED_TIME_QUEUED% <= 50%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                             AS "5: 50% < ELAPSED_TIME_QUEUED%" 
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
        AND warehouse_name = :warehouse
        AND warehouse_size IS NOT NULL
        AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR queued_percent_range IN (
        "0: ELAPSED_TIME_QUEUED% = 0%",
        "1: 0% < ELAPSED_TIME_QUEUED% <= 1%",
        "2: 1% < ELAPSED_TIME_QUEUED% <= 5%",
        "3: 5% < ELAPSED_TIME_QUEUED% <= 20%",
        "4: 20% < ELAPSED_TIME_QUEUED% <= 50%",
        "5: 50% < ELAPSED_TIME_QUEUED%"        
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      QUEUED_PERCENT_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as PERCENT_SQL_COUNT
    FROM sqlcnt_per_queued_percent
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill5(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;



//キュー待ちが長いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill6(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  ELAPSED_TIME_S NUMBER,
  QUEUED_TIME_S NUMBER,
  PERCENT_QUEUED STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        round(total_elapsed_time/1000,2) elapsed_time_s,
        round(QUEUED_OVERLOAD_TIME/1000,2) queued_time_s,
        round(QUEUED_OVERLOAD_TIME/total_elapsed_time * 100,2) || '%' as "PERCENT_QUEUED",
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and queued_time_s > 0    
    order by queued_time_s desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill6(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;


//trxブロック発生状況
CREATE OR REPLACE PROCEDURE code_schema.localSpill7(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  TXBLOCKED_PERCENT_RANGE STRING,
  SQL_COUNT NUMBER,
  "%SQL_COUNT" STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_txblocked_percent AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                                   AS "0: ELAPSED_TIME_TXBLOCKED% = 0%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0      and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "1: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.01   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "2: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.05   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "3: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.2    and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "4: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                                 AS "5: 50% < ELAPSED_TIME_TXBLOCKED%"
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR txblocked_percent_range IN (
        "0: ELAPSED_TIME_TXBLOCKED% = 0%",
        "1: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
        "2: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
        "3: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
        "4: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
        "5: 50% < ELAPSED_TIME_TXBLOCKED%"  
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      TXBLOCKED_PERCENT_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as "%SQL_COUNT"
    FROM sqlcnt_per_txblocked_percent
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill7(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;

//TXブロック待ち時間が長いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill8(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  ELAPSED_TIME_S NUMBER,
  TXBLOCKED_TIME_S NUMBER,
  PERCENT_TXBLOCKED STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        round(total_elapsed_time/1000,2) elapsed_time_s,
        round(TRANSACTION_BLOCKED_TIME/1000,2) txblocked_time_s,
        round(TRANSACTION_BLOCKED_TIME/total_elapsed_time * 100,2) || '%' as "PERCENT_TXBLOCKED",
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and txblocked_time_s > 0  
    order by txblocked_time_s desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill8(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;


//クエリ実行時間の傾向
CREATE OR REPLACE PROCEDURE code_schema.localSpill9(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  ELAPSED_TIME_RANGE STRING,
  SQL_COUNT NUMBER,
  "%SQL_COUNT" STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_range AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 0   and (total_elapsed_time / 1000) <= 1 THEN 1 ELSE NULL END)      AS "1: 0s < ELAPSED_TIME <= 1s", 
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 1   and (total_elapsed_time / 1000) <= 10 THEN 1 ELSE NULL END)     AS "2: 1s < ELAPSED_TIME <= 10s", 
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 10  and (total_elapsed_time / 1000) <= 60 THEN 1 ELSE NULL END)     AS "3: 10s < ELAPSED_TIME <= 60s",
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 60  and (total_elapsed_time / 1000) <= 600 THEN 1 ELSE NULL END)    AS "4: 60s < ELAPSED_TIME <= 600s",
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 600 and (total_elapsed_time / 1000) <= 3600 THEN 1 ELSE NULL END)   AS "5: 600s < ELAPSED_TIME <= 3600s",
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 3600 THEN 1 ELSE NULL END) AS "6: 3600s < ELAPSED_TIME", 
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND total_elapsed_time > 0
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR elapsed_time_range IN (
            "6: 3600s < ELAPSED_TIME",
            "5: 600s < ELAPSED_TIME <= 3600s",
            "4: 60s < ELAPSED_TIME <= 600s",
            "3: 10s < ELAPSED_TIME <= 60s",
            "2: 1s < ELAPSED_TIME <= 10s",
            "1: 0s < ELAPSED_TIME <= 1s"
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      ELAPSED_TIME_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as "%SQL_COUNT"
    FROM sqlcnt_per_range
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill9(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;



//クエリ実行時間が長いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill10(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  TOTAL_ELAPSED_TIME_S NUMBER(10,1)
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
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
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and total_elapsed_time_s > 0 
    order by total_elapsed_time_s desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill10(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;


//クエリスキャンサイズの傾向
CREATE OR REPLACE PROCEDURE code_schema.localSpill11(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  SCAN_SIZE_RANGE STRING,
  SQL_COUNT NUMBER,
  "%SQL_COUNT" STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_scansize AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN 1 ELSE NULL END) AS "1: 0B < SCAN_SIZE <= 1GB",					
          COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN 1 ELSE NULL END) AS "2: 1GB < SCAN_SIZE <= 20GB",   					
          COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN 1 ELSE NULL END) AS "3: 20GB < SCAN_SIZE <= 50GB",					
          COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN 1 ELSE NULL END)                                           AS "4: 50GB < SCAN_SIZE"				
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND BYTES_SCANNED > 0
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR scan_size_range IN (
          "4: 50GB < SCAN_SIZE",
          "3: 20GB < SCAN_SIZE <= 50GB",
          "2: 1GB < SCAN_SIZE <= 20GB",
          "1: 0B < SCAN_SIZE <= 1GB"
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      SCAN_SIZE_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 1) || '%' as "%SQL_COUNT"
    FROM sqlcnt_per_scansize
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill11(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;


//対象クエリスキャンサイズ範囲のSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill12(
  warehouse STRING,
  begin_str STRING,
  end_str STRING,
  minNumber NUMBER,
  maxNumber NUMBER
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  BYTES_SCANNED_GB NUMBER(10,2),
  TOTAL_ELAPSED_TIME_S NUMBER(10,2)
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        round(BYTES_SCANNED/1024/1024/1024,2) AS BYTES_SCANNED_GB,
        round(total_elapsed_time / 1000,2) AS total_elapsed_time_s
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and ROUND(BYTES_SCANNED/1024/1024/1024,2) > :minNumber
    and ROUND(BYTES_SCANNED/1024/1024/1024,2) <= :maxNumber
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    order by ROUND(BYTES_SCANNED/1024/1024/1024,2) DESC
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill12(STRING,STRING,STRING,NUMBER,NUMBER) TO APPLICATION ROLE sql_native_app;




//スキャンパーティション割合
CREATE OR REPLACE PROCEDURE code_schema.localSpill13(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  SCAN_PARTITION_RATO_RANGE STRING,
  SQL_COUNT NUMBER,
  "%SCAN_PARTITION_RATIO" STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
      with scan_partition_ratio as (
      SELECT * FROM (
        SELECT
          warehouse_name,					
          warehouse_size,					
          COUNT(*) total_count_sql,					
          COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 0  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 1   THEN 1 ELSE NULL END) AS "1: 0 < SCAN_P_RATIO <= 1%",					
          COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 1  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 10  THEN 1 ELSE NULL END) AS "2: 1 < SCAN_P_RATIO <= 10%",					
          COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 10 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 30  THEN 1 ELSE NULL END) AS "3: 10 < SCAN_P_RATIO <= 30%",   					
          COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 30 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 60  THEN 1 ELSE NULL END) AS "4: 30 < SCAN_P_RATIO <= 60%",					
          COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 60 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 90  THEN 1 ELSE NULL END) AS "5: 60 < SCAN_P_RATIO <= 90%",					
          COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 90                                                    THEN 1 ELSE NULL END) AS "6: 90% < SCAN_P_RATIO"					
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND PARTITIONS_TOTAL > 0
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR SCAN_PARTITION_RATO_RANGE IN (
            "6: 90% < SCAN_P_RATIO",
            "5: 60 < SCAN_P_RATIO <= 90%",
            "4: 30 < SCAN_P_RATIO <= 60%",
            "3: 10 < SCAN_P_RATIO <= 30%",
            "2: 1 < SCAN_P_RATIO <= 10%",
            "1: 0 < SCAN_P_RATIO <= 1%"
            )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      SCAN_PARTITION_RATO_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 1) || '%' as "%SCAN_PARTITION_RATIO"
    FROM scan_partition_ratio
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill13(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;



//フルスキャンSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill14(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  "%SCAN_PARTITION_RATIO" NUMBER,
  PARTITIONS_SCANNED NUMBER,
  PARTITIONS_TOTAL NUMBER,
  BYTES_SCANNED_GB NUMBER(10,2),
  TOTAL_ELAPSED_TIME_S NUMBER(10,2)
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    select
      warehouse_name,			
      warehouse_size,			
      query_id,			
      query_text,			
      round(PARTITIONS_SCANNED/PARTITIONS_TOTAL*100,2) "%SCAN_PARTITION_RATIO" ,			
      PARTITIONS_SCANNED,			
      PARTITIONS_TOTAL,			
      round(BYTES_SCANNED/1024/1024/1024,2) BYTES_SCANNED_GB,			
      round(total_elapsed_time / 1000,2) total_elapsed_time_s			
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and (PARTITIONS_TOTAL > 0 and "%SCAN_PARTITION_RATIO" >= 90 and "%SCAN_PARTITION_RATIO" <= 100)	
    order by "%SCAN_PARTITION_RATIO" desc, PARTITIONS_SCANNED desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill14(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;



//QueryAccelerationService
CREATE OR REPLACE PROCEDURE code_schema.localSpill15(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  ELIGIBLE_QUERY_ACCELERATION_TIME NUMBER,
  UPPER_LIMIT_SCALE_FACTOR NUMBER
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    SELECT 
        query_id, 
        query_text,
        eligible_query_acceleration_time,
        UPPER_LIMIT_SCALE_FACTOR
    FROM 
        snowflake.account_usage.QUERY_ACCELERATION_ELIGIBLE
    where 
        warehouse_name = :warehouse
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between :begin_str AND :end_str
    ORDER BY eligible_query_acceleration_time DESC
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill15(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;


//WH簡易分析
CREATE OR REPLACE PROCEDURE code_schema.localSpill16(
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  PERCENT_LARGE STRING,
  PERCENT_SMALL STRING,
  AVG_BYTES_LARGE STRING,
  "AVG_LARGE_EXE_TIME(s)" NUMBER,
  "AVG_ALL_EXE_TIME(s)" NUMBER,
  COUNT_QUERIES NUMBER,
  CREDITS_USED NUMBER,
  "DATE PERIOD" STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    with credits as (
    select 
        warehouse_name::varchar warehouse_name,
        round(sum(credits_used),1) as credits_used
    from snowflake.account_usage.warehouse_metering_history 
    where start_time between to_char(to_timestamp(:begin_str),'YYYY-MM-DD')::TIMESTAMP_LTZ  and to_char(to_timestamp(:end_str),'YYYY-MM-DD')::TIMESTAMP_LTZ
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
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between :begin_str AND :end_str
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
        to_char(to_timestamp(:begin_str),'YYYY-MM-DD') || ' - ' || to_char(to_timestamp(:end_str),'YYYY-MM-DD') "DATE PERIOD",
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
        c.credits_used desc
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill16(STRING,STRING) TO APPLICATION ROLE sql_native_app;



//WH詳細分析
CREATE OR REPLACE PROCEDURE code_schema.localSpill17(
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME VARCHAR,
  WAREHOUSE_SIZE VARCHAR,
  "DATE PERIOD" VARCHAR,
  CREDITS_USED NUMBER(38,1),
  "%CNTSQL: 0B < SCAN_SIZE <= 1GB" VARCHAR,
  "%CNTSQL: 1GB < SCAN_SIZE <= 20GB" VARCHAR,
  "%CNTSQL: 20GB < SCAN_SIZE <= 50GB" VARCHAR,
  "%CNTSQL: 50GB < SCAN_SIZE" VARCHAR,
  "AVG_SCAN_SIZE(GB)" NUMBER(38,2),
  "MDN_SCAN_SIZE(GB)" NUMBER(38,2),
  "SUM_SCAN_SIZE(GB)" NUMBER(38,2),
  "MAX_SCAN_SIZE(GB)" NUMBER(38,2),
  "AVG_LOCAL_SPILLED_SIZE(GB)" NUMBER(38,2),
  "MDN_LOCAL_SPILLED_SIZE(GB)" NUMBER(38,2),
  "SUM_LOCAL_SPILLED_SIZE(GB)" NUMBER(38,2),
  "MAX_LOCAL_SPILLED_SIZE(GB)" NUMBER(38,2),
  "AVG_REMOTE_SPILLED_SIZE(GB)" NUMBER(38,2),
  "MDN_REMOTE_SPILLED_SIZE(GB)" NUMBER(38,2),
  "SUM_REMOTE_SPILLED_SIZE(GB)" NUMBER(38,2),
  "MAX_REMOTE_SPILLED_SIZE(GB)" NUMBER(38,2),
  "AVG_ELAPSED_TIME(s)" NUMBER(38,2),
  "MDN_ELAPSED_TIME(s)" NUMBER(38,2),
  "SUM_ELAPSED_TIME(s)" NUMBER(38,2),
  "SUM_ELAPSED_TIME(h)" NUMBER(38,2),
  "MAX_ELAPSED_TIME(s)" NUMBER(38,2),
  "AVG_QUEUED_TIME(s)" NUMBER(38,2),
  "MDN_QUEUED_TIME(s)" NUMBER(38,2),
  "SUM_QUEUED_TIME(s)" NUMBER(38,2),
  "MAX_QUEUED_TIME(s)" NUMBER(38,2),
  "AVG_TXBLOCKED_TIME(s)" NUMBER(38,2),
  "MDN_TXBLOCKED_TIME(s)" NUMBER(38,2),
  "SUM_TXBLOCKED_TIME(s)" NUMBER(38,2),
  "MAX_TXBLOCKED_TIME(s)" NUMBER(38,2),
  "AVG_SCAN_PARTITION_RATIO(%)" NUMBER(38,2),
  "MDN_SCAN_PARTITION_RATIO(%)" NUMBER(38,2),
  COUNT_TOTAL_SQL NUMBER(18,0),
  "CNTSQL: 0B < SCAN_SIZE <= 1GB" NUMBER(18,0),
  "CNTSQL: 1GB < SCAN_SIZE <= 20GB" NUMBER(18,0),
  "CNTSQL: 20GB < SCAN_SIZE <= 50GB" NUMBER(18,0),
  "CNTSQL: 50GB < SCAN_SIZE" NUMBER(18,0),
  "CNTSQL: LOCAL_SPILLED_SIZE = 0B" NUMBER(18,0),
  "CNTSQL: 0B < LOCAL_SPILLED_SIZE <= 1MB" NUMBER(18,0),
  "CNTSQL: 1MB < LOCAL_SPILLED_SIZE <= 1GB" NUMBER(18,0),
  "CNTSQL: 1GB < LOCAL_SPILLED_SIZE <= 10GB" NUMBER(18,0),
  "CNTSQL: 10GB < LOCAL_SPILLED_SIZE <= 100GB" NUMBER(18,0),
  "CNTSQL: 100GB < LOCAL_SPILLED_SIZE <= 1TB" NUMBER(18,0),
  "CNTSQL: 1TB < LOCAL_SPILLED_SIZE" NUMBER(18,0),
  "CNTSQL: REMOTE_SPILLED_SIZE = 0B" NUMBER(18,0),
  "CNTSQL: 0B < REMOTE_SPILLED_SIZE <= 1MB" NUMBER(18,0),
  "CNTSQL: 1MB < REMOTE_SPILLED_SIZE <= 1GB" NUMBER(18,0),
  "CNTSQL: 1GB < REMOTE_SPILLED_SIZE <= 10GB" NUMBER(18,0),
  "CNTSQL: 10GB < REMOTE_SPILLED_SIZE <= 100GB" NUMBER(18,0),
  "CNTSQL: 100GB < REMOTE_SPILLED_SIZE <= 1TB" NUMBER(18,0),
  "CNTSQL: 1TB < REMOTE_SPILLED_SIZE" NUMBER(18,0),
  "CNTSQL: 0s < ELAPSED_TIME <= 1s" NUMBER(18,0),
  "CNTSQL: 1s < ELAPSED_TIME <= 10s" NUMBER(18,0),
  "CNTSQL: 10s < ELAPSED_TIME <= 60s" NUMBER(18,0),
  "CNTSQL: 60s < ELAPSED_TIME <= 600s" NUMBER(18,0),
  "CNTSQL: 600s < ELAPSED_TIME <= 3600s" NUMBER(18,0),
  "CNTSQL: 3600s < ELAPSED_TIME" NUMBER(18,0),
  "CNTSQL: ELAPSED_TIME_QUEUED% = 0%" NUMBER(18,0),
  "CNTSQL: 0% < ELAPSED_TIME_QUEUED% <= 1%" NUMBER(18,0),
  "CNTSQL: 1% < ELAPSED_TIME_QUEUED% <= 5%" NUMBER(18,0),
  "CNTSQL: 5% < ELAPSED_TIME_QUEUED% <= 20%" NUMBER(18,0),
  "CNTSQL: 20% < ELAPSED_TIME_QUEUED% <= 50%" NUMBER(18,0),
  "CNTSQL: 50% < ELAPSED_TIME_QUEUED%" NUMBER(18,0),
  "CNTSQL: ELAPSED_TIME_TXBLOCKED% = 0%" NUMBER(18,0),
  "CNTSQL: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%" NUMBER(18,0),
  "CNTSQL: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%" NUMBER(18,0),
  "CNTSQL: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%" NUMBER(18,0),
  "CNTSQL: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%" NUMBER(18,0),
  "CNTSQL: 50% < ELAPSED_TIME_TXBLOCKED%" NUMBER(18,0),
  "CNTSQL: 0 < SCAN_P_RATIO <= 1%" NUMBER(18,0),
  "CNTSQL: 1 < SCAN_P_RATIO <= 10%" NUMBER(18,0),
  "CNTSQL: 10 < SCAN_P_RATIO <= 30%" NUMBER(18,0),
  "CNTSQL: 30 < SCAN_P_RATIO <= 60%" NUMBER(18,0),
  "CNTSQL: 60 < SCAN_P_RATIO <= 90%" NUMBER(18,0),
  "CNTSQL: 90% < SCAN_P_RATIO" NUMBER(18,0),
  "SUM_ELAPSED_TIME(h): 0B < SCAN_SIZE <= 1GB" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 1GB < SCAN_SIZE <= 20GB" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 20GB < SCAN_SIZE <= 50GB" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 50GB < SCAN_SIZE" NUMBER(38,1),
  "AVG_ELAPSED_TIME(s): 0B < SCAN_SIZE <= 1GB" NUMBER(38,1),
  "AVG_ELAPSED_TIME(s): 1GB < SCAN_SIZE <= 20GB" NUMBER(38,1),
  "AVG_ELAPSED_TIME(s): 20GB < SCAN_SIZE <= 50GB" NUMBER(38,1),
  "AVG_ELAPSED_TIME(s): 50GB < SCAN_SIZE" NUMBER(38,1),
  "AVG_SCAN_SIZE(GB): 0B < SCAN_SIZE <= 1GB" NUMBER(38,2),
  "AVG_SCAN_SIZE(GB): 1GB < SCAN_SIZE <= 20GB" NUMBER(38,2),
  "AVG_SCAN_SIZE(GB): 20GB < SCAN_SIZE <= 50GB" NUMBER(38,2),
  "AVG_SCAN_SIZE(GB): 50GB < SCAN_SIZE" NUMBER(38,2),
  "SUM_ELAPSED_TIME(h): 0s < ELAPSED_TIME <= 1s" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 1s < ELAPSED_TIME <= 10s" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 10s < ELAPSED_TIME <= 60s" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 60s < ELAPSED_TIME <= 600s" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 600s < ELAPSED_TIME <= 3600s" NUMBER(38,1),
  "SUM_ELAPSED_TIME(h): 3600s < ELAPSED_TIME" NUMBER(38,1),
  TOTAL_ELIGIBLE_QAS_SQL_COUNT NUMBER(18,0),
  "avg_eligible_query_acceleration_time(s)" NUMBER(38,2),
  "mdn_eligible_query_acceleration_time(s)" NUMBER(38,2),
  "sum_eligible_query_acceleration_time(s)" NUMBER(38,0),
  "max_eligible_query_acceleration_time(s)" NUMBER(38,0)
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    with credits as (
    select 
        warehouse_name::varchar warehouse_name,
        round(sum(credits_used),1) as credits_used
    from snowflake.account_usage.warehouse_metering_history 
    where start_time between to_char(to_timestamp(:begin_str),'YYYY-MM-DD')::TIMESTAMP_LTZ  and to_char(to_timestamp(:end_str),'YYYY-MM-DD')::TIMESTAMP_LTZ
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
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between :begin_str and :end_str
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
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) 
    BETWEEN :begin_str AND :end_str
    and BYTES_SCANNED > 0
    and total_elapsed_time > 0
    group by all
    )
    select 
        q.warehouse_name,
        q.warehouse_size,
        to_char(to_timestamp(:begin_str),'YYYY-MM-DD') || ' - ' || to_char(to_timestamp(:end_str),'YYYY-MM-DD') "DATE PERIOD",
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
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill17(STRING,STRING) TO APPLICATION ROLE sql_native_app;


