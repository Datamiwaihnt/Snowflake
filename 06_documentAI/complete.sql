// complete関数用のデータベース・スキーマ作成
create database complete_db;
create or replace stage input_stage
    DIRECTORY = (ENABLE = true)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

//クロスリージョン推論
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
    
//実行
SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
    'What is your TOEIC score',
    TO_FILE('@input_stage', 'Resume_Jap01_page-0001.jpg'));

SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
    'What is your address',
    TO_FILE('@input_stage', 'Resume_Jap01_page-0001.jpg'));


SELECT 
  'TOEIC Score' AS question,
  SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'What is your TOEIC score',
    TO_FILE('@input_stage', 'Resume_Jap01_page-0001.jpg')
  ) AS response

UNION ALL

SELECT 
  'Address' AS question,
  SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'What is your address',
    TO_FILE('@input_stage', 'Resume_Jap01_page-0001.jpg')
  ) AS response;
