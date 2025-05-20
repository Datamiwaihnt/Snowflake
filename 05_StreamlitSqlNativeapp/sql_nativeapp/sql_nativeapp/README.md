## Introduction

This is the basic project template for a Snowflake Native App project. It contains minimal code meant to help you set up your first application object in your account quickly..

### Project Structure

| File Name            | Purpose                                                                                                                                                                                 |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| README.md            | The current file you are looking at, meant to guide you through a Snowflake Native App project.                                                                                         |
| app/setup_script.sql | Contains SQL statements that are run when an account installs or upgrades a Snowflake Native App.                                                                                       |
| app/manifest.yml     | Defines properties required by the application package. Find more details at the [Manifest Documentation.](https://docs.snowflake.com/en/developer-guide/native-apps/creating-manifest) |
| app/README.md        | Exposed to the account installing the Snowflake Native App with details on what it does and how to use it.                                                                              |
| snowflake.yml        | Used by the Snowflake CLI tool to discover your project's code and interact with your Snowflake account with all relevant prvileges and grants.                                         |

- WH 設定
  - 確認観点を追記
- ローカルスピル
  - ローカルスピルサイズ範囲ごとの SQL 数
    - 確認観点を追記
    - グラフは OK
  - ローカルスピルが多い SQL
    - 確認観点を追記
- リモートスピル

  - リモートスピルサイズ範囲ごとの SQL 数
    - 確認観点を追記
    - グラフは OK
  - リモートスピルが多い SQL
    - 確認観点を追記
    - 出力データなしなので、考えないと。

- クエリ負荷
  - キュー待ち
    - 直書きしているので、それなおす
  - trx ブロック
    - TX ブロック待ち発生状況
      - 確認観点を追記
    - TX ブロック待ち時間が長い SQL
      - 出力データなしなので、考えないと。
- クエリ実行統計
  - クエリ実行時間
    - クエリ実行時間の傾向
      - 確認観点を追記
    - クエリ実行時間が長い SQL
      - 確認観点を追記
  - クエリスキャンサイズ
    - クエリスキャンサイズの傾向
      - 確認観点を追記
    - 対象クエリスキャンサイズ範囲の SQL
      - 確認観点を追記
  - スキャンパーティション割合
    - スキャンパーティション割合の傾向
      - 確認観点を追記
    - フルスキャン SQL
      - 確認観点を追記
