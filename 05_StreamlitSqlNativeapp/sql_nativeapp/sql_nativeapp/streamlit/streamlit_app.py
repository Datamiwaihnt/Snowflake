import streamlit as st

def page2():
  st.title("hello")


pages = {
    "WH設定":[
        st.Page("resources/sql1.py",title="WH設定")
      ],
      "スピリング":[
        st.Page("./resources/sql2_3.py",title="ローカルスピル"),
        st.Page("./resources/sql4_5.py",title="リモートスピル")
      ],
      "クエリ負荷":[
        st.Page("./resources/sql6_7.py",title="キュー待ち"),
        st.Page("./resources/sql8_9.py",title="トランザクションブロック")
      ],
      "クエリ実行統計":[
      st.Page("./resources/sql10_11.py",title="クエリ実行時間"),
      st.Page("./resources/sql12_13.py",title="クエリスキャンサイズ"),
      st.Page("./resources/sql14_15.py",title="スキャンパーティション割合")
      ],
      "その他":[
      st.Page("./resources/sql16.py",title="Query Acceleration Service"),
      st.Page("./resources/sql17.py",title="WH全体分析(簡易版)"),
      st.Page("./resources/sql18.py",title="WH全体分析(詳細版)")
      ]
}

pg = st.navigation(pages)
pg.run()
