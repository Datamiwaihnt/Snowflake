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
      "Acceleration":[
        st.Page("./resources/sql16.py",title="Acceleration"),
      ],            
      "WH全体分析(簡易版)":[
        st.Page("./resources/sql17.py",title="キュー待ち"),
      ],            
      "WH全体分析(詳細版)":[
        st.Page("./resources/sql18.py",title="キュー待ち"),
      ]      
    
}

pg = st.navigation(pages)
pg.run()