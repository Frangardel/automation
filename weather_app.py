import streamlit as st
import pandas as pd
import sqlite3

conn = sqlite3.connect("temperatures_db.sqlite3")

c = conn.cursor()

query = "SELECT * FROM temperatures"

df = pd.read_sql_query(query, conn)

st.title("Chupi Pandi")

st.dataframe(df)