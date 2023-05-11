import streamlit as st
import pandas as pd

movies = pd.read_csv("./data/clean_movies.csv")
reviews = pd.read_csv("./data/clean_reviews.csv")
q1 = pd.read_csv("./output/q1.csv")
#q2 = pd.read_csv("./output/q2.csv")
q3 = pd.read_csv("./output/q3.csv")
# q4 = pd.read_csv("./output/q4.csv")
# q5 = pd.read_csv("./output/q5.csv")

import altair as alt
c = alt.Chart(q3).mark_circle().encode(
    x='critic_name', y='genre', color='reviews_amount', tooltip=['critic_name', 'genre', 'reviews_amount'])
st.write(c)

