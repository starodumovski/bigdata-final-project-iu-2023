"""Libs to creadte a dashboard"""
import streamlit as st
import pandas as pd
import altair as alt

st.title("Rotten Tomatoes Movies' Reviews")

st.markdown('In this Presentation, we will be showing' \
'our findings in our project, where we investigated' \
'datasets obtained from rotten tomatoes,' \
'a movie critique website about')

#movies = pd.read_csv("./data/full_movies.csv", sep=',')
REVIEWS = pd.read_csv("./data/clean_reviews.csv")
Q1 = pd.read_csv("./output/q1.csv").fillna(0)
#q2 = pd.read_csv("./output/q2.csv")
Q3 = pd.read_csv("./output/q3.csv").fillna(0)
Q4 = pd.read_csv("./output/q4.csv").fillna(0)
# q5 = pd.read_csv("./output/q5.csv")

st.header("Data description")
st.table(reviews)

st.header("Explorative Data Analysis")
st.markdown("---")
st.markdown("## 1$^{st}$ query")
st.markdown("Query about amount of genres' reviews")
C_1 = st.bar_chart(Q1)
st.write(C_1)
st.text(Q1['genre'].values)

st.markdown("## 1$^{st}$ query")
st.markdown("This is a 3th query to find which \
genre is more reviewed by the critic")
C_3 = alt.Chart(Q3).mark_circle().encode(
    x='critic_name', y='genre', color='reviews_amount', \
tooltip=['critic_name', 'genre', 'reviews_amount'])
st.write(C_3)
