import streamlit as st
import pandas as pd

st.title('Rotten Tomatoes Reviews Regressor')

st.markdown('In this Presentation, we will be showing our findings in our project, where we investigated datasets obtained from rotten tomatoes, a movie critique website about')

#movies = pd.read_csv("./data/full_movies.csv", sep=',')
reviews = pd.read_csv("./data/clean_reviews.csv")
q1 = pd.read_csv("./output/q1.csv")
#q2 = pd.read_csv("./output/q2.csv")
q3 = pd.read_csv("./output/q3.csv")
q4 = pd.read_csv("./output/q4.csv")
# q5 = pd.read_csv("./output/q5.csv")

import altair as alt

st.header("Data description")
st.table(reviews)

st.header("Explorative Data Analysis")
st.markdown("---")
st.markdown("## 1$^{st}$ query")
st.markdown("Query about amount of genres' reviews")
c_1 = st.bar_chart(q1)
st.write(c_1)
st.text(q1['genre'].values)

st.markdown("## 1$^{st}$ query")
st.markdown("This is a 3th query to find which genre is more reviewed by the critic")
c_3 = alt.Chart(q3).mark_circle().encode(
    x='critic_name', y='genre', color='reviews_amount', tooltip=['critic_name', 'genre', 'reviews_amount'])
st.write(c_3)


