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
st.text('Movies dataset represent different metrics about movies and their directors, aouthors and genres to be considered.')
st.text("Review table containst critic's name, publisher editor, scores, classification of a score (fresh film or rotten) and content.")

st.text("Also all table contains date columns for movies's release date and critic's publish date")
st.dataframe(REVIEWS.describe())

st.header("Explorative Data Analysis")
st.text('Our insights are about finding the possible correlation between rating given by critic and his genre prefferences (if we are able to detect such things)')
st.text("That's why our queries are connected to each other to get such information.")
st.markdown("---")
st.markdown("## 1$^{st}$ query")
st.markdown("Query about amount of genres' reviews")
C_1 = st.bar_chart(Q1)
st.write(C_1)
st.text(Q1['genre'].values)

st.markdown("## 3$^{st}$ query")
st.markdown("This is a 3th query to find which \
genre is more reviewed by the critic. More dark, more reviewed and popular")
C_3 = alt.Chart(Q3).mark_circle() \
.encode(x='critic_name', y='genre', color='reviews_amount', size='reviews_amount', \
tooltip=['critic_name', 'genre', 'reviews_amount'])
st.write(C_3)

st.markdown("## 4$^{st}$ query")
st.markdown("This is a 4th query to find which \
genre is more successful. More dark, more reviewed and popular")
C_4 = alt.Chart(Q4).mark_circle() \
.encode(x='critic_name', y='genre', color='liked_percent', size='liked_percent', \
tooltip=['critic_name', 'genre', 'liked_percent'])
st.write(C_4)

COMBINE = pd.merge(Q4, Q3, on=["critic_name", "genre"])
st.markdown("## COMBINE queries above")
st.text('Here we can see that most unstable reviews are at the top of the graph, since there are a few amount of reviews, but high average rating.')
C_COMB = alt.Chart(COMBINE).mark_circle() \
.encode(x='critic_name', y='genre', color='reviews_amount', size='liked_percent', \
tooltip=['critic_name', 'genre', 'reviews_amount', 'liked_percent'])
st.write(C_COMB)
