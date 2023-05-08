#!/bin/bashi
path1="./data/clean_movies"
path2="./data/clean_reviews"
fname1="clean_movies.csv"
fname2="clean_reviews.csv"

for eachfile in "${path1}/*.csv"
do
	mv $eachfile "${path1}/${fname1}"
	mv "${path1}/${fname1}" "./data/"
done

for eachfile in "${path2}/*.csv"
do
	mv $eachfile "${path2}/${fname2}"
	mv "${path2}/${fname2}" "./data/"
done
rm -rf $path1 $path2
