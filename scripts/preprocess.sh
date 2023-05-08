#!/bin/bash
revs_full="./data/spark_reviews.csv"

rm -rf ./data/clean_*

echo "Combining reviews dataset into one file ${revs_full}"
cat ./data/reviews_part* > $revs_full

echo "Preprocessing the datasets via pyspark"
spark-submit ./scripts/spark_pre.py

rm $revs_full

echo "Retrieving final files"
bash ./scripts/transfer_data.sh
echo "Preprocessing done"
