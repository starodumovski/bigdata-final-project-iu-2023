#!/bin/bash
QUERIES_AMOUNT=4
echo "USE projectdb;" > ./sql/temp_queries.hql
for i in $(seq 1 $QUERIES_AMOUNT)
do
	cat "./sql/q${i}.hql" >> ./sql/temp_queries.hql
done

echo "YOUR QUERIES:"
cat ./sql/temp_queries.hql

hive -f ./sql/temp_queries.hql
rm -rf ./sql./temp_queries.hql
bash ./scripts/save_queries.sh
