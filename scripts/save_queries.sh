#!/bin/bash
header_1="genre,reviews_amount"
header_2="critic_name,reviews_count"
header_3="critic_name,genre,reviews_amount"
header_4="critic_name,genre,liked_percent"

for i in 1 2 3 4
do
	rm -rf "output/q${i}.csv"; tmp_header="header_${i}"; echo ${!tmp_header} > "output/q${i}.csv"; cat "/root/q${i}"/* >> "output/q${i}.csv"
done
