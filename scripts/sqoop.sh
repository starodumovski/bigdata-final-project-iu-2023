#!/bin/bash
project_dir="/project"

hdfs dfs -rm -r $project_dir

sqoop import-all-tables \
    -Dmapreduce.job.user.classpath.first=true \
    --connect jdbc:postgresql://localhost/project \
    --username postgres \
    --warehouse-dir $project_dir \
    --as-avrodatafile \
    --compression-codec=snappy \
    --outdir avsc \
    --m 1

hdfs dfs -put avsc "${project_dir}/avsc"
rm -rf avsc
