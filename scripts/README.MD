# Scripts that are used in pipeline of the project
Here you can see what will be called when you will call the first file on the stage

## Preprocessing
`preprocess.sh`:
  - installing python requirements
  - preprocess the data via `pyspark`
## Stage 1
`stage1.sh`:
  - downloading if needed postgres  `.jar` file
  - downloading the dataset into postgresql
  - transfering the postgres tables of dataset into HDFS via `sqoop`
## Stage 2
`stage2.sh`:
  - creating all tables inside the hive
  - getting data from queries with HQL
## Stage 3
`stage3.sh`
## Stage 4
`stage4.sh`:
  - creating the dashboard on the server.port `60000`
