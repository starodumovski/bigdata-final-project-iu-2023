i#!/bin/bash

hive -f ./sql/bd.hql

bash ./scripts/execute_queries.sh; bash ./scripts/save_queries.sh
