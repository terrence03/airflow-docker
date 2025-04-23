#!/bin/bash

# 初始化 airflow 資料庫
airflow db init

# 建立預設使用者（若尚未建立）
airflow users create \
    --username airflow \
    --firstname Air \
    --lastname Flow \
    --role Admin \
    --email airflow@example.com \
    --password airflow
