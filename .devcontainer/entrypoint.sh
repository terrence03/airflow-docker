#!/bin/bash

# 初始化 DB 並建立使用者
/opt/airflow/scripts/init_airflow.sh

# 啟動 scheduler（背景）
airflow scheduler &

# 啟動 webserver（背景）
exec airflow webserver -D
