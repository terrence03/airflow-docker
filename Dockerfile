FROM apache/airflow:2.10.5-python3.12

# 切換為 root 以進行系統層級安裝
USER root

# 更新系統並安裝常用工具
RUN apt-get update && apt-get install -y \
    nano \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 設定工作目錄
WORKDIR /opt/airflow

# 複製 airflow.cfg 設定檔（視需要可用 volumes 取代）
COPY airflow.cfg /opt/airflow/airflow.cfg
RUN chown airflow: /opt/airflow/airflow.cfg

# 複製初始化與啟動腳本
COPY requirements.txt .
COPY .devcontainer/init_airflow.sh /opt/airflow/scripts/init_airflow.sh
COPY .devcontainer/entrypoint.sh /opt/airflow/scripts/entrypoint.sh
RUN chmod +x /opt/airflow/scripts/*.sh

# 切換回 airflow 使用者
USER airflow

# 安裝額外的 Python 套件（依需求調整）
RUN pip install --no-cache-dir -r requirements.txt

# 設定 airflow home（雖 airflow image 已預設，但加上更清楚）
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

# 啟動指令
# ENTRYPOINT ["/opt/airflow/scripts/entrypoint.sh"]

EXPOSE 8080
