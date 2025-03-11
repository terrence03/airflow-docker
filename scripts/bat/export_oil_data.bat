cd  /d D:\Projects\airflow-docker
call .\.venv\Scripts\activate dev
call python .\scripts\python\export_oil_data.py
pause
