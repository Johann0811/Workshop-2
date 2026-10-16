FROM apache/airflow:3.1.8

COPY requirements.txt /requirements.txt
COPY docker/patch_db_manager.py /patch_db_manager.py

RUN pip install --no-cache-dir \
    "apache-airflow==3.1.8" \
    -r /requirements.txt \
 && python /patch_db_manager.py