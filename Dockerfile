FROM bitnami/spark:latest

COPY ./requirements.txt ./requirements.txt
COPY ./source_data ./source_data
COPY ./etl_jobs/Transformation.py ./etl_jobs/Transformation.py

RUN pip install -r requirements.txt 