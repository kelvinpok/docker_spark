# Simple spark datarame transformation on docker

please run the command below in sequence


- 1.docker build -t spark_docker --no-cache . \
  Build custom image named spark-docker in local machine using Dockerfile
- 2.docker-compose up -d \
  Build multiple containers using docker-compose yml file
- 3.docker exec -it pyspark-spark-master-1 /bin/bash \
  Access the container pyspark-spark-master-1
- 4.Access localhost:9090 to find the spark master url \
  find the spark master url in browser
- 5.spark-submit --master {spark master url} /opt/bitnami/spark/etl_jobs/Transformation.py \
  execute the script using spark-submit \
  In my case, it should be spark-submit --master spark://172.21.0.2:7077 /opt/bitnami/spark/etl_jobs/Transformation.py
- 6.docker-compose down \
  open another cli and go into the working directory type the command to stop and remove container
