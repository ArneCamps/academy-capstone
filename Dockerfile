FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

WORKDIR /workspace/academy-capstone

USER 0
ADD https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.9.0-spark_3.1/spark-snowflake_2.12-2.9.0-spark_3.1.jar /opt/spark/jars/spark-snowflake_2.12-2.9.0-spark_3.1.jar
ADD https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.3/snowflake-jdbc-3.13.3.jar /opt/spark/jars/snowflake-jdbc-3.13.3.jar

RUN chmod a+r /opt/spark/jars/spark-snowflake_2.12-2.9.0-spark_3.1.jar /opt/spark/jars/snowflake-jdbc-3.13.3.jar && \
    pip install --upgrade pip && \
    pip install pyspark==3.1.2 && \
    pip install boto3 

EXPOSE 8080

COPY *.py ./


