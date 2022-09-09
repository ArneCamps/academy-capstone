FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

WORKDIR /workspace/academy-capstone

USER 0

RUN pip install --upgrade pip && \
    pip install pyspark==3.1.2 && \
    pip install boto3 

EXPOSE 8080

COPY *.py ./
