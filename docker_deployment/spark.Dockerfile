FROM pytorch/pytorch:2.5.1-cuda12.1-cudnn9-runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=/opt/conda/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python \
    KAFKA_SERVER=kafka:29092 \
    RAW_TOPIC=raw_flows \
    PROCESSED_TOPIC=processed_flows \
    SPARK_STARTING_OFFSETS=earliest \
    SPARK_CHECKPOINT=file:///app/checkpoints/spark_checkpoints_cciot \
    SAVED_PREPROCESSED_DIR=/app/artifacts/saved_preprocessed \
    SPARK_WORKER_LOG=/tmp/spark_worker_debug.log \
    SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==4.0.1 \
    pandas==2.2.3 \
    numpy==2.1.3 \
    pyarrow==18.1.0 \
    joblib==1.4.2 \
    scikit-learn==1.6.1

COPY spark_streaming.py spark_schema.py /app/

CMD ["python", "-u", "spark_streaming.py"]
