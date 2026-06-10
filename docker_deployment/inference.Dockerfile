FROM pytorch/pytorch:2.5.1-cuda12.1-cudnn9-runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KAFKA_SERVER=kafka:29092 \
    PROCESSED_TOPIC=processed_flows \
    INFERENCE_GROUP_ID=inference_group \
    KAFKA_AUTO_OFFSET_RESET=earliest \
    KAFKA_ENABLE_AUTO_COMMIT=true \
    INFLUXDB_URL=http://influxdb:8086 \
    INFLUXDB_ORG=nids_org \
    INFLUXDB_BUCKET=nids_bucket \
    MODEL_DIR=/app/artifacts/model_final \
    INFERENCE_DEVICE=auto \
    CHUNK_SIZE=256 \
    SEQ_TIME_STEPS=10 \
    GRAPH_WINDOW_SIZE=50 \
    GRAPH_MAX_DT=30.0

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    kafka-python==2.3.0 \
    influxdb-client==1.49.0 \
    pydantic==2.11.7 \
    xgboost==2.1.4 \
    torch-geometric==2.6.1

COPY inference/ /app/inference/

WORKDIR /app/inference

CMD ["python", "-u", "daemon.py"]
