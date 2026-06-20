# NIDS Docker Deployment

This directory contains the streaming deployment pipeline for the proposed Network Intrusion Detection System (NIDS). The deployment currently targets **use case 3—the CIC IIoT 2025 dataset**.

![Deployment pipeline](../Images/Deployment%20Diagram.png)

## Pipeline Overview

```text
data_1s.parquet
      │
      ▼
Data Generator ──► Kafka: raw_flows
                        │
                        ▼
                Spark preprocessing
                        │
                        ▼
                 Kafka: processed_flows
                        │
                        ▼
               Ensemble inference daemon
                        │
                        ▼
                     InfluxDB
                        │
                        ▼
                      Grafana
```

The data generator runs on the host machine and replays chronologically ordered network flows to Kafka. Spark Streaming applies the saved preprocessing pipeline, while the inference daemon combines the GAT–XGBoost and Residual CNN–BiLSTM branches through an XGBoost meta-learner. Predictions are written to InfluxDB and visualized in Grafana.

## Services

| Service | Purpose | Container name | Host port |
|---|---|---|---|
| Zookeeper | Kafka coordination | `zookeeper` | Internal only |
| Kafka | Raw and processed flow topics | `kafka` | `9092` |
| Spark Streaming | Online preprocessing | `spark-streaming` | Internal only |
| Inference daemon | Ensemble model inference | `inference-daemon` | Internal only |
| InfluxDB | Prediction and flow storage | `influxdb` | `8086` |
| Grafana | Monitoring dashboard | `grafana` | `3000` |
| Grafana Image Renderer | Server-side panel rendering | `grafana-renderer` | Internal only |

All containers communicate through the `nids_net` Docker bridge network.

## Prerequisites

- Docker Desktop with Docker Compose v2
- Python 3.11 or 3.12 on the host machine
- Dependencies installed from the repository root:

  ```powershell
  python -m pip install -r requirements.txt
  ```

- The preprocessed CIC IIoT data and trained deployment artifacts described below

GPU inference is optional. With `INFERENCE_DEVICE=auto`, the inference daemon uses CUDA when it is available to the container and otherwise falls back to CPU.

## Required Artifacts

### Preprocessing artifacts

Spark expects the following directory to be mounted read-only at `/app/artifacts/saved_preprocessed`:

```text
Use case 3_CIC_IIOT_2025/saved_preprocessed/
├── cols_to_drop.pkl
├── freq_network_protocols_dst.pkl
├── freq_network_protocols_src.pkl
├── label_encoder.pkl
├── mlb_log_data_types.pkl
├── mlb_network_protocols_dst.pkl
├── mlb_network_protocols_src.pkl
├── numeric_columns.pkl
└── quantile_scaler.pkl
```

The host-side data generator additionally reads:

```text
Use case 3_CIC_IIOT_2025/saved_preprocessed/data_1s.parquet
```

### Model artifacts

The inference container expects these models under `/app/artifacts/model_final`:

```text
Use case 3_CIC_IIOT_2025/model_saved/
├── gat_embedder_exper_1_best.pth
├── cnn_bilstm_exper_1_best.pth
├── GAT_XGB_Hybrid_Temporal_Model_exper_1_best.json
└── meta_learner_xgb_hybrid.json
```

## Path Synchronization After Repository Restructuring

The repository was recently reorganized. Before starting the stack, ensure that `docker-compose.yml` uses the new directories:

```yaml
inference-daemon:
  volumes:
    - ../Use case 3_CIC_IIOT_2025/model_saved:/app/artifacts/model_final:ro

spark-streaming:
  volumes:
    - ../Use case 3_CIC_IIOT_2025/saved_preprocessed:/app/artifacts/saved_preprocessed:ro
```

The filenames loaded in `inference/models/ensemble.py` must also match the four deployment artifacts listed above. Any absolute `C:\Users\...\IoT Dataset\CCIOT` paths are legacy development defaults and should be overridden through environment variables or replaced with paths from the current repository structure.

## Configuration

Runtime configuration is read from `docker_deployment/.env`. This file is intentionally excluded from Git because it may contain credentials.

Important variables include:

| Variable | Default | Description |
|---|---|---|
| `RAW_TOPIC` | `raw_flows` | Kafka topic receiving unprocessed flows |
| `PROCESSED_TOPIC` | `processed_flows` | Kafka topic receiving preprocessed flows |
| `KAFKA_HOST_PORT` | `9092` | Kafka port exposed to the host |
| `SPARK_STARTING_OFFSETS` | `earliest` | Initial Kafka offsets for Spark |
| `CHUNK_SIZE` | `256` | Number of flows processed per inference batch |
| `SEQ_TIME_STEPS` | `10` | Sequence length for the temporal branch |
| `GRAPH_WINDOW_SIZE` | `50` | Historical graph buffer size |
| `GRAPH_MAX_DT` | `30.0` | Maximum graph time difference in seconds |
| `INFERENCE_DEVICE` | `auto` | Inference device: `auto`, `cpu`, or `cuda` |
| `INFLUXDB_ORG` | `nids_org` | InfluxDB organization |
| `INFLUXDB_BUCKET` | `nids_bucket` | InfluxDB destination bucket |

Default credentials are suitable only for local development. Change all passwords and tokens before exposing the services outside the host machine.

## Quick Start

Run all commands from the `docker_deployment` directory:

```powershell
cd "C:\Users\Admin\Downloads\Graduation Thesis\docker_deployment"
```

### 1. Build and start the stack

```powershell
docker compose up -d --build
```

### 2. Check container health

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Wait until Kafka, InfluxDB, Spark Streaming, and the inference daemon report a healthy status. Images may take longer to build during the first run.

### 3. Start the host-side data generator

Set the path to the replay dataset and run the generator:

```powershell
$env:DATA_PATH="..\Use case 3_CIC_IIOT_2025\saved_preprocessed\data_1s.parquet"
$env:KAFKA_SERVER="localhost:9092"
$env:RAW_TOPIC="raw_flows"
python data_generator.py
```

The default `SPEED_FACTOR` is `10000`, meaning that event-time delays are replayed at 10,000 times their original speed. Override it when necessary:

```powershell
$env:SPEED_FACTOR="1000"
python data_generator.py
```

## Managing Individual Services

Start only Kafka:

```powershell
docker compose up -d zookeeper kafka
```

Start the monitoring stack:

```powershell
docker compose up -d influxdb renderer grafana
```

Build and start Spark Streaming:

```powershell
docker compose up -d --build spark-streaming
```

Build and start the inference daemon:

```powershell
docker compose up -d --build inference-daemon
```

Restart or stop an individual service:

```powershell
docker compose restart spark-streaming
docker compose restart inference-daemon

docker compose stop spark-streaming
docker compose stop inference-daemon
```

## Monitoring Logs

Follow Spark Streaming logs:

```powershell
docker compose logs -f spark-streaming
```

Inspect the latest Spark logs and preprocessing worker log:

```powershell
docker compose logs --tail=200 spark-streaming
docker exec spark-streaming sh -lc "tail -n 120 /tmp/spark_worker_debug.log"
```

Follow inference logs:

```powershell
docker compose logs -f inference-daemon
```

Inspect the latest inference logs:

```powershell
docker compose logs --tail=200 inference-daemon
```

For any unhealthy service:

```powershell
docker compose logs --tail=200 <service-name>
```

## Web Interfaces

| Interface | URL | Default login |
|---|---|---|
| Grafana | <http://localhost:3000> | `admin` / `admin` |
| InfluxDB | <http://localhost:8086> | Values configured in `.env` |

The Flux queries used to create Grafana panels are available in [`grafana_code.md`](grafana_code.md).

## Stopping the Stack

Stop and remove the containers while preserving named volumes:

```powershell
docker compose down
```

Stop the containers without removing them:

```powershell
docker compose stop
```

## Replaying Data from the Beginning

Spark stores consumed Kafka offsets in a named checkpoint volume. To replay the data from `earliest`, stop Spark, remove only its checkpoint volume, and start it again:

```powershell
docker compose stop spark-streaming
docker volume rm docker_deployment_spark_checkpoints
docker compose up -d spark-streaming
```

Then rerun `data_generator.py`.

> **Warning:** Do not use `docker compose down -v` when you need to preserve InfluxDB or Grafana data. The `-v` option removes all named volumes, including `influxdb_data`, `grafana_data`, and `spark_checkpoints`.

To perform an intentional full reset:

```powershell
docker compose down -v
docker compose up -d --build
```

## Troubleshooting

### Kafka is not ready

Wait for the Kafka health check to complete, then inspect its logs:

```powershell
docker compose logs --tail=200 kafka
```

### Spark cannot find preprocessing artifacts

Verify that the `spark-streaming` volume points to:

```text
../Use case 3_CIC_IIOT_2025/saved_preprocessed
```

Also confirm that all required `.pkl` files exist in that directory.

### The inference daemon cannot load a model

Check the model volume, the filenames in `inference/models/ensemble.py`, and the container logs:

```powershell
docker compose logs --tail=200 inference-daemon
docker exec inference-daemon ls -lah /app/artifacts/model_final
```

### No data appears in Grafana

1. Confirm that the data generator is producing Kafka messages.
2. Check the Spark and inference logs for processed batches.
3. Verify the InfluxDB organization, bucket, and token in `.env`.
4. Select a dashboard time range that covers the original event timestamps in the replay dataset.

The CIC IIoT replay data currently spans approximately:

```text
2025-01-15T13:04:54.353Z to 2025-09-09T15:09:39.400Z
```
