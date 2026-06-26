# Installation Guide

This file provides a brief summary of the required environment and setup steps for running the experiments and the deployment pipeline.

## Environment Requirements

- Operating system: Windows 10/11 or Linux
- Python: 3.11 or 3.12
- Recommended RAM: at least 16 GB
- Recommended disk space: enough for the downloaded datasets, trained models, and experiment outputs
- Optional GPU: NVIDIA GPU with CUDA support for faster deep-learning training and inference
- Optional deployment tools:
  - Docker Desktop
  - Docker Compose v2

## Python Environment Setup

Create and activate a virtual environment from the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the required Python packages:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The main dependencies include PyTorch, PyTorch Geometric, XGBoost, LightGBM, CatBoost, scikit-learn, pandas, PyArrow, JupyterLab, PySpark, Kafka client libraries, and InfluxDB client libraries.

## Dataset Setup

The raw datasets are not included in this repository. Download the required preprocessed datasets from the following Google Drive folders:

| Use case | Dataset folder | Google Drive link |
|---|---|---|
| 1 | `http_based_preprocessed` | [Download](https://drive.google.com/drive/folders/1PQq5GPuusSG-_hsv2hBtqOei_AC_64ff?usp=sharing) |
| 2 | `tcp_based_preprocessed` | [Download](https://drive.google.com/drive/folders/1VNV_FFIQkyCVYppzTRgWL0QwmKuPDN8W?usp=sharing) |
| 3 | `CIC_IIOT_data_preprocessed` | [Download](https://drive.google.com/drive/folders/10HZ656RApIDO-7P8rTR91ygqo_txPXPv?usp=sharing) |

Place the downloaded files in the corresponding use-case directories:

```text
Use case 1_HTTP_based_IOT_ZWave_2025/
Use case 2_Application_based_IOT_ZWave_2025/
Use case 3_CIC_IIOT_2025/
```

For the Docker deployment, the CIC IIoT preprocessed artifacts should be available under:

```text
Use case 3_CIC_IIOT_2025/saved_preprocessed/
```

## Running Experiments

Start JupyterLab from the repository root:

```powershell
jupyter lab
```

Run the notebooks in the following general order:

1. Preprocessing notebooks, if the preprocessed files need to be regenerated.
2. Baseline experiment notebooks.
3. GAT-XGBoost experiment notebooks.
4. Residual CNN-BiLSTM-Attention experiment notebooks.
5. Meta-learner and SOTA comparison notebooks.

Some notebooks may contain absolute local paths from the development environment. Update the data and model paths near the beginning of each notebook before running them on another machine.

## Running the Docker Deployment

The Docker deployment currently targets use case 3, CIC IIoT 2025.

First, make sure Docker Desktop is running. Then review the configuration file:

```text
docker_deployment/.env
```

Start the deployment stack:

```powershell
cd docker_deployment
docker compose up -d --build
```

Run the host-side data generator:

```powershell
$env:DATA_PATH="..\Use case 3_CIC_IIOT_2025\saved_preprocessed\data_1s.parquet"
python data_generator.py
```

Useful web interfaces:

- Grafana: <http://localhost:3000>
- InfluxDB: <http://localhost:8086>

Stop the deployment stack:

```powershell
docker compose down
```

More detailed deployment instructions are available in:

```text
docker_deployment/README_DOCKER.md
```
