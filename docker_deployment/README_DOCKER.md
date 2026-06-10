# NIDS Docker Pipeline

Cách chạy pipeline NIDS trong Docker:

- Docker chạy Kafka, InfluxDB, Grafana, Spark Streaming va Inference Daemon.
- `data_generator.py` chạy trên host, đẩy dữ liệu vào kafka qua `localhost:9092`.
- Cấu hình chính nằm trong file `.env`.

## 1. Start toan bo Docker services

Chạy trong thư mục `docker_deployment`:

```powershell
cd "C:\Users\Admin\Downloads\IoT Dataset\docker_deployment"
docker compose up -d --build
```

Kiểm tra trạng thái các container:

```powershell
docker ps
```

Kiểm tra health status của container:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Sau khi Docker services đã chạy, chạy generator trên host:

```powershell
cd "C:\Users\Admin\Downloads\IoT Dataset\docker_deployment"
python data_generator.py
```

## 2. Start từng service riêng lẻ

Start Kafka stack:

```powershell
docker compose up -d zookeeper kafka
```

Start InfluxDB va Grafana:

```powershell
docker compose up -d influxdb grafana
```

Build và start Spark Streaming:

```powershell
docker compose up -d --build spark-streaming
```

Build và start Inference Daemon:

```powershell
docker compose up -d --build inference-daemon
```

Restart riêng Spark Streaming:

```powershell
docker compose restart spark-streaming
```

Restart riêng Inference Daemon:

```powershell
docker compose restart inference-daemon
```

Stop riêng một service:

```powershell
docker compose stop spark-streaming
docker compose stop inference-daemon
```

## 3. Xem log Spark và Inference

Xem log Spark Streaming:

```powershell
docker compose logs -f spark-streaming
```

Xem 200 dòng log gần nhất cua Spark:

```powershell
docker compose logs --tail=200 spark-streaming
```

Xem log worker preprocessing bên trong container Spark:

```powershell
docker exec spark-streaming sh -lc "tail -n 120 /tmp/spark_worker_debug.log"
```

Xem log Inference Daemon:

```powershell
docker compose logs -f inference-daemon
```

Xem 200 dòng log gần nhất của Inference Daemon:

```powershell
docker compose logs --tail=200 inference-daemon
```

Kiem tra nhanh container đang chạy:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Nếu container hiện `(healthy)` thì service đã sẵn sàng. Nếu hiện `(starting)`, đợi thêm vài giây và chạy lại lệnh trên. Nếu hiện `(unhealthy)`, xem log service đó bằng `docker compose logs --tail=200 <service-name>`.

## 4. Reset Spark checkpoint để chạy lại từ đầu

Khi muốn chạy lại generator và đọc lại kafka theo `SPARK_STARTING_OFFSETS=earliest`, cần xóa riêng checkpoint của spark.

Dung cac lenh sau:

```powershell
cd "C:\Users\Admin\Downloads\IoT Dataset\docker_deployment"
docker compose stop spark-streaming
docker volume rm docker_deployment_spark_checkpoints
docker compose up -d spark-streaming
```

Khong dung lenh nay neu ban muon giu du lieu InfluxDB va Grafana:

```powershell
docker compose down -v
```

Ly do: `down -v` se xoa tat ca volume, bao gom ca:

- `docker_deployment_influxdb_data`
- `docker_deployment_grafana_data`
- `docker_deployment_spark_checkpoints`

## 5. Truy cap UI

InfluxDB:

```text
http://localhost:8086
```

Grafana:

```text
http://localhost:3000
```

Gia tri mac dinh nam trong `.env`:

```text
INFLUXDB_ORG=nids_org
INFLUXDB_BUCKET=nids_bucket
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
```
