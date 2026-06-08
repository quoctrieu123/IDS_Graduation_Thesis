import os
import json
import logging
import torch
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import ValidationError

# Import các module nội bộ
from schemas import ProcessedFlow
from data_builder import BufferManager
from models.ensemble import EnsembleManager

# ==========================================
# CẤU HÌNH HỆ THỐNG
# ==========================================
# Cấu hình luồng
CHUNK_SIZE = 256
KAFKA_TOPIC = "processed_flows"
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

# Cấu hình InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "nids_org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "nids_bucket")
# Trong InfluxDB v2, ta dùng Token thay cho user:pass
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "nids-super-secret-token-12345") 

# Thiết lập Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. KHỞI TẠO THIẾT BỊ (Tận dụng sức mạnh RTX 4070 Super)
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logger.info(f"🚀 Khởi động NIDS Daemon. Môi trường tính toán: {device.upper()}")

    # 2. KHỞI TẠO CÁC MODULE AI
    logger.info("Đang nạp trọng số mô hình vào VRAM...")
    buffer_manager = BufferManager(seq_time_steps=10, graph_window_size=50, max_dt=30.0)
    # khơi tạo các mô hình
    try:
        ensemble_manager = EnsembleManager(device=device)
    except Exception as e:
        logger.error(f"Lỗi khi load mô hình (Kiểm tra lại đường dẫn /app/artifacts/): {e}")
        return

    # 3. KHỞI TẠO INFLUXDB CLIENT
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    # 4. KHỞI TẠO KAFKA CONSUMER
    logger.info(f"Đang kết nối tới Kafka Broker tại {KAFKA_SERVER}...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        group_id='inference_group_replay_3',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest', # Đọc luồng mới nhất để tránh độ trễ tích lũy
        enable_auto_commit=True,
    )
    
    # Bộ đệm tạm thời để gom đủ CHUNK_SIZE
    current_chunk = []
    current_chunk_meta = []
    
    logger.info("🎧 Bắt đầu lắng nghe luồng dữ liệu mạng...")

    # 5. VÒNG LẶP SUY LUẬN (INFERENCE LOOP)
    for message in consumer:
        try:
            # tạo pydantic để kiểm duyệt dữ liệu
            flow = ProcessedFlow(**message.value)
            current_chunk.append(flow)
            current_chunk_meta.append({
                "flow_id": f"{message.topic}-{message.partition}-{message.offset}",
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
            })
            
        except ValidationError as e:
            logger.warning(f"Bỏ qua gói tin lỗi định dạng: {e.errors()[0]['msg']}")
            continue

        # KHI ĐÃ GOM ĐỦ 256 GÓI TIN MỚI
        if len(current_chunk) == CHUNK_SIZE:
            # a. Xây dựng Đầu vào (Tensors & Graphs)
            seq_x, graph_x, edge_idx, edge_attr, target_idx = buffer_manager.build_inputs(current_chunk)
            logger.info("Build input stats: %s", buffer_manager.last_build_stats)
            
            # Nếu seq_x là None nghĩa là hệ thống mới bật, chưa đủ 10 lịch sử
            if seq_x is not None:
                # chạy infer cho cả hệ thống
                predictions = ensemble_manager.predict(seq_x, graph_x, edge_idx, edge_attr, target_idx)
                
                # c. Ghi kết quả vào InfluxDB (Batch Writing)
                points = []
                # Số lượng predictions sinh ra luôn khớp hoàn hảo với (CHUNK_SIZE - số flow thiếu lịch sử)
                num_preds = len(predictions)
                # Các flow hợp lệ nằm ở cuối của current_chunk
                valid_flows = current_chunk[-num_preds:]
                valid_meta = current_chunk_meta[-num_preds:]
                
                # tạo các point để batch write vào influxdb
                for flow, meta, pred in zip(valid_flows, valid_meta, predictions):
                    point = Point("network_flow") \
                        .tag("flow_id", meta["flow_id"]) \
                        .tag("src_ip", flow.network_ips_src) \
                        .tag("dst_ip", flow.network_ips_dst) \
                        .field("predicted_label", int(pred)) \
                        .field("kafka_partition", int(meta["partition"])) \
                        .field("kafka_offset", int(meta["offset"])) \
                        .time(int(flow.timestamp * 1e9), WritePrecision.NS) # Ghi chuẩn xác tới nano-giây
                    points.append(point)
                
                # ghi batch vào influxdb
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
                logger.info(f"✅ Đã xử lý và đẩy {num_preds} kết quả dự đoán lên InfluxDB.")
            else:
                logger.info("⏳ Chunk đầu tiên đang gom lịch sử, chưa thực hiện dự đoán.")
            
            # dọn rác để sẵn sàng cho chunk tiếp theo
            current_chunk.clear()
            current_chunk_meta.clear()

if __name__ == "__main__":
    main()
