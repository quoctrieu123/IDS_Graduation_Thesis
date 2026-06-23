# đọc file parquet, lấy từng dòng dữ liệu, chuyển timestmap_start sang float (giây), và đẩy vào Kafka topic theo đúng thứ tự thời gian
import time
import json
import os
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

# --- CẤU HÌNH ---
SPEED_FACTOR = 10000.0  # Chỉnh > 1.0 để tua nhanh thời gian (vd: 10.0 là nhanh gấp 10 lần)
KAFKA_TOPIC = 'raw_flows'
KAFKA_SERVER = 'localhost:9092'
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_PATH = PROJECT_ROOT / "Use case 3_CIC_IIOT_2025" / "saved_preprocessed" / "data_1s.parquet"
DATA_PATH = str(DEFAULT_DATA_PATH)

SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", str(SPEED_FACTOR)))
KAFKA_TOPIC = os.getenv("RAW_TOPIC", KAFKA_TOPIC)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", KAFKA_SERVER)
DATA_PATH = os.getenv("DATA_PATH", DATA_PATH)

def main():
    print(f"Đang đọc dữ liệu từ {DATA_PATH}...")
    df = pd.read_parquet(DATA_PATH)

    # --- TIỀN XỬ LÝ THỜI GIAN THEO YÊU CẦU ---
    # Ép kiểu chuỗi ISO8601 sang datetime của Pandas, có nhận diện múi giờ UTC
    df['timestamp_start'] = pd.to_datetime(df['timestamp_start'], format='ISO8601', utc=True)

    # Chuyển datetime sang số nguyên nanoseconds, sau đó chia 1 tỷ (1e9)
    # để lấy số giây dưới dạng float (chính xác đến microsecond)
    df['timestamp_start'] = df['timestamp_start'].astype('int64') / 1e9

    # Bắt buộc: Sắp xếp lại toàn bộ dataset theo trục thời gian từ quá khứ đến hiện tại
    df = df.sort_values(by='timestamp_start').reset_index(drop=True)

    # --- KHỞI TẠO KAFKA PRODUCER ---
    print(f"Đang kết nối tới Kafka Broker tại {KAFKA_SERVER}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        # Hàm tự động parse dictionary sang chuỗi JSON và mã hóa thành bytes
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("Bắt đầu đẩy luồng dữ liệu (Streaming)...")

    # Lấy mốc thời gian của flow đầu tiên làm "điểm neo"
    prev_time = df.iloc[0]['timestamp_start']
    total_flows = len(df)

    for index, row in df.iterrows():
        current_time = row['timestamp_start']

        # Bước 1: Tính khoảng cách thời gian thực (Event Time Delta)
        delta_t_real = current_time - prev_time

        # Bước 2: Tính thời gian chờ mô phỏng (Processing Time Delta)
        delta_t_sim = delta_t_real / SPEED_FACTOR

        # Bước 3: Dừng chương trình (sleep) nếu có khoảng cách thời gian
        # (Nếu delta_t_sim = 0, nghĩa là các gói tin đến cùng 1 microsecond, sẽ gửi đi ngay lập tức)
        if delta_t_sim > 0:
            time.sleep(delta_t_sim)

        # Bước 4: Chuyển đổi dòng dữ liệu thành Dictionary (bao gồm cả cột timestamp float)
        payload = row.to_dict()

        # Bước 5: Bắn vào Kafka
        producer.send(KAFKA_TOPIC, value=payload)

        # Cập nhật mốc thời gian cho vòng lặp kế tiếp
        prev_time = current_time

        # In log tiến độ để dễ theo dõi
        if index % 1000 == 0:
            print(f"Đã stream {index}/{total_flows} flows...")

    # Đảm bảo toàn bộ message còn kẹt trong buffer của Producer được đẩy đi hết
    producer.flush()
    print("Hoàn tất toàn bộ đường ống dữ liệu!")

if __name__ == "__main__":
    main()
