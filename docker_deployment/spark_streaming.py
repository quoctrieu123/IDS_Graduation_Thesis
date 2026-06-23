# file tiền xử lý dữ liệu
# đọc dữ liệu từ kafka topic raw_flows, tiền xử lý, và đẩy vào topic processed_flows
import pandas as pd
import numpy as np
import ast
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from spark_schema import raw_schema, output_schema
import pyspark
import os
import sys
import traceback
import datetime
from pathlib import Path
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, DoubleType, LongType, BooleanType
)

DEPLOYMENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DEPLOYMENT_DIR.parent

DEFAULT_SAVED_PREPROCESSED_DIR = PROJECT_ROOT / "Use case 3_CIC_IIOT_2025" / "saved_preprocessed"
DEFAULT_SPARK_CHECKPOINT = (DEPLOYMENT_DIR / "spark_checkpoints_cciot").resolve().as_uri()
DEFAULT_SPARK_WORKER_LOG = DEPLOYMENT_DIR / "spark_worker_debug.log"

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw_flows")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "processed_flows")
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "earliest")
SPARK_CHECKPOINT = os.getenv("SPARK_CHECKPOINT", DEFAULT_SPARK_CHECKPOINT)
SAVED_PREPROCESSED_DIR = os.getenv("SAVED_PREPROCESSED_DIR", str(DEFAULT_SAVED_PREPROCESSED_DIR))
SPARK_WORKER_LOG = os.getenv("SPARK_WORKER_LOG", str(DEFAULT_SPARK_WORKER_LOG))

if os.name == "nt":
    hadoop_home = os.getenv("HADOOP_HOME", r"C:\hadoop-3.0.0")
    if hadoop_home:
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
# Cấu hình cứng để luôn gọi đúng thư viện của bản 3.5.1
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["ARROW_DEFAULT_MEMORY_POOL"] = "system"

kafka_package = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")

spark = SparkSession.builder \
    .appName("CCIOT_Streaming_Preprocessor") \
    .config("spark.jars.packages", kafka_package) \
    .getOrCreate()

# ==========================================
# 2. ĐỊNH NGHĨA SCHEMA (Mô phỏng)
# ==========================================
# Schema đầu vào (từ Topic raw_flows) - Thay bằng cấu trúc file parquet gốc của bạn
# đối với các cột thừa trong raw_schema, sẽ tự động bỏ qua
# đối với các cột thiếu, giá trị được điền là null
raw_schema = raw_schema  # Đã được định nghĩa trong spark_schema.py, đảm bảo khớp với cấu trúc JSON từ Kafka

# đối với các cột thiếu: báo lỗi runtime error
# đối với các cột thừa: tự động bỏ qua
output_schema = output_schema  # Đã được định nghĩa trong spark_schema.py, đảm bảo có đủ 138 cột sau khi xử lý

# ==========================================
# 3. HÀM XỬ LÝ MICRO-BATCH (Pandas UDF)
# ==========================================
def parse_string_to_list(val):
    try:
        if isinstance(val, str):
            return ast.literal_eval(val)
        return val if isinstance(val, list) else []
    except (ValueError, SyntaxError):
        return []

def map_to_frequent(proto_list,freq_set):
    res = set()
    for p in proto_list:
        if p in freq_set:
            res.add(p)
        else:
            res.add("other")
    return list(res)

def log_to_worker_file(msg):
    # Chọn một đường dẫn an toàn trên máy bạn để lưu log
    log_path = SPARK_WORKER_LOG
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.datetime.now()}] {msg}\n")

def artifact_path(filename):
    return os.path.join(SAVED_PREPROCESSED_DIR, filename)

def preprocess_micro_batch(iterator):
    log_to_worker_file("Khởi động Worker mới...")
    # Khởi tạo artifacts một lần trên mỗi Worker để tối ưu RAM
    try:
        scaler = joblib.load(artifact_path("quantile_scaler.pkl"))
        cols_to_drop = joblib.load(artifact_path("cols_to_drop.pkl"))
        numeric_cols = joblib.load(artifact_path("numeric_columns.pkl"))
        # Load các MultiLabelBinarizer đã fit từ lúc train
        mlb_log = joblib.load(artifact_path("mlb_log_data_types.pkl"))
        mlb_src = joblib.load(artifact_path("mlb_network_protocols_src.pkl"))
        mlb_dst = joblib.load(artifact_path("mlb_network_protocols_dst.pkl"))
        freq_src_protos = joblib.load(artifact_path("freq_network_protocols_src.pkl"))
        freq_dst_protos = joblib.load(artifact_path("freq_network_protocols_dst.pkl"))
    except Exception as e:
        log_to_worker_file(f"Lỗi khi loat model: {traceback.format_exc()}")
        raise e
    expected_cols = output_schema.fieldNames()

    for pdf in iterator:
        if pdf.empty:
            yield pdf
            continue
        try:
            log_to_worker_file(f"-> Nhận micro-batch với {len(pdf)} dòng. Số cột ban đầu: {len(pdf.columns)}")
            current_drop = [c for c in cols_to_drop if c in pdf.columns]
            pdf.drop(columns=current_drop, inplace=True, errors='ignore')


            # B. Parse list và áp dụng One-hot encoding (MultiLabelBinarizer)
            if 'log_data-types' in pdf.columns:
                parsed = pdf['log_data-types'].apply(parse_string_to_list)
                bin_matrix = mlb_log.transform(parsed)
                new_cols = [f"log_type_{c}" for c in mlb_log.classes_]
                df_bin = pd.DataFrame(bin_matrix, columns=new_cols, index=pdf.index)
                pdf = pd.concat([pdf, df_bin], axis=1).drop(columns=['log_data-types'])

            if 'network_protocols_src' in pdf.columns:
                parsed = pdf['network_protocols_src'].apply(parse_string_to_list)
                grouped = parsed.apply(lambda x: map_to_frequent(x, freq_src_protos))
                bin_matrix = mlb_src.transform(grouped)
                new_cols = [f"src_proto_{c}" for c in mlb_src.classes_]
                df_bin = pd.DataFrame(bin_matrix, columns=new_cols, index=pdf.index)
                pdf = pd.concat([pdf, df_bin], axis=1).drop(columns=['network_protocols_src'])

            if 'network_protocols_dst' in pdf.columns:
                parsed = pdf['network_protocols_dst'].apply(parse_string_to_list)
                grouped = parsed.apply(lambda x: map_to_frequent(x, freq_dst_protos))
                bin_matrix = mlb_dst.transform(grouped)
                new_cols = [f"dst_proto_{c}" for c in mlb_dst.classes_]
                df_bin = pd.DataFrame(bin_matrix, columns=new_cols, index=pdf.index)
                pdf = pd.concat([pdf, df_bin], axis=1).drop(columns=['network_protocols_dst'])
            log_to_worker_file("-> Xong phần parse list và One-hot.")
            if "timestamp_start" in pdf.columns:
                pdf.rename(columns={"timestamp_start": "timestamp"}, inplace=True)


            # E. Lấy danh sách cột số và áp dụng QuantileTransformer
            current_numeric_cols = numeric_cols.copy()
            if 'label' in current_numeric_cols: current_numeric_cols.remove('label')
            if 'timestamp' in current_numeric_cols: current_numeric_cols.remove('timestamp')

            valid_numeric_cols = [c for c in current_numeric_cols if c in pdf.columns]
            log_to_worker_file(f"-> Chuẩn bị scale {len(valid_numeric_cols)} cột numeric.")
            if valid_numeric_cols:
                pdf[valid_numeric_cols] = scaler.transform(pdf[valid_numeric_cols])
            log_to_worker_file("-> Scale thành công. Chuẩn bị mapping Schema.")
            pdf = pdf.reindex(columns=expected_cols, fill_value=0)
            for field in output_schema.fields:
                col_name = field.name

                # Bắt buộc ép về đúng type của Spark Arrow
                if isinstance(field.dataType, IntegerType):
                    pdf[col_name] = pdf[col_name].fillna(0).astype('int32')

                elif isinstance(field.dataType, LongType):
                    pdf[col_name] = pdf[col_name].fillna(0).astype('int64')

                elif isinstance(field.dataType, FloatType):
                    pdf[col_name] = pdf[col_name].fillna(0.0).astype('float32')

                elif isinstance(field.dataType, DoubleType):
                    pdf[col_name] = pdf[col_name].fillna(0.0).astype('float64')

                elif isinstance(field.dataType, BooleanType):
                    pdf[col_name] = pdf[col_name].fillna(False).astype(bool)

                elif isinstance(field.dataType, StringType):
                    pdf[col_name] = pdf[col_name].fillna("").astype(str)
                else:
                    log_to_worker_file(f"CẢNH BÁO NGHIÊM TRỌNG: Cột '{col_name}' có kiểu {field.dataType} chưa được ép kiểu!")
            pdf.reset_index(drop=True, inplace=True)
            log_to_worker_file(f"-> Hoàn tất batch. Chuẩn bị yield {len(pdf.columns)} cột.")
            #expected_cols = output_schema.fieldNames()
            #pdf = pdf[expected_cols].fillna(0)
            # Trả về dataframe Pandas cho Spark
            pdf_contiguous = pdf.copy()
            yield pdf_contiguous
        except Exception as e:
            error_details = (
                f"\n!!! CRASH TRONG QUÁ TRÌNH XỬ LÝ BATCH !!!\n"
                f"Lỗi: {str(e)}\n"
                f"Chi tiết (Traceback):\n{traceback.format_exc()}\n"
                f"Danh sách các cột hiện tại lúc bị crash:\n{list(pdf.columns)}\n"
            )
            log_to_worker_file(error_details)
            raise e # Vẫn ném lỗi ra ngoài để Spark biết luồng này hỏng
# ==========================================
# 4. KẾT NỐI LUỒNG KAFKA VÀ CHẠY
# ==========================================
# Đọc luồng từ Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", RAW_TOPIC) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .load()

# Parse JSON
df_parsed = df_kafka.select(from_json(col("value").cast("string"), raw_schema).alias("data")).select("data.*")

# Áp dụng Pandas UDF
df_processed = df_parsed.mapInPandas(preprocess_micro_batch, schema=output_schema)

# Đóng gói lại thành JSON và đẩy sang topic processed_flows
query = df_processed \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("topic", PROCESSED_TOPIC) \
    .option("checkpointLocation", SPARK_CHECKPOINT) \
    .start()

query.awaitTermination()
