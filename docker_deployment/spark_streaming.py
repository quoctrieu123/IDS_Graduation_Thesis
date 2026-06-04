import pandas as pd
import numpy as np
import ast
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# ==========================================
# 1. KHỞI TẠO SPARK SESSION
# ==========================================
spark = SparkSession.builder \
    .appName("CCIOT_Streaming_Preprocessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# ==========================================
# 2. ĐỊNH NGHĨA SCHEMA (Mô phỏng)
# ==========================================
# Schema đầu vào (từ Topic raw_flows) - Thay bằng cấu trúc file parquet gốc của bạn
raw_schema = StructType([
    StructField("timestamp_start", StringType(), True),
    StructField("label2", StringType(), True),
    StructField("log_data-types", StringType(), True),
    StructField("network_protocols_src", StringType(), True),
    StructField("network_protocols_dst", StringType(), True),
    # ... Thêm các cột raw khác vào đây
])

# Schema đầu ra (từ hàm mapInPandas ra Topic processed_flows) - Phải khớp 137 cột
output_schema = StructType([
    StructField("timestamp", FloatType(), True),
    StructField("label", IntegerType(), True),
    StructField("log_type_array", FloatType(), True),
    StructField("log_type_numeric", FloatType(), True),
    StructField("log_type_string", FloatType(), True),
    StructField("src_proto_arp", FloatType(), True),
    # ... Định nghĩa toàn bộ 137 cột (float/int) vào đây
])

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

def preprocess_micro_batch(iterator):
    # Khởi tạo artifacts một lần trên mỗi Worker để tối ưu RAM
    scaler = joblib.load("/app/artifacts/quantile_scaler.joblib")
    le = joblib.load("/app/artifacts/label_encoder.joblib")
    cols_to_drop = joblib.load("/app/artifacts/dropped_columns.joblib")
    
    # Load các MultiLabelBinarizer đã fit từ lúc train
    mlb_log = joblib.load("/app/artifacts/mlb_log_types.joblib")
    mlb_src = joblib.load("/app/artifacts/mlb_src_proto.joblib")
    mlb_dst = joblib.load("/app/artifacts/mlb_dst_proto.joblib")
    
    for pdf in iterator:
        if pdf.empty:
            yield pdf
            continue
            
        # A. Đổi tên cột
        if "timestamp_start" in pdf.columns:
            pdf.rename(columns={"timestamp_start": "timestamp", "label2": "label"}, inplace=True)
            
        # B. Parse list và áp dụng One-hot encoding (MultiLabelBinarizer)
        if 'log_data-types' in pdf.columns:
            parsed = pdf['log_data-types'].apply(parse_string_to_list)
            bin_matrix = mlb_log.transform(parsed)
            new_cols = [f"log_type_{c}" for c in mlb_log.classes_]
            df_bin = pd.DataFrame(bin_matrix, columns=new_cols, index=pdf.index)
            pdf = pd.concat([pdf, df_bin], axis=1).drop(columns=['log_data-types'])

        if 'network_protocols_src' in pdf.columns:
            parsed = pdf['network_protocols_src'].apply(parse_string_to_list)
            bin_matrix = mlb_src.transform(parsed)
            new_cols = [f"src_proto_{c}" for c in mlb_src.classes_]
            df_bin = pd.DataFrame(bin_matrix, columns=new_cols, index=pdf.index)
            pdf = pd.concat([pdf, df_bin], axis=1).drop(columns=['network_protocols_src'])

        if 'network_protocols_dst' in pdf.columns:
            parsed = pdf['network_protocols_dst'].apply(parse_string_to_list)
            bin_matrix = mlb_dst.transform(parsed)
            new_cols = [f"dst_proto_{c}" for c in mlb_dst.classes_]
            df_bin = pd.DataFrame(bin_matrix, columns=new_cols, index=pdf.index)
            pdf = pd.concat([pdf, df_bin], axis=1).drop(columns=['network_protocols_dst'])

        # C. Xóa các cột không cần thiết
        current_drop = [c for c in cols_to_drop if c in pdf.columns]
        pdf.drop(columns=current_drop, inplace=True, errors='ignore')

        # D. Xử lý Label (Nếu có luồng nhãn đi kèm để test)
        if 'label' in pdf.columns:
            # Xử lý nhãn lạ bằng cách gán về nhãn benign (hoặc nhãn mặc định lớp 0)
            known_classes = set(le.classes_)
            pdf['label'] = pdf['label'].map(lambda x: x if x in known_classes else le.classes_[0])
            pdf['label'] = le.transform(pdf['label'])

        # E. Lấy danh sách cột số và áp dụng QuantileTransformer
        numeric_cols = pdf.select_dtypes(include=['number']).columns.tolist()
        if 'label' in numeric_cols: numeric_cols.remove('label')
        if 'timestamp' in numeric_cols: numeric_cols.remove('timestamp')
        
        # Đảm bảo thứ tự cột numeric khớp với lúc fit scaler
        # (Khuyến nghị: Nên lưu danh sách numeric_cols lúc train ra joblib và dùng lại ở đây)
        pdf[numeric_cols] = scaler.transform(pdf[numeric_cols])
        
        # Trả về dataframe Pandas cho Spark
        yield pdf

# ==========================================
# 4. KẾT NỐI LUỒNG KAFKA VÀ CHẠY
# ==========================================
# Đọc luồng từ Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_flows") \
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
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed_flows") \
    .option("checkpointLocation", "/tmp/spark_checkpoints_cciot") \
    .start()

query.awaitTermination()