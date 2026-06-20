processed_links = [r"ip-files-fillna-lstm\APACHE-MHDDoS-fillna.csv", 
                   r"ip-files-fillna-lstm\BYPASS-MHDDoS-1-fillna.csv",
                   r"ip-files-fillna-lstm\BYPASS-MHDDoS-2-fillna.csv",
                   r"ip-files-fillna-lstm\BYPASS-MHDDoS-fillna.csv",
                   r"ip-files-fillna-lstm\CFB-MHDDoS-fillna.csv",
                   r"ip-files-fillna-lstm\DGB-MHDDoS-fillna.csv",
                   r"ip-files-fillna-lstm\HEAD-MHDDoS-fillna.csv",
                   r"ip-files-fillna-lstm\HTTP-Flood-Xerox-fillna.csv",
                   r"ip-files-fillna-lstm\KILLER-MHDDoS-fillna.csv",
                   r"ip-files-fillna-lstm\Mixed-Get-Post-Head-fillna.csv",
                   r"ip-files-fillna-lstm\NULL-MHDDoS-fillna.csv",
                   r"ip-files-fillna-lstm\POST-Flood-MHDDoS-fillna.csv",
                   r"ip-files-fillna-lstm\STOMP-MHDDoS-fillna.csv",
]

def reduce_mem_usage(df, filename=""):
    """
    Tự động quét qua các cột và ép kiểu dữ liệu xuống để tiết kiệm RAM.
    Đồng thời đưa các cột bị lỗi object/mixed types dứt khoát về dạng string.
    """
    start_mem = df.memory_usage().sum() / 1024**2
    
    for col in df.columns:
        col_type = df[col].dtype
        
        # Chỉ xử lý các cột dạng số (không phải chuỗi/object)
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            
            # Xử lý số Nguyên (Integer)
            if pd.api.types.is_integer_dtype(col_type):
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            
            # Xử lý số Thực (Float) - Ta ép về float32 là tốt nhất cho LSTM/CNN
            elif pd.api.types.is_float_dtype(col_type):
                if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            # Sửa Fix lỗi Parquet ArrowTypeError cho các cột object lẫn lộn
            # Ép ép dứt khoát tất cả object về kiểu chuỗi (string) để pyarrow không crash
            df[col] = df[col].astype(str)
                    
    end_mem = df.memory_usage().sum() / 1024**2
    if filename:
        print(f'[{filename}] Giảm bộ nhớ: {start_mem:.2f} MB -> {end_mem:.2f} MB (Tiết kiệm {100 * (start_mem - end_mem) / start_mem:.1f}%)')
    
    return df

import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import QuantileTransformer, LabelEncoder

def process_chunk_based_split():
    train_list = []
    valid_list = []
    test_list = []
    num_chunks = 3

    # 1. Đọc các file và chia 10 khối
    for link in processed_links:
        print(f"[{link}] Reading and processing...")
        df = pd.read_csv(link)
        df = df.sort_values(by = "timestamp").reset_index(drop = True)
        # 2. Xử lý ép kiểu dữ liệu giảm RAM
        df = reduce_mem_usage(df, filename=link)

        # 3. Chia Cách 1: 10 chunks cố định, lấy 65-15-20 mỗi chunk
        chunk_size = len(df) // num_chunks
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size if i < num_chunks - 1 else len(df)
            chunk = df.iloc[start_idx:end_idx]
            
            train_bound = int(len(chunk) * 0.65)
            valid_bound = train_bound + int(len(chunk) * 0.15)
            
            train_list.append(chunk.iloc[:train_bound])
            valid_list.append(chunk.iloc[train_bound:valid_bound])
            test_list.append(chunk.iloc[valid_bound:])

    print("--- Đang gộp các DataFrames lại... ---")
    train_df = pd.concat(train_list, ignore_index=True)
    valid_df = pd.concat(valid_list, ignore_index=True)
    test_df = pd.concat(test_list, ignore_index=True)

    # 4. Ép 2 cột thời lượng về numeric, giá trị không hợp lệ -> 0.0
    time_cols = ["delta_start", "handshake_duration"]
    for df in [train_df, valid_df, test_df]:
        for col in time_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # 5. Thay thế Inf bằng max, -Inf bằng min trên bản thân TỪNG DataFrame
    def resolve_infinity(df_target):
        num_cols = df_target.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            has_inf_pos = (df_target[col] == np.inf).any()
            has_inf_neg = (df_target[col] == -np.inf).any()
            
            if has_inf_pos or has_inf_neg:
                max_val = df_target.loc[df_target[col] != np.inf, col].max()
                min_val = df_target.loc[df_target[col] != -np.inf, col].min()
                
                # Biện pháp phòng hờ: Nếu toàn tập là inf dẫn đến max là NaN
                if pd.isna(max_val): max_val = 0.0
                if pd.isna(min_val): min_val = 0.0
                
                df_target[col] = df_target[col].replace(np.inf, max_val)
                df_target[col] = df_target[col].replace(-np.inf, min_val)
        return df_target
        
    print("--- Đang xử lý các giá trị Infinity... ---")
    train_df = resolve_infinity(train_df)
    valid_df = resolve_infinity(valid_df)
    test_df = resolve_infinity(test_df)

    # 7. Bỏ các cột có <= 1 giá trị ở train_df rồi áp dụng lên valid và test
    print("--- Loại bỏ các cột mang giá trị hằng số (chỉ có 1 unique value)... ---")
    constant_cols = [col for col in train_df.columns if train_df[col].nunique() <= 1]
    train_df.drop(columns=constant_cols, inplace=True, errors='ignore')
    valid_df.drop(columns=constant_cols, inplace=True, errors='ignore')
    test_df.drop(columns=constant_cols, inplace=True, errors='ignore')

    # Trích các cột numerical để chuẩn bị scale
    numeric_cols = train_df.select_dtypes(include=[np.number]).columns.tolist()
    if "label" in numeric_cols:
        numeric_cols.remove("label")

    # 6. Dùng QuantileTransformer (Fit trên Train -> Transform cho 3 tập)
    print("--- Scale dữ liệu bằng QuantileTransformer... ---")
    scaler = QuantileTransformer(output_distribution= "normal", random_state=42)
    train_df[numeric_cols] = scaler.fit_transform(train_df[numeric_cols])
    valid_df[numeric_cols] = scaler.transform(valid_df[numeric_cols])
    test_df[numeric_cols] = scaler.transform(test_df[numeric_cols])

    # 8. Label Encoding
    if "label" in train_df.columns:
        print("--- Đang Label Encoding... ---")
        le = LabelEncoder()
        train_df["label"] = le.fit_transform(train_df["label"].astype(str))
        
        # Đề phòng Valid/Test có nhãn không tồn tại trong Train (rất hiếm vì đã chia chu kỳ)
        known_classes = set(le.classes_)
        def safe_transform_labels(series):
            return le.transform(series.astype(str).map(lambda x: x if x in known_classes else le.classes_[0]))

        valid_df["label"] = safe_transform_labels(valid_df["label"])
        test_df["label"] = safe_transform_labels(test_df["label"])

    # 9. Lưu vào folder chunk-based-split theo định dạng .parquet
    save_folder = os.path.join("final_data", "chunk-based-split-3")
    os.makedirs(save_folder, exist_ok=True)
    
    print("--- Đang lưu DataFrames sang định dạng Parquet... ---")
    train_df.to_parquet(os.path.join(save_folder, "train_df_prepared.parquet"), index=False)
    valid_df.to_parquet(os.path.join(save_folder, "valid_df_prepared.parquet"), index=False)
    test_df.to_parquet(os.path.join(save_folder, "test_df_prepared.parquet"), index=False)
    
    print(f"\n[Hoàn tất] File đã được lưu thành công trong: {save_folder}")
    print(f"Kích thước Train: {train_df.shape}")
    print(f"Kích thước Valid: {valid_df.shape}")
    print(f"Kích thước Test:  {test_df.shape}")

if __name__ == "__main__":
    process_chunk_based_split()