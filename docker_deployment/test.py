import pandas as pd
data = pd.read_parquet(r'C:\Users\Admin\Downloads\IoT Dataset\CCIOT\saved_preprocessed\data_1s.parquet')
print(data.columns)

# in ra timestamp_start nhỏ nhất và lớn nhất
print("Timestamp Start nhỏ nhất:", data['timestamp_start'].min())
print("Timestamp Start lớn nhất:", data['timestamp_start'].max())