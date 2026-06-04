from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional

class ProcessedFlow(BaseModel):
    """
    Schema định nghĩa cấu trúc của một luồng mạng đã qua tiền xử lý.
    Bất kỳ message nào từ Kafka không khớp schema này sẽ bị loại bỏ.
    """
    
    # Cấu hình Pydantic V2: 
    # extra='ignore': Tự động vứt bỏ các trường thừa từ Spark đẩy sang (nếu có)
    # strict=True: Ép kiểu nghiêm ngặt, chống trôi dữ liệu
    model_config = ConfigDict(extra='ignore', strict=True)

    # ==========================================
    # 1. THÔNG TIN META (Không đưa vào ma trận Tensor)
    # ==========================================
    timestamp: float = Field(..., description="Thời gian sự kiện thực tế (Event Time)")
    label: Optional[int] = Field(None, description="Nhãn (chỉ dùng để đối chiếu khi test, không đưa vào dự đoán)")

    # ==========================================
    # 2. ĐẶC TRƯNG ONE-HOT (Log Types & Protocols)
    # ==========================================
    log_type_array: float = Field(...)
    log_type_numeric: float = Field(...)
    log_type_string: float = Field(...)
    
    src_proto_arp: float = Field(...)
    dst_proto_arp: float = Field(...)
    
    # [BẠN PASTE CÁC CỘT PROTOCOL CÒN LẠI Ở ĐÂY]
    # Ví dụ: src_proto_data: float = Field(...)

    # ==========================================
    # 3. ĐẶC TRƯNG MẠNG NUMERICAL 
    # Lưu ý sử dụng `alias` cho các cột có dấu gạch ngang hoặc ký tự đặc biệt
    # ==========================================
    network_packet_size_avg: float = Field(..., alias="network_packet-size_avg")
    network_packet_size_max: float = Field(..., alias="network_packet-size_max")
    network_packet_size_min: float = Field(..., alias="network_packet-size_min")
    network_packet_size_std_deviation: float = Field(..., alias="network_packet-size_std_deviation")
    
    network_tcp_flags_avg: float = Field(..., alias="network_tcp-flags_avg")
    
    # [BẠN PASTE CÁC CỘT NUMERICAL CÒN LẠI Ở ĐÂY]

    # ==========================================
    # 4. FIELD VALIDATORS (Bắt lỗi Logic)
    # ==========================================
    @field_validator('timestamp')
    @classmethod
    def check_valid_timestamp(cls, v):
        """Đảm bảo timestamp luôn là một mốc thời gian dương hợp lệ"""
        if v <= 0:
            raise ValueError(f"Lỗi: Timestamp không hợp lệ ({v})")
        return v
        
    @field_validator('network_packet_size_avg', 'network_packet_size_max', 'network_packet_size_min')
    @classmethod
    def check_not_nan(cls, v):
        """Chặn đứng các giá trị NaN/Infinity lọt qua từ Spark"""
        # Tránh lỗi float('nan') hoặc float('inf') làm hỏng weight của GNN
        if v != v or v == float('inf') or v == float('-inf'):
            raise ValueError("Dữ liệu chứa NaN hoặc Infinity")
        return v

    def to_tensor_list(self) -> list:
        """
        Hàm tiện ích giúp xuất toàn bộ đặc trưng (loại trừ timestamp và label)
        thành một list float để dễ dàng biến thành PyTorch Tensor.
        """
        # Lấy tất cả giá trị dưới dạng dict
        data_dict = self.model_dump(exclude={'timestamp', 'label'})
        # Đảm bảo thứ tự cột luôn cố định theo thứ tự khai báo trong class
        return list(data_dict.values())