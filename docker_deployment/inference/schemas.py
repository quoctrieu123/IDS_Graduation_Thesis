# file giúp kiểm duyệt dữ liệu đầu vào từ topic "processed flow" của Kafka, đảm bảo chỉ những gói tin hợp lệ mới được đưa vào pipeline suy luận
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

    # các cột chứa tập hợp cần parse cho GNN
    network_ips_dst: str = Field(...)
    network_ips_src: str = Field(...)
    network_ports_dst: str = Field(...)
    network_ports_src: str = Field(...)


    # ==========================================
    # 2. ĐẶC TRƯNG ONE-HOT (Log Types & Protocols)
    # ==========================================
    log_data_ranges_avg: float = Field(..., alias="log_data-ranges_avg")
    log_data_ranges_max: float = Field(..., alias="log_data-ranges_max")
    log_data_ranges_min: float = Field(..., alias="log_data-ranges_min")
    log_data_ranges_std_deviation: float = Field(..., alias="log_data-ranges_std_deviation")
    log_data_types_count: float = Field(..., alias="log_data-types_count")
    log_interval_messages: float = Field(..., alias="log_interval-messages")
    log_messages_count: float = Field(...)

    # ==========================================
    # 3. ĐẶC TRƯNG MẠNG NUMERICAL 
    # Lưu ý sử dụng `alias` cho các cột có dấu gạch ngang hoặc ký tự đặc biệt
    # ==========================================
    network_fragmentation_score: float = Field(..., alias="network_fragmentation-score")
    network_fragmented_packets: float = Field(..., alias="network_fragmented-packets")
    network_header_length_avg: float = Field(..., alias="network_header-length_avg")
    network_header_length_max: float = Field(..., alias="network_header-length_max")
    network_header_length_min: float = Field(..., alias="network_header-length_min")
    network_header_length_std_deviation: float = Field(..., alias="network_header-length_std_deviation")
    network_interval_packets: float = Field(..., alias="network_interval-packets")
    
    network_ip_flags_avg: float = Field(..., alias="network_ip-flags_avg")
    network_ip_flags_max: float = Field(..., alias="network_ip-flags_max")
    network_ip_flags_min: float = Field(..., alias="network_ip-flags_min")
    network_ip_flags_std_deviation: float = Field(..., alias="network_ip-flags_std_deviation")
    network_ip_length_avg: float = Field(..., alias="network_ip-length_avg")
    network_ip_length_max: float = Field(..., alias="network_ip-length_max")
    network_ip_length_min: float = Field(..., alias="network_ip-length_min")
    network_ip_length_std_deviation: float = Field(..., alias="network_ip-length_std_deviation")
    
    network_ips_all_count: float = Field(...)
    network_ips_dst_count: float = Field(...)
    network_ips_src_count: float = Field(...)
    network_macs_all_count: float = Field(...)
    network_macs_dst_count: float = Field(...)
    network_macs_src_count: float = Field(...)
    
    network_mss_avg: float = Field(...)
    network_mss_max: float = Field(...)
    network_mss_min: float = Field(...)
    network_mss_std_deviation: float = Field(...)
    
    network_packet_size_avg: float = Field(..., alias="network_packet-size_avg")
    network_packet_size_max: float = Field(..., alias="network_packet-size_max")
    network_packet_size_min: float = Field(..., alias="network_packet-size_min")
    network_packet_size_std_deviation: float = Field(..., alias="network_packet-size_std_deviation")
    
    network_packets_all_count: float = Field(...)
    network_packets_dst_count: float = Field(...)
    network_packets_src_count: float = Field(...)
    
    network_payload_length_avg: float = Field(..., alias="network_payload-length_avg")
    network_payload_length_max: float = Field(..., alias="network_payload-length_max")
    network_payload_length_min: float = Field(..., alias="network_payload-length_min")
    network_payload_length_std_deviation: float = Field(..., alias="network_payload-length_std_deviation")
    
    network_ports_all_count: float = Field(...)
    network_ports_dst_count: float = Field(...)
    network_ports_src_count: float = Field(...)
    network_protocols_all_count: float = Field(...)
    network_protocols_dst_count: float = Field(...)
    network_protocols_src_count: float = Field(...)
    
    network_tcp_flags_ack_count: float = Field(..., alias="network_tcp-flags-ack_count")
    network_tcp_flags_fin_count: float = Field(..., alias="network_tcp-flags-fin_count")
    network_tcp_flags_psh_count: float = Field(..., alias="network_tcp-flags-psh_count")
    network_tcp_flags_rst_count: float = Field(..., alias="network_tcp-flags-rst_count")
    network_tcp_flags_syn_count: float = Field(..., alias="network_tcp-flags-syn_count")
    network_tcp_flags_urg_count: float = Field(..., alias="network_tcp-flags-urg_count")
    network_tcp_flags_avg: float = Field(..., alias="network_tcp-flags_avg")
    network_tcp_flags_max: float = Field(..., alias="network_tcp-flags_max")
    network_tcp_flags_min: float = Field(..., alias="network_tcp-flags_min")
    network_tcp_flags_std_deviation: float = Field(..., alias="network_tcp-flags_std_deviation")
    
    network_time_delta_avg: float = Field(..., alias="network_time-delta_avg")
    network_time_delta_max: float = Field(..., alias="network_time-delta_max")
    network_time_delta_min: float = Field(..., alias="network_time-delta_min")
    network_time_delta_std_deviation: float = Field(..., alias="network_time-delta_std_deviation")
    
    network_ttl_avg: float = Field(...)
    network_ttl_max: float = Field(...)
    network_ttl_min: float = Field(...)
    network_ttl_std_deviation: float = Field(...)
    
    network_window_size_avg: float = Field(..., alias="network_window-size_avg")
    network_window_size_max: float = Field(..., alias="network_window-size_max")
    network_window_size_min: float = Field(..., alias="network_window-size_min")
    network_window_size_std_deviation: float = Field(..., alias="network_window-size_std_deviation")

    # ==========================================
    # 4. ĐẶC TRƯNG ONE-HOT MÃ HÓA (Không có dấu gạch ngang ở tên gốc, ngoại trừ data-text-lines)
    # ==========================================
    log_type_array: float = Field(...)
    log_type_numeric: float = Field(...)
    log_type_string: float = Field(...)
    
    src_proto_arp: float = Field(...)
    src_proto_data: float = Field(...)
    src_proto_data_text_lines: float = Field(..., alias="src_proto_data-text-lines")
    src_proto_dhcpv6: float = Field(...)
    src_proto_dns: float = Field(...)
    src_proto_http: float = Field(...)
    src_proto_icmp: float = Field(...)
    src_proto_ieee1905: float = Field(...)
    src_proto_igmp: float = Field(...)
    src_proto_json: float = Field(...)
    src_proto_lldp: float = Field(...)
    src_proto_mdns: float = Field(...)
    src_proto_mqtt: float = Field(...)
    src_proto_nbns: float = Field(...)
    src_proto_ntp: float = Field(...)
    src_proto_other: float = Field(...)
    src_proto_quic: float = Field(...)
    src_proto_rtcp: float = Field(...)
    src_proto_ssdp: float = Field(...)
    src_proto_tcp: float = Field(...)
    src_proto_telnet: float = Field(...)
    src_proto_tls: float = Field(...)
    src_proto_xml: float = Field(...)
    
    dst_proto_arp: float = Field(...)
    dst_proto_c1222: float = Field(...)
    dst_proto_chargen: float = Field(...)
    dst_proto_data: float = Field(...)
    dst_proto_data_text_lines: float = Field(..., alias="dst_proto_data-text-lines")
    dst_proto_daytime: float = Field(...)
    dst_proto_discard: float = Field(...)
    dst_proto_dns: float = Field(...)
    dst_proto_echo: float = Field(...)
    dst_proto_ftp: float = Field(...)
    dst_proto_gopher: float = Field(...)
    dst_proto_hcrt: float = Field(...)
    dst_proto_http: float = Field(...)
    dst_proto_icmp: float = Field(...)
    dst_proto_ipdc: float = Field(...)
    dst_proto_json: float = Field(...)
    dst_proto_lbtrm: float = Field(...)
    dst_proto_mqtt: float = Field(...)
    dst_proto_nbns: float = Field(...)
    dst_proto_nbss: float = Field(...)
    dst_proto_ncp: float = Field(...)
    dst_proto_ntp: float = Field(...)
    dst_proto_other: float = Field(...)
    dst_proto_quic: float = Field(...)
    dst_proto_rpc: float = Field(...)
    dst_proto_rtcp: float = Field(...)
    dst_proto_rtsp: float = Field(...)
    dst_proto_snmp: float = Field(...)
    dst_proto_ssdp: float = Field(...)
    dst_proto_ssh: float = Field(...)
    dst_proto_tcp: float = Field(...)
    dst_proto_telnet: float = Field(...)
    dst_proto_time: float = Field(...)
    dst_proto_tls: float = Field(...)
    dst_proto_udp: float = Field(...)
    dst_proto_wsp: float = Field(...)

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
    
    # kiểm tra các cột "network_packet_size" để đảm bảo không có giá trị NaN hoặc Infinity lọt qua
    @field_validator('network_packet_size_avg', 'network_packet_size_max', 'network_packet_size_min')
    @classmethod
    def check_not_nan(cls, v):
        """Chặn đứng các giá trị NaN/Infinity lọt qua từ Spark"""
        # Tránh lỗi float('nan') hoặc float('inf') làm hỏng weight của GNN
        if v != v or v == float('inf') or v == float('-inf'):
            raise ValueError("Dữ liệu chứa NaN hoặc Infinity")
        return v

    # hàm đổi toàn bộ các feature (trừ timestamp, label, network_ips_dst, network_ips_src, network_ports_dst, network_ports_src) thành một list float để dễ dàng biến thành PyTorch Tensor
    def to_tensor_list(self) -> list:
        """
        Hàm tiện ích giúp xuất toàn bộ đặc trưng (loại trừ timestamp và label)
        thành một list float để dễ dàng biến thành PyTorch Tensor.
        """
        # Lấy tất cả giá trị dưới dạng dict
        data_dict = self.model_dump(exclude={
            'timestamp', 
            'label', 
            'network_ips_dst', 
            'network_ips_src', 
            'network_ports_dst', 
            'network_ports_src'
        })
        return list(data_dict.values())