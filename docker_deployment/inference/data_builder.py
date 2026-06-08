# file xây dựng sliding window và graph cho mỗi chunk 256 gói tin mới từ kafka
import torch
import numpy as np
import ast
from collections import deque
from typing import List
from schemas import ProcessedFlow

# Hàm tính Jaccard Similarity giữa 2 tập hợp (dùng cho tính trọng số cạnh)
def jaccard_sim(set_a: set, set_b: set) -> float:
    if not set_a and not set_b: return 0.0
    intersect = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    return float(intersect / union) if union > 0 else 0.0

# hàm parse chuỗi string dạng list thành set
def safe_parse_to_set(val: str) -> set:
    try:
        parsed = ast.literal_eval(val)
        return set(parsed) if isinstance(parsed, list) else set()
    except:
        return set()

class BufferManager:
    def __init__(self, seq_time_steps: int = 10, graph_window_size: int = 50, max_dt: float = 30.0):
        self.seq_time_steps = seq_time_steps
        self.seq_history = [] 
        
        self.graph_window_size = graph_window_size
        self.max_dt = max_dt
        self.graph_history = [] 
        self.last_build_stats = {}

    # hàm xây dựng đầu vào sliding window + graph cho mỗi chunk 256 gói tin mới
    def build_inputs(self, current_chunk: List[ProcessedFlow]):
        h_seq = len(self.seq_history)
        N_chunk = len(current_chunk)

        # lưu lại thống kê chi tiết về quá trình xây dựng dữ liệu để debug và tối ưu sau này
        self.last_build_stats = {
            "chunk_size": N_chunk,
            "seq_history_before": h_seq,
            "graph_history_before": len(self.graph_history),
            "seq_time_steps": self.seq_time_steps,
            "graph_window_size": self.graph_window_size,
            "max_dt": self.max_dt,
        }
        
        # BƯỚC 1: NHÁNH SEQUENCE (CNN-BiLSTM)
        seq_combined = self.seq_history + current_chunk
        seq_combined.sort(key=lambda x: x.timestamp) # sort lại thời gian cho sliding window
        
        # chỉ lấy target indices từ những phần tử có đủ lịch sử để tạo cửa sổ sequence (10 flows) ở trên
        start_target_idx = max(0, self.seq_time_steps - 1 - h_seq)
        
        # Nếu chưa đủ 10 flows để tạo cửa sổ đầu tiên, báo hệ thống chờ thêm
        if len(seq_combined) < self.seq_time_steps:
            self.seq_history = seq_combined
            self.graph_history = (self.graph_history + current_chunk)[-self.graph_window_size:]
            self.last_build_stats.update({
                "status": "warming_up",
                "seq_combined": len(seq_combined),
                "seq_history_after": len(self.seq_history),
                "graph_history_after": len(self.graph_history),
            })
            return None, None, None, None, None

        # tạo tensor cho nhánh sequence với window size là 10
        seq_windows = []
        for i in range(len(seq_combined) - self.seq_time_steps + 1):
            window_slice = seq_combined[i : i + self.seq_time_steps]
            seq_windows.append([flow.to_tensor_list() for flow in window_slice])
            
        seq_tensor = torch.tensor(seq_windows, dtype=torch.float32)
        
        # Cập nhật lịch sử sequence cho lượt sau
        self.seq_history = seq_combined[-(self.seq_time_steps - 1):]


        # Tạo graph cho nhánh GAT
        h_graph = len(self.graph_history)
        graph_combined = self.graph_history + current_chunk
        graph_combined.sort(key=lambda x: x.timestamp) # sort lại theo timestamp cho việc xây dựng graph
        
        # trích xuất các đặc trưng của node 
        node_features = [flow.to_tensor_list() for flow in graph_combined]
        graph_x = torch.tensor(node_features, dtype=torch.float32)

        # LƯU Ý QUAN TRỌNG NHẤT: Đồng bộ target indices với sequence
        # Chỉ lấy indices của những node có cửa sổ sequence hợp lệ ở trên
        target_indices = [h_graph + i for i in range(start_target_idx, N_chunk)]

        all_src, all_dst, all_edge_attrs = [], [], []
        
        # tạo list timestamps, ips_src, ips_dst, ports_src, ports_dst để xây dựng graph nhanh hơn
        timestamps = [f.timestamp for f in graph_combined]
        ips_src = [safe_parse_to_set(f.network_ips_src) for f in graph_combined]
        ips_dst = [safe_parse_to_set(f.network_ips_dst) for f in graph_combined]
        ports_src = [safe_parse_to_set(f.network_ports_src) for f in graph_combined]
        ports_dst = [safe_parse_to_set(f.network_ports_dst) for f in graph_combined]

        window_indices = deque()
        num_total = len(graph_combined)

        for curr_idx in range(num_total):
            curr_time = timestamps[curr_idx]
            curr_ip_src = ips_src[curr_idx]
            curr_ip_dst = ips_dst[curr_idx]
            curr_ip_all = curr_ip_src.union(curr_ip_dst)
            
            while window_indices and (curr_time - timestamps[window_indices[0]]) > self.max_dt:
                window_indices.popleft()
                
            recent_nodes = list(window_indices)[-self.graph_window_size:]
            # tạo cạnh cùng các thuộc tính
            for past_idx in recent_nodes:
                past_ip_all = ips_src[past_idx].union(ips_dst[past_idx])
                
                if len(curr_ip_all.intersection(past_ip_all)) > 0:
                    dt_raw = abs(curr_time - timestamps[past_idx])
                    dt = np.log1p(dt_raw * 1e6) / 18.0
                    
                    w_ip_src = jaccard_sim(curr_ip_src, ips_src[past_idx])
                    w_ip_dst = jaccard_sim(curr_ip_dst, ips_dst[past_idx])
                    w_port_src = jaccard_sim(ports_src[curr_idx], ports_src[past_idx])
                    w_port_dst = jaccard_sim(ports_dst[curr_idx], ports_dst[past_idx])
                    
                    attr = [dt, w_ip_src, w_ip_dst, w_port_src, w_port_dst]
                    
                    all_src.append(past_idx)
                    all_dst.append(curr_idx)
                    all_edge_attrs.append(attr)
            
            window_indices.append(curr_idx)
            
        edge_index = torch.tensor([all_src, all_dst], dtype=torch.long)
        edge_attr = torch.tensor(all_edge_attrs, dtype=torch.float32)

        # cập nhật lịch sử graph
        self.graph_history = graph_combined[-self.graph_window_size:]
        self.last_build_stats.update({
            "status": "ready",
            "seq_combined": len(seq_combined),
            "seq_windows": len(seq_windows),
            "seq_history_after": len(self.seq_history),
            "graph_nodes": len(graph_combined),
            "graph_edges": len(all_src),
            "edge_attr_dim": int(edge_attr.shape[1]) if edge_attr.ndim == 2 and edge_attr.shape[0] > 0 else 0,
            "target_count": len(target_indices),
            "target_first": target_indices[0] if target_indices else None,
            "target_last": target_indices[-1] if target_indices else None,
            "graph_history_after": len(self.graph_history),
        })

        # Trả về toàn bộ data sạch sẽ
        return seq_tensor, graph_x, edge_index, edge_attr, target_indices
