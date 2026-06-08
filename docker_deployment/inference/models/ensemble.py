import torch
import xgboost as xgb
import numpy as np

from .gat_model import GAT_Embedder
from .seq_model import CNN_BiLSTM_Attention

class EnsembleManager:
    def __init__(self, device='cpu'):
        self.device = torch.device(device)
        
        # 1. Khởi tạo 2 mô hình PyTorch
        # Sửa các tham số in_channels, out_channels và num_heads
        self.gnn = GAT_Embedder(
            in_channels=133, 
            hidden_channels=64,      
            embedding_dim=32,        
            num_classes=8, 
            heads=8,
            edge_dropout=0.3,
            edge_dim=5 
        ).to(device)
        self.seq = CNN_BiLSTM_Attention(num_features=133, num_classes=8).to(self.device)
        
        self.gnn.load_state_dict(torch.load(r'C:\Users\Admin\Downloads\IoT Dataset\CCIOT\model_final\gat_embedder_best.pth', map_location=self.device))
        self.seq.load_state_dict(torch.load(r'C:\Users\Admin\Downloads\IoT Dataset\CCIOT\model_final\best_cnn_bilstm_best.pth', map_location=self.device))
        
        # Khóa mô hình ở chế độ suy luận (Tắt Dropout, khóa BatchNorm)
        self.gnn.eval()
        self.seq.eval()
        
        # 2. Khởi tạo 2 mô hình XGBoost
        self.xgb_bottom = xgb.Booster()
        self.xgb_bottom.load_model(r'C:\Users\Admin\Downloads\IoT Dataset\CCIOT\model_final\GAT_XGB_Hybrid_Temporal_Model_best.json')
        
        self.xgb_meta = xgb.Booster()
        self.xgb_meta.load_model(r'C:\Users\Admin\Downloads\IoT Dataset\CCIOT\model_final\meta_learner_xgb_final.json')

    # nhận vào dữ liệu được xây dựng từ BufferManager và trả về nhãn dự đoán cuối cùng sau khi chạy qua toàn bộ pipeline
    def predict(self, window_x, graph_x, edge_index, edge_attr=None, target_indices=None):
        """
        Thực hiện chạy toàn bộ pipeline
        - window_x: Tensor (Batch, 10, 133)
        - graph_x: Tensor (N_nodes, 133)
        - edge_index: Tensor (2, E)
        - target_indices: Danh sách vị trí của Batch (256 nodes) mục tiêu trong đồ thị N_nodes
        """
        with torch.no_grad(): # Tắt tính toán gradient để tối ưu RAM và tốc độ
            # ==========================================
            # NHÁNH TRÊN: SEQUENCE
            # ==========================================
            seq_out = self.seq(window_x.to(self.device)) # Shape: (B, 8)
            seq_out_np = seq_out.cpu().numpy()
            
            # ==========================================
            # NHÁNH DƯỚI: GRAPH + XGBoost 1
            # ==========================================
            edge_attr_device = edge_attr.to(self.device) if edge_attr is not None else None
            _, graph_embedding = self.gnn(
                graph_x.to(self.device),
                edge_index.to(self.device),
                edge_attr_device,
            )
            
            # Khâu Target Flow Alignment: Chỉ lấy embedding của các nodes mục tiêu
            target_embeddings = graph_embedding[target_indices] # Shape: (B, 128)
            target_embeddings_np = target_embeddings.cpu().numpy()
            
            # Đưa qua XGBoost của nhánh dưới
            dmatrix_bottom = xgb.DMatrix(target_embeddings_np)
            xgb_bottom_out = self.xgb_bottom.predict(dmatrix_bottom) # Shape: (B, 8)
            
            # ==========================================
            # META LEARNER: Ghép nối (Concat)
            # ==========================================
            # Nối kết quả của nhánh trên và nhánh dưới (B, 8+8 = 16)
            meta_features = np.concatenate((seq_out_np, xgb_bottom_out), axis=1)
            
            dmatrix_meta = xgb.DMatrix(meta_features)
            final_predictions = self.xgb_meta.predict(dmatrix_meta)
            
            # Lấy nhãn có xác suất cao nhất
            final_labels = np.argmax(final_predictions, axis=1)
            
            return final_labels
