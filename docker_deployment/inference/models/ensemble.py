import os
from pathlib import Path
import torch
import xgboost as xgb
import numpy as np

from .gat_model import GAT_Embedder
from .seq_model import CNN_BiLSTM_Attention

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MODEL_DIR = PROJECT_ROOT / "Use case 3_CIC_IIOT_2025" / "model_saved"
MODEL_DIR = os.getenv("MODEL_DIR", str(DEFAULT_MODEL_DIR))

def model_path(filename):
    return os.path.join(MODEL_DIR, filename)

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
        
        self.gnn.load_state_dict(torch.load(model_path("gat_embedder_exper_1_best.pth"), map_location=self.device))
        self.seq.load_state_dict(torch.load(model_path("cnn_bilstm_exper_1_best.pth"), map_location=self.device))
        
        # Khóa mô hình ở chế độ suy luận (Tắt Dropout, khóa BatchNorm)
        self.gnn.eval()
        self.seq.eval()
        
        # 2. Khởi tạo 2 mô hình XGBoost
        self.xgb_bottom = xgb.Booster()
        self.xgb_bottom.load_model(model_path("GAT_XGB_Hybrid_Temporal_Model_exper_1_best.json"))
        
        self.xgb_meta = xgb.Booster()
        self.xgb_meta.load_model(model_path("meta_learner_xgb_hybrid.json"))

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
            seq_prob_np = torch.softmax(seq_out, dim=1).cpu().numpy()
            
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
            # Meta learner was trained with XGB probabilities first, then CNN-BiLSTM probabilities.
            meta_features = np.concatenate((xgb_bottom_out, seq_prob_np), axis=1)
            
            dmatrix_meta = xgb.DMatrix(meta_features)
            final_predictions = self.xgb_meta.predict(dmatrix_meta)
            
            # Lấy nhãn có xác suất cao nhất
            final_labels = np.argmax(final_predictions, axis=1)
            
            return final_labels
