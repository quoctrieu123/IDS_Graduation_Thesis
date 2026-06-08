import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GATv2Conv
from torch_geometric.utils import dropout_edge

class GAT_Embedder(torch.nn.Module):
    def __init__(self, in_channels=137, hidden_channels=64, embedding_dim=32, num_classes=8, heads=4, edge_dropout=0.1, edge_dim=5):
        super(GAT_Embedder, self).__init__()
        self.edge_dropout = edge_dropout 
        self.conv1 = GATv2Conv(in_channels, hidden_channels, heads=heads, dropout=0.1, edge_dim=edge_dim)
        self.bn1 = nn.BatchNorm1d(hidden_channels * heads)
        self.conv2 = GATv2Conv(hidden_channels * heads, embedding_dim, heads=1, concat=False, dropout=0.1, edge_dim=edge_dim)
        self.bn2 = nn.BatchNorm1d(embedding_dim)
        self.classifier = nn.Linear(embedding_dim, num_classes)

    def forward(self, x, edge_index, edge_attr=None):
        # Lưu ý: Khi gọi model.eval() lúc chạy thực tế, self.training sẽ là False
        # Do đó dropout_edge và F.dropout sẽ tự động bị vô hiệu hóa, giữ nguyên dữ liệu 100%
        edge_index_dp, edge_mask = dropout_edge(edge_index, p=self.edge_dropout, force_undirected=False, training=self.training)
        edge_attr_dp = edge_attr[edge_mask] if edge_attr is not None else None
        if edge_attr is not None:
            edge_attr_dp = edge_attr[edge_mask.to(edge_attr.device)] 
        else:
            edge_attr_dp = None
        x = F.dropout(x, p=0.4, training=self.training) 
        x = self.conv1(x, edge_index_dp, edge_attr=edge_attr_dp)
        x = self.bn1(x)
        x = F.elu(x)
        
        edge_index_dp2, edge_mask2 = dropout_edge(edge_index, p=self.edge_dropout, force_undirected=False, training=self.training)
        edge_attr_dp2 = edge_attr[edge_mask2] if edge_attr is not None else None
        
        x = F.dropout(x, p=0.4, training=self.training) 
        embedding = self.conv2(x, edge_index_dp2, edge_attr=edge_attr_dp2)
        embedding = self.bn2(embedding)
        embedding = F.elu(embedding) 
        
        out = self.classifier(embedding)
        
        # Trong Inference, ta lấy embedding để đưa cho XGBoost nhánh dưới,
        # hoặc lấy out nếu muốn đưa xác suất (logits) vào Meta-Learner.
        return out, embedding