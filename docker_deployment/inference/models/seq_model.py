import torch
import torch.nn as nn

class Attention(nn.Module):
    def __init__(self, hidden_dim):
        super(Attention, self).__init__()
        self.attention = nn.Linear(hidden_dim, 1)

    def forward(self, lstm_outputs):
        scores = self.attention(lstm_outputs)
        weights = torch.softmax(scores, dim=1)
        context_vector = torch.sum(weights * lstm_outputs, dim=1)
        return context_vector, weights

class SEBlock1D(nn.Module):
    def __init__(self, channels, reduction=8):
        super(SEBlock1D, self).__init__()
        self.avg_pool = nn.AdaptiveAvgPool1d(1)
        self.fc = nn.Sequential(
            nn.Linear(channels, channels // reduction, bias=False),
            nn.ReLU(inplace=True),
            nn.Linear(channels // reduction, channels, bias=False),
            nn.Sigmoid()
        )

    def forward(self, x):
        b, c, _ = x.size()
        y = self.avg_pool(x).view(b, c) 
        y = self.fc(y).view(b, c, 1)    
        return x * y.expand_as(x)       

class ResidualBlock1D(nn.Module):
    def __init__(self, in_channels, out_channels, kernel_size=3):
        super(ResidualBlock1D, self).__init__()
        padding = kernel_size // 2
        self.conv1 = nn.Conv1d(in_channels, out_channels, kernel_size, padding=padding) 
        self.gn1 = nn.GroupNorm(num_groups=8, num_channels=out_channels) 
        self.relu = nn.ReLU() 
        self.conv2 = nn.Conv1d(out_channels, out_channels, kernel_size, padding=padding) 
        self.gn2 = nn.GroupNorm(num_groups=8, num_channels=out_channels) 
        self.dropout = nn.Dropout1d(0.2)
        
        self.se = SEBlock1D(out_channels)
        self.shortcut = nn.Sequential()
        if in_channels != out_channels: 
            self.shortcut = nn.Sequential(
                nn.Conv1d(in_channels, out_channels, kernel_size=1),
                nn.GroupNorm(num_groups=8, num_channels=out_channels)
            )
            
    def forward(self, x):
        residual = self.shortcut(x)
        out = self.conv1(x)
        out = self.gn1(out)
        out = self.relu(out)
        out = self.dropout(out)
        out = self.conv2(out)
        out = self.gn2(out)
        out = self.se(out)
        out += residual  
        out = self.relu(out)
        return out

class CNN_BiLSTM_Attention(nn.Module):
    def __init__(self, num_features=137, num_classes=8, time_steps=10, hidden_size=128):
        super(CNN_BiLSTM_Attention, self).__init__()
        self.res1 = ResidualBlock1D(num_features, 64)
        self.res2 = ResidualBlock1D(64, 128)
        self.pool = nn.MaxPool1d(kernel_size=2) 
        
        self.bilstm = nn.LSTM(input_size=128, hidden_size=hidden_size, 
                              batch_first=True, bidirectional=True)
        self.layer_norm = nn.LayerNorm(hidden_size * 2)

        self.attention = Attention(hidden_size * 2)
        self.dropout = nn.Dropout(0.5)
        
        self.fc1 = nn.Linear(hidden_size * 2, 64)
        self.fc_ln = nn.LayerNorm(64)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(64, num_classes)
        
    def forward(self, x):
        x = x.permute(0, 2, 1) 
        x = self.res1(x)
        x = self.res2(x)
        x = self.pool(x)
        x = x.permute(0, 2, 1)
        
        out, _ = self.bilstm(x)
        out = self.layer_norm(out)
        context_vector, attn_weights = self.attention(out)
        
        out = self.dropout(context_vector)
        out = self.fc1(out) 
        out = self.fc_ln(out)
        out = self.relu(out)
        out = self.dropout(out)
        out = self.fc2(out) # Output: (Batch, num_classes)
        return out