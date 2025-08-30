# 數據處理階段

## 🎯 目標
將原始數據清理、標準化並創建分析就緒的數據集

## 📁 文件說明
- `data_processor.py`: 主要數據處理腳本
- `requirements.txt`: Python依賴套件
- `README.md`: 此說明文件

## 🚀 使用方法

### 1. 安裝依賴
```bash
pip install -r requirements.txt
```

### 2. 執行數據處理
```bash
python data_processor.py
```

## 📊 輸出結果
處理後的數據會保存在 `processed_data/` 目錄下：

### 清理後數據
- `daily/sui_protocols_clean_YYYYMMDD.csv`: 清理後的Sui協議數據
- `daily/aptos_protocols_clean_YYYYMMDD.csv`: 清理後的Aptos協議數據  
- `daily/sui_price_clean_YYYYMMDD.csv`: 清理後的SUI價格數據
- `daily/aptos_price_clean_YYYYMMDD.csv`: 清理後的APT價格數據

### 分析數據
- `analysis_ready/comparative_analysis_YYYYMMDD.json`: 比較分析結果

## 🔧 數據處理內容

### 協議數據清理
- ✅ 移除CEX和非DeFi協議
- ✅ 處理TVL異常值
- ✅ 標準化協議分類
- ✅ 計算增長評分和排名

### 價格數據清理  
- ✅ 計算技術指標 (移動平均、波動率)
- ✅ 計算價格變化率 (1天、7天、30天)
- ✅ 添加時間特徵
- ✅ 計算累積回報率

### 比較分析
- ✅ 協議分類比較
- ✅ 規模分佈分析
- ✅ 增長指標比較
- ✅ 生態集中度分析
- ✅ 價格表現比較
- ✅ 風險指標比較
- ✅ 綜合評分計算

## ⚠️ 注意事項
- 確保原始數據位於 `../Data_Collection/raw_data/` 目錄
- 檢查日期格式是否為 `20250822` 格式
- 如遇到路徑問題，請調整腳本中的 `raw_data_dir` 路徑