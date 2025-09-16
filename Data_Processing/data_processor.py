# Sui vs Aptos 數據處理和清理工具

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import os
import logging
import warnings
import requests
import time
warnings.filterwarnings('ignore')

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SuiAptosDataProcessor:
    """Sui vs Aptos 數據處理器"""
    
    def __init__(self, raw_data_dir="../Data_Collection/raw_data", output_dir="processed_data"):
        self.raw_data_dir = raw_data_dir
        self.output_dir = output_dir
        
        # 創建輸出目錄
        for subdir in ['daily', 'weekly', 'monthly', 'analysis_ready']:
            os.makedirs(f"{output_dir}/{subdir}", exist_ok=True)
        
        logger.info(f"數據處理器初始化完成")
        logger.info(f"原始數據目錄: {raw_data_dir}")
        logger.info(f"輸出目錄: {output_dir}")
    
    def load_raw_data(self):
        """載入原始數據 - 從DefiLlama API獲取最新的鏈級別數據"""
        logger.info("開始從DefiLlama API載入最新數據...")
        
        data = {}
        
        try:
            # 1. 獲取鏈級別的TVL數據
            logger.info("正在獲取鏈級別TVL數據...")
            chains_response = requests.get("https://api.llama.fi/v2/chains", timeout=30)
            chains_response.raise_for_status()
            chains_data = chains_response.json()
            
            # 找出Sui和Aptos的當前TVL
            sui_chain_tvl = 0
            aptos_chain_tvl = 0
            
            for chain in chains_data:
                if chain.get('name') == 'Sui':
                    sui_chain_tvl = chain.get('tvl', 0)
                elif chain.get('name') == 'Aptos':
                    aptos_chain_tvl = chain.get('tvl', 0)
            
            logger.info(f"鏈級別TVL - Sui: ${sui_chain_tvl/1e9:.2f}B, Aptos: ${aptos_chain_tvl/1e9:.2f}B")
            
            # 2. 獲取具體協議數據 (僅用於分類分析)
            logger.info("正在獲取協議詳細數據...")
            protocols_response = requests.get("https://api.llama.fi/protocols", timeout=30)
            protocols_response.raise_for_status()
            all_protocols = protocols_response.json()
            
            # 3. 過濾並調整協議TVL (按鏈分配)
            sui_protocols = []
            aptos_protocols = []
            
            sui_total_from_protocols = 0
            aptos_total_from_protocols = 0
            
            for protocol in all_protocols:
                chains = protocol.get('chains', [])
                protocol_tvl = float(protocol.get('tvl', 0)) if protocol.get('tvl') is not None else 0
                
                if 'Sui' in chains and protocol_tvl > 0:
                    # 如果協議在多個鏈上，需要按比例分配TVL
                    chain_allocation = 1.0 / len(chains) if len(chains) > 1 else 1.0
                    allocated_tvl = protocol_tvl * chain_allocation
                    
                    protocol_data = {
                        'name': protocol.get('name', ''),
                        'slug': protocol.get('slug', ''),
                        'category': protocol.get('category', 'Unknown'),
                        'tvl_usd': allocated_tvl,
                        'original_tvl': protocol_tvl,
                        'chain_count': len(chains),
                        'allocation_ratio': chain_allocation,
                        'change_1d': float(protocol.get('change_1d', 0)) if protocol.get('change_1d') is not None else 0.0,
                        'change_7d': float(protocol.get('change_7d', 0)) if protocol.get('change_7d') is not None else 0.0,
                        'change_1m': float(protocol.get('change_1m', 0)) if protocol.get('change_1m') is not None else 0.0,
                        'chains_count': len(chains),
                        'chain': 'Sui',
                        'collected_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    sui_protocols.append(protocol_data)
                    sui_total_from_protocols += allocated_tvl
                
                if 'Aptos' in chains and protocol_tvl > 0:
                    # 如果協議在多個鏈上，需要按比例分配TVL
                    chain_allocation = 1.0 / len(chains) if len(chains) > 1 else 1.0
                    allocated_tvl = protocol_tvl * chain_allocation
                    
                    protocol_data = {
                        'name': protocol.get('name', ''),
                        'slug': protocol.get('slug', ''),
                        'category': protocol.get('category', 'Unknown'),
                        'tvl_usd': allocated_tvl,
                        'original_tvl': protocol_tvl,
                        'chain_count': len(chains),
                        'allocation_ratio': chain_allocation,
                        'change_1d': float(protocol.get('change_1d', 0)) if protocol.get('change_1d') is not None else 0.0,
                        'change_7d': float(protocol.get('change_7d', 0)) if protocol.get('change_7d') is not None else 0.0,
                        'change_1m': float(protocol.get('change_1m', 0)) if protocol.get('change_1m') is not None else 0.0,
                        'chains_count': len(chains),
                        'chain': 'Aptos',
                        'collected_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    aptos_protocols.append(protocol_data)
                    aptos_total_from_protocols += allocated_tvl
            
            # 4. 如果協議總和與鏈TVL不匹配，進行調整
            if sui_total_from_protocols > 0 and sui_chain_tvl > 0:
                sui_adjustment_factor = sui_chain_tvl / sui_total_from_protocols
                for protocol in sui_protocols:
                    protocol['tvl_usd'] *= sui_adjustment_factor
                    
            if aptos_total_from_protocols > 0 and aptos_chain_tvl > 0:
                aptos_adjustment_factor = aptos_chain_tvl / aptos_total_from_protocols
                for protocol in aptos_protocols:
                    protocol['tvl_usd'] *= aptos_adjustment_factor
            
            logger.info(f"調整後協議TVL總和 - Sui: ${sum(p['tvl_usd'] for p in sui_protocols)/1e9:.2f}B")
            logger.info(f"調整後協議TVL總和 - Aptos: ${sum(p['tvl_usd'] for p in aptos_protocols)/1e9:.2f}B")
            
            # 5. 轉換為DataFrame
            data['sui_protocols'] = pd.DataFrame(sui_protocols)
            data['aptos_protocols'] = pd.DataFrame(aptos_protocols)
            
            # 6. 獲取歷史TVL數據作為價格數據的替代
            logger.info("正在獲取歷史TVL數據...")
            
            sui_price_data = self._get_historical_tvl_data('Sui')
            aptos_price_data = self._get_historical_tvl_data('Aptos')
            
            data['sui_price'] = pd.DataFrame(sui_price_data)
            data['aptos_price'] = pd.DataFrame(aptos_price_data)
            
            # 7. 創建比較數據
            data['comparison'] = {
                'data_source': 'DefiLlama_Chain_API',
                'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'sui_protocol_count': len(sui_protocols),
                'aptos_protocol_count': len(aptos_protocols),
                'sui_chain_tvl': sui_chain_tvl,
                'aptos_chain_tvl': aptos_chain_tvl
            }
            
            logger.info("API數據載入成功:")
            logger.info(f"  Sui協議: {len(data['sui_protocols'])} 個")
            logger.info(f"  Aptos協議: {len(data['aptos_protocols'])} 個")
            logger.info(f"  SUI歷史數據: {len(data['sui_price'])} 天")
            logger.info(f"  APT歷史數據: {len(data['aptos_price'])} 天")
            
            # 8. 保存備份
            self._save_raw_data_backup(data)
            
            return data
            
        except requests.RequestException as e:
            logger.error(f"API請求失敗: {e}")
            logger.info("嘗試載入本地備份數據...")
            return self._load_backup_data()
            
        except Exception as e:
            logger.error(f"數據載入失敗: {e}")
            return None
    
    def _get_historical_tvl_data(self, chain_name):
        """獲取鏈的歷史TVL數據作為價格分析的基礎"""
        try:
            # 使用歷史鏈TVL API
            url = f"https://api.llama.fi/v2/historicalChainTvl/{chain_name}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            historical_data = response.json()
            price_data = []
            
            # 轉換歷史TVL數據為價格格式 (使用TVL作為"價格"的代理)
            for data_point in historical_data:
                if isinstance(data_point, dict) and 'date' in data_point and 'tvl' in data_point:
                    price_data.append({
                        'date': data_point['date'],
                        'price_usd': float(data_point['tvl']) / 1e9,  # 使用TVL(B)作為代理價格
                        'market_cap_usd': float(data_point['tvl']),
                        'volume_24h_usd': 0,
                        'chain': chain_name
                    })
                elif isinstance(data_point, list) and len(data_point) >= 2:
                    # 處理 [timestamp, tvl] 格式
                    timestamp, tvl = data_point[0], data_point[1]
                    date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                    price_data.append({
                        'date': date,
                        'price_usd': float(tvl) / 1e9,  # 使用TVL(B)作為代理價格
                        'market_cap_usd': float(tvl),
                        'volume_24h_usd': 0,
                        'chain': chain_name
                    })
            
            return price_data
            
        except Exception as e:
            logger.warning(f"獲取{chain_name}歷史TVL數據失敗: {e}")
            # 返回最小化的數據
            current_date = datetime.now()
            return [{
                'date': (current_date - timedelta(days=i)).strftime('%Y-%m-%d'),
                'price_usd': 1.0,
                'market_cap_usd': 1e9,
                'volume_24h_usd': 0,
                'chain': chain_name
            } for i in range(30)]
    
    def _get_price_data(self, token_id, chain_name):
        """獲取代幣價格歷史數據"""
        try:
            # DefiLlama價格API (獲取365天數據)
            end_timestamp = int(datetime.now().timestamp())
            start_timestamp = int((datetime.now() - timedelta(days=365)).timestamp())
            
            url = f"https://coins.llama.fi/prices/historical/{start_timestamp}/{token_id}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            price_data = response.json()
            prices = []
            
            # 如果API格式不同，則使用替代方法
            if 'prices' in price_data:
                for price_point in price_data['prices']:
                    prices.append({
                        'date': datetime.fromtimestamp(price_point[0]).strftime('%Y-%m-%d'),
                        'price_usd': float(price_point[1]),
                        'market_cap_usd': 0,  # 需要額外API調用
                        'volume_24h_usd': 0,  # 需要額外API調用
                        'chain': chain_name
                    })
            else:
                # 使用當前價格API作為後備
                current_url = f"https://api.llama.fi/coins/prices/current/{token_id}"
                current_response = requests.get(current_url, timeout=30)
                if current_response.status_code == 200:
                    current_data = current_response.json()
                    current_price = current_data.get('coins', {}).get(token_id, {}).get('price', 0)
                    
                    # 生成最近365天的模擬數據（基於當前價格）
                    for i in range(365):
                        date = datetime.now() - timedelta(days=i)
                        prices.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'price_usd': float(current_price),
                            'market_cap_usd': 0,
                            'volume_24h_usd': 0,
                            'chain': chain_name
                        })
            
            return prices
            
        except Exception as e:
            logger.warning(f"獲取{chain_name}價格數據失敗: {e}")
            # 返回最小化的價格數據
            return [{
                'date': datetime.now().strftime('%Y-%m-%d'),
                'price_usd': 1.0,
                'market_cap_usd': 0,
                'volume_24h_usd': 0,
                'chain': chain_name
            }]
    
    def _save_raw_data_backup(self, data):
        """保存原始數據作為備份"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = f"{self.raw_data_dir}/api_backup_{timestamp}"
            os.makedirs(backup_dir, exist_ok=True)
            
            data['sui_protocols'].to_csv(f"{backup_dir}/sui_protocols_api.csv", index=False)
            data['aptos_protocols'].to_csv(f"{backup_dir}/aptos_protocols_api.csv", index=False)
            data['sui_price'].to_csv(f"{backup_dir}/sui_price_api.csv", index=False)
            data['aptos_price'].to_csv(f"{backup_dir}/aptos_price_api.csv", index=False)
            
            with open(f"{backup_dir}/comparison_api.json", 'w', encoding='utf-8') as f:
                json.dump(data['comparison'], f, ensure_ascii=False, indent=2)
                
            logger.info(f"數據備份保存到: {backup_dir}")
            
        except Exception as e:
            logger.warning(f"備份保存失敗: {e}")
    
    def _load_backup_data(self):
        """載入本地備份數據作為後備"""
        try:
            # 嘗試載入最新的CSV文件
            data = {}
            data['sui_protocols'] = pd.read_csv(f"{self.raw_data_dir}/sui_data/sui_protocols_final_20250822.csv")
            data['aptos_protocols'] = pd.read_csv(f"{self.raw_data_dir}/aptos_data/aptos_protocols_final_20250822.csv")
            data['sui_price'] = pd.read_csv(f"{self.raw_data_dir}/sui_data/sui_price_final_20250822.csv")
            data['aptos_price'] = pd.read_csv(f"{self.raw_data_dir}/aptos_data/aptos_price_final_20250822.csv")
            
            with open(f"{self.raw_data_dir}/comparison_data/sui_vs_aptos_final_20250822.json", 'r', encoding='utf-8') as f:
                data['comparison'] = json.load(f)
                
            logger.info("已載入本地備份數據")
            return data
            
        except Exception as e:
            logger.error(f"本地備份數據載入也失敗: {e}")
            return None
    
    def clean_protocol_data(self, protocols_df, chain_name):
        """清理協議數據 - 強化CEX過濾"""
        logger.info(f"開始清理 {chain_name} 協議數據...")
        
        # 複製數據避免修改原始數據
        df = protocols_df.copy()
        initial_count = len(df)
        
        # 1. 強化CEX和非DeFi協議過濾
        # 擴展排除關鍵詞列表，包含所有可能的CEX
        exclude_keywords = [
            # 主要CEX
            'Binance', 'binance', 'BINANCE',
            'OKX', 'okx', 'OKEx', 
            'Bybit', 'bybit', 'BYBIT',
            'Gate', 'gate.io', 'GATE',
            'HTX', 'htx', 'Huobi', 'huobi',
            'KuCoin', 'kucoin', 'KUCOIN',
            'Bitfinex', 'bitfinex', 'BITFINEX',
            'Bitstamp', 'bitstamp', 'BITSTAMP',
            'MEXC', 'mexc',
            'Kraken', 'kraken', 'KRAKEN',
            'Coinbase', 'coinbase', 'COINBASE',
            
            # 其他CEX和交易所
            'HashKey Exchange', 'hashkey',
            'Indodax', 'indodax',
            'Phemex', 'phemex',
            'FTX', 'ftx',
            'Crypto.com', 'crypto.com',
            'Bitget', 'bitget',
            'XT.com', 'xt.com',
            
            # 通用關鍵詞
            'CEX', 'cex',
            'Exchange', 'exchange', 'EXCHANGE',
            'Trading', 'trading',
            
            # 可疑的非DeFi協議
            'Custody', 'custody',
            'Wallet', 'wallet' # 注意：這可能過於寬泛，需要謹慎
        ]
        
        # 逐個關鍵詞過濾，記錄每個步驟
        for keyword in exclude_keywords:
            before_count = len(df)
            df = df[~df['name'].str.contains(keyword, case=False, na=False)]
            removed = before_count - len(df)
            if removed > 0:
                logger.info(f"  移除包含 '{keyword}' 的協議: {removed} 個")
        
        # 2. 額外的數值驗證 - 移除異常高的TVL（可能是錯誤數據）
        # 設定合理的TVL上限，任何超過100B的協議都可疑
        tvl_upper_limit = 100e9  # 100億美元
        before_outlier = len(df)
        df = df[df['tvl_usd'] < tvl_upper_limit]
        outlier_removed = before_outlier - len(df)
        if outlier_removed > 0:
            logger.info(f"  移除TVL異常高的協議 (>{tvl_upper_limit/1e9:.0f}B): {outlier_removed} 個")
        
        # 3. 處理TVL異常值
        # 移除TVL為0或負數的協議
        df = df[df['tvl_usd'] > 0]
        
        # 標記TVL異常高的協議 (可能是數據錯誤)
        if len(df) > 0:
            tvl_q99 = df['tvl_usd'].quantile(0.99)
            df['is_outlier'] = df['tvl_usd'] > tvl_q99
            logger.info(f"  標記TVL異常值: {df['is_outlier'].sum()} 個 (>99分位數: ${tvl_q99:,.0f})")
        
        # 4. 分類標準化
        category_mapping = {
            'Dexes': 'DEX',
            'Derivatives': 'Derivatives', 
            'Lending': 'Lending',
            'Yield': 'Yield Farming',
            'Liquid Staking': 'Liquid Staking',
            'Bridge': 'Bridge',
            'Launchpad': 'Launchpad',
            'Gaming': 'Gaming',
            'NFT': 'NFT',
            'Insurance': 'Insurance',
            'RWA': 'RWA',  # 添加RWA分類
            'Unknown': 'Others'
        }
        
        df['category_clean'] = df['category'].map(category_mapping).fillna('Others')
        
        # 5. 添加計算欄位
        df['tvl_millions'] = df['tvl_usd'] / 1_000_000
        df['tvl_billions'] = df['tvl_usd'] / 1_000_000_000
        
        # 6. 計算增長評分 (基於變化率)
        df['growth_score'] = (
            df['change_7d'].fillna(0) * 0.5 + 
            df['change_1m'].fillna(0) * 0.3 + 
            df['change_1d'].fillna(0) * 0.2
        )
        
        # 7. 添加排名
        df['tvl_rank'] = df['tvl_usd'].rank(method='dense', ascending=False)
        df['growth_rank'] = df['growth_score'].rank(method='dense', ascending=False)
        
        # 8. 最終驗證 - 顯示清理結果
        final_count = len(df)
        removed_total = initial_count - final_count
        
        logger.info(f"  {chain_name} 清理摘要:")
        logger.info(f"    原始協議數: {initial_count}")
        logger.info(f"    移除協議數: {removed_total}")
        logger.info(f"    最終協議數: {final_count}")
        
        if final_count > 0:
            logger.info(f"    TVL總和: ${df['tvl_usd'].sum()/1e9:.2f}B")
            logger.info(f"    最大協議TVL: ${df['tvl_usd'].max()/1e6:.1f}M")
            
            # 顯示前5大協議以供驗證
            top_5 = df.nlargest(5, 'tvl_usd')
            logger.info(f"    前5大協議:")
            for idx, row in top_5.iterrows():
                logger.info(f"      {row['name']}: ${row['tvl_usd']/1e6:.1f}M ({row['category']})")
        
        return df
    
    def clean_price_data(self, price_df, chain_name):
        """清理價格數據"""
        logger.info(f"開始清理 {chain_name} 價格數據...")
        
        df = price_df.copy()
        
        # 檢查是否有價格數據
        if len(df) == 0:
            logger.warning(f"{chain_name} 沒有價格數據，創建最小化數據集")
            # 創建最小化的價格數據
            current_date = datetime.now()
            minimal_data = []
            for i in range(30):  # 創建30天的基礎數據
                date = current_date - timedelta(days=i)
                minimal_data.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'price_usd': 1.0,  # 使用默認價格
                    'market_cap_usd': 0,
                    'volume_24h_usd': 0,
                    'chain': chain_name
                })
            df = pd.DataFrame(minimal_data)
        
        # 1. 轉換日期格式
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
        else:
            logger.error(f"{chain_name} 價格數據缺少 'date' 欄位")
            return pd.DataFrame()  # 返回空DataFrame
        
        # 2. 處理缺失值
        numeric_cols = ['price_usd', 'market_cap_usd', 'volume_24h_usd']
        for col in numeric_cols:
            if col in df.columns:
                # 使用前值填充小量缺失值
                df[col] = df[col].fillna(method='ffill')
                # 如果還有缺失值，用0填充
                df[col] = df[col].fillna(0)
        
        # 3. 計算技術指標
        if len(df) > 1:
            df['price_change_1d'] = df['price_usd'].pct_change()
            df['price_change_7d'] = df['price_usd'].pct_change(periods=min(7, len(df)-1))
            df['price_change_30d'] = df['price_usd'].pct_change(periods=min(30, len(df)-1))
            
            # 4. 移動平均
            df['ma_7d'] = df['price_usd'].rolling(window=min(7, len(df))).mean()
            df['ma_30d'] = df['price_usd'].rolling(window=min(30, len(df))).mean()
            
            # 5. 波動率 (30天滾動標準差)
            df['volatility_30d'] = df['price_change_1d'].rolling(window=min(30, len(df))).std()
            
            # 6. 累積回報
            df['cumulative_return'] = (df['price_usd'] / df['price_usd'].iloc[0] - 1) * 100
        else:
            # 如果只有一行數據，設置默認值
            df['price_change_1d'] = 0
            df['price_change_7d'] = 0
            df['price_change_30d'] = 0
            df['ma_7d'] = df['price_usd']
            df['ma_30d'] = df['price_usd']
            df['volatility_30d'] = 0
            df['cumulative_return'] = 0
        
        # 7. 添加時間特徵
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['quarter'] = df['date'].dt.quarter
        df['weekday'] = df['date'].dt.dayofweek
        
        logger.info(f"  {chain_name} 價格數據清理完成: {len(df)} 天")
        
        return df
    
    def create_comparative_analysis(self, sui_protocols_clean, aptos_protocols_clean, sui_price_clean, aptos_price_clean):
        """創建比較分析數據集"""
        logger.info("開始創建比較分析數據集...")
        
        analysis = {}
        
        # 1. 協議比較
        protocol_comparison = {
            'category_comparison': self._compare_categories(sui_protocols_clean, aptos_protocols_clean),
            'size_distribution': self._analyze_size_distribution(sui_protocols_clean, aptos_protocols_clean),
            'growth_comparison': self._compare_growth_metrics(sui_protocols_clean, aptos_protocols_clean),
            'concentration_analysis': self._analyze_concentration(sui_protocols_clean, aptos_protocols_clean)
        }
        
        # 2. 價格比較
        price_comparison = {
            'performance_metrics': self._compare_price_performance(sui_price_clean, aptos_price_clean),
            'risk_metrics': self._compare_risk_metrics(sui_price_clean, aptos_price_clean),
            'correlation_analysis': self._analyze_price_correlation(sui_price_clean, aptos_price_clean)
        }
        
        # 3. 綜合評分 - 修正評分邏輯以反映實際領先者
        ecosystem_scores = self._calculate_ecosystem_scores(
            sui_protocols_clean, aptos_protocols_clean, 
            sui_price_clean, aptos_price_clean
        )
        
        analysis = {
            'protocol_comparison': protocol_comparison,
            'price_comparison': price_comparison,
            'ecosystem_scores': ecosystem_scores,
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return analysis
    
    def _compare_categories(self, sui_df, aptos_df):
        """比較協議分類分佈"""
        sui_categories = sui_df.groupby('category_clean').agg({
            'tvl_usd': ['sum', 'count', 'mean']
        }).round(0)
        
        aptos_categories = aptos_df.groupby('category_clean').agg({
            'tvl_usd': ['sum', 'count', 'mean']
        }).round(0)
        
        # 轉換為簡單字典格式
        sui_cat_dict = {}
        aptos_cat_dict = {}
        
        for category in sui_categories.index:
            sui_cat_dict[category] = {
                'total_tvl': float(sui_categories.loc[category, ('tvl_usd', 'sum')]),
                'protocol_count': int(sui_categories.loc[category, ('tvl_usd', 'count')]),
                'avg_tvl': float(sui_categories.loc[category, ('tvl_usd', 'mean')])
            }
        
        for category in aptos_categories.index:
            aptos_cat_dict[category] = {
                'total_tvl': float(aptos_categories.loc[category, ('tvl_usd', 'sum')]),
                'protocol_count': int(aptos_categories.loc[category, ('tvl_usd', 'count')]),
                'avg_tvl': float(aptos_categories.loc[category, ('tvl_usd', 'mean')])
            }
        
        return {
            'sui_categories': sui_cat_dict,
            'aptos_categories': aptos_cat_dict
        }
    
    def _analyze_size_distribution(self, sui_df, aptos_df):
        """分析協議規模分佈"""
        size_brackets = [0, 1e6, 10e6, 100e6, 1e9, float('inf')]
        size_labels = ['<$1M', '$1M-$10M', '$10M-$100M', '$100M-$1B', '>$1B']
        
        sui_df['size_bracket'] = pd.cut(sui_df['tvl_usd'], bins=size_brackets, labels=size_labels)
        aptos_df['size_bracket'] = pd.cut(aptos_df['tvl_usd'], bins=size_brackets, labels=size_labels)
        
        sui_dist = sui_df['size_bracket'].value_counts().to_dict()
        aptos_dist = aptos_df['size_bracket'].value_counts().to_dict()
        
        return {
            'sui_distribution': {str(k): int(v) for k, v in sui_dist.items()},
            'aptos_distribution': {str(k): int(v) for k, v in aptos_dist.items()}
        }
    
    def _compare_growth_metrics(self, sui_df, aptos_df):
        """比較增長指標"""
        return {
            'sui_growth': {
                'avg_7d_change': float(sui_df['change_7d'].mean()),
                'avg_30d_change': float(sui_df['change_1m'].mean()),
                'positive_growth_7d': int((sui_df['change_7d'] > 0).sum()),
                'negative_growth_7d': int((sui_df['change_7d'] < 0).sum())
            },
            'aptos_growth': {
                'avg_7d_change': float(aptos_df['change_7d'].mean()),
                'avg_30d_change': float(aptos_df['change_1m'].mean()),
                'positive_growth_7d': int((aptos_df['change_7d'] > 0).sum()),
                'negative_growth_7d': int((aptos_df['change_7d'] < 0).sum())
            }
        }
    
    def _analyze_concentration(self, sui_df, aptos_df):
        """分析生態集中度"""
        sui_total = sui_df['tvl_usd'].sum()
        aptos_total = aptos_df['tvl_usd'].sum()
        
        return {
            'sui_concentration': {
                'top_1_share': float(sui_df.iloc[0]['tvl_usd'] / sui_total * 100),
                'top_5_share': float(sui_df.head(5)['tvl_usd'].sum() / sui_total * 100),
                'top_10_share': float(sui_df.head(10)['tvl_usd'].sum() / sui_total * 100),
                'herfindahl_index': float((sui_df['tvl_usd'] / sui_total).pow(2).sum())
            },
            'aptos_concentration': {
                'top_1_share': float(aptos_df.iloc[0]['tvl_usd'] / aptos_total * 100),
                'top_5_share': float(aptos_df.head(5)['tvl_usd'].sum() / aptos_total * 100),
                'top_10_share': float(aptos_df.head(10)['tvl_usd'].sum() / aptos_total * 100),
                'herfindahl_index': float((aptos_df['tvl_usd'] / aptos_total).pow(2).sum())
            }
        }
    
    def _compare_price_performance(self, sui_price, aptos_price):
        """比較價格表現"""
        periods = [7, 30, 90, 180]
        performance = {}
        
        for period in periods:
            if len(sui_price) > period and len(aptos_price) > period:
                sui_return = ((sui_price.iloc[-1]['price_usd'] - sui_price.iloc[-period]['price_usd']) 
                             / sui_price.iloc[-period]['price_usd'] * 100)
                aptos_return = ((aptos_price.iloc[-1]['price_usd'] - aptos_price.iloc[-period]['price_usd']) 
                               / aptos_price.iloc[-period]['price_usd'] * 100)
                
                performance[f'{period}_days'] = {
                    'sui_return': float(sui_return),
                    'aptos_return': float(aptos_return),
                    'outperformance': float(sui_return - aptos_return)
                }
        
        return performance
    
    def _compare_risk_metrics(self, sui_price, aptos_price):
        """比較風險指標"""
        return {
            'volatility_30d': {
                'sui': float(sui_price['volatility_30d'].iloc[-1]),
                'aptos': float(aptos_price['volatility_30d'].iloc[-1])
            },
            'max_drawdown': {
                'sui': float((sui_price['price_usd'].cummax() - sui_price['price_usd']).max() / sui_price['price_usd'].cummax().max() * 100),
                'aptos': float((aptos_price['price_usd'].cummax() - aptos_price['price_usd']).max() / aptos_price['price_usd'].cummax().max() * 100)
            }
        }
    
    def _analyze_price_correlation(self, sui_price, aptos_price):
        """分析價格相關性"""
        # 合併數據以計算相關性
        merged = pd.merge(sui_price[['date', 'price_change_1d']], 
                         aptos_price[['date', 'price_change_1d']], 
                         on='date', suffixes=('_sui', '_aptos'))
        
        correlation = merged['price_change_1d_sui'].corr(merged['price_change_1d_aptos'])
        
        return {
            'daily_correlation': float(correlation),
            'correlation_strength': 'High' if abs(correlation) > 0.7 else 'Medium' if abs(correlation) > 0.4 else 'Low'
        }
    
    def _calculate_ecosystem_scores(self, sui_protocols, aptos_protocols, sui_price, aptos_price):
        """計算生態系統綜合評分 - 修正為以實際領先者為基準"""
        
        # TVL評分 - 以實際更高者為100分基準
        sui_tvl = sui_protocols['tvl_usd'].sum()
        aptos_tvl = aptos_protocols['tvl_usd'].sum()
        
        if sui_tvl > aptos_tvl:
            # Sui領先的情況
            tvl_score_sui = 100.0
            tvl_score_aptos = (aptos_tvl / sui_tvl) * 100
        else:
            # Aptos領先的情況
            tvl_score_aptos = 100.0
            tvl_score_sui = (sui_tvl / aptos_tvl) * 100
        
        # 多樣性評分 (協議數量)
        diversity_score_sui = len(sui_protocols)
        diversity_score_aptos = len(aptos_protocols)
        
        # 增長評分 (基於90天價格表現)
        sui_90d_return = ((sui_price.iloc[-1]['price_usd'] - sui_price.iloc[-91]['price_usd']) 
                         / sui_price.iloc[-91]['price_usd'] * 100) if len(sui_price) > 90 else 0
        aptos_90d_return = ((aptos_price.iloc[-1]['price_usd'] - aptos_price.iloc[-91]['price_usd']) 
                           / aptos_price.iloc[-91]['price_usd'] * 100) if len(aptos_price) > 90 else 0
        
        # 標準化評分 (0-100)
        growth_score_sui = max(0, min(100, 50 + sui_90d_return))
        growth_score_aptos = max(0, min(100, 50 + aptos_90d_return))
        
        # 綜合評分
        sui_overall = (tvl_score_sui * 0.4 + diversity_score_sui * 0.3 + growth_score_sui * 0.3)
        aptos_overall = (tvl_score_aptos * 0.4 + diversity_score_aptos * 0.3 + growth_score_aptos * 0.3)
        
        return {
            'sui_scores': {
                'tvl_score': float(tvl_score_sui),
                'diversity_score': float(diversity_score_sui),
                'growth_score': float(growth_score_sui),
                'overall_score': float(sui_overall)
            },
            'aptos_scores': {
                'tvl_score': float(tvl_score_aptos),
                'diversity_score': float(diversity_score_aptos),
                'growth_score': float(growth_score_aptos),
                'overall_score': float(aptos_overall)
            }
        }
    
    def save_processed_data(self, processed_data):
        """保存處理後的數據"""
        logger.info("開始保存處理後的數據...")
        
        timestamp = datetime.now().strftime('%Y%m%d')
        
        # 保存清理後的數據
        processed_data['sui_protocols_clean'].to_csv(
            f"{self.output_dir}/daily/sui_protocols_clean_{timestamp}.csv", index=False)
        
        processed_data['aptos_protocols_clean'].to_csv(
            f"{self.output_dir}/daily/aptos_protocols_clean_{timestamp}.csv", index=False)
        
        processed_data['sui_price_clean'].to_csv(
            f"{self.output_dir}/daily/sui_price_clean_{timestamp}.csv", index=False)
        
        processed_data['aptos_price_clean'].to_csv(
            f"{self.output_dir}/daily/aptos_price_clean_{timestamp}.csv", index=False)
        
        # 保存比較分析
        with open(f"{self.output_dir}/analysis_ready/comparative_analysis_{timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(processed_data['comparative_analysis'], f, ensure_ascii=False, indent=2)
        
        logger.info("處理後數據保存完成")
    
    def process_all_data(self):
        """執行完整的數據處理流程"""
        logger.info("=== 開始數據處理流程 ===")
        
        # 1. 載入原始數據
        raw_data = self.load_raw_data()
        if raw_data is None:
            logger.error("原始數據載入失敗，終止處理")
            return None
        
        # 2. 清理協議數據
        sui_protocols_clean = self.clean_protocol_data(raw_data['sui_protocols'], 'Sui')
        aptos_protocols_clean = self.clean_protocol_data(raw_data['aptos_protocols'], 'Aptos')
        
        # 3. 清理價格數據
        sui_price_clean = self.clean_price_data(raw_data['sui_price'], 'Sui')
        aptos_price_clean = self.clean_price_data(raw_data['aptos_price'], 'Aptos')
        
        # 4. 創建比較分析
        comparative_analysis = self.create_comparative_analysis(
            sui_protocols_clean, aptos_protocols_clean,
            sui_price_clean, aptos_price_clean
        )
        
        # 5. 整合處理後數據
        processed_data = {
            'sui_protocols_clean': sui_protocols_clean,
            'aptos_protocols_clean': aptos_protocols_clean,
            'sui_price_clean': sui_price_clean,
            'aptos_price_clean': aptos_price_clean,
            'comparative_analysis': comparative_analysis
        }
        
        # 6. 保存處理後數據
        self.save_processed_data(processed_data)
        
        logger.info("=== 數據處理流程完成 ===")
        return processed_data

def main():
    """主執行函數"""
    print("🔧 Sui vs Aptos 數據處理工具 (修正版)")
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    # 創建數據處理器
    processor = SuiAptosDataProcessor()
    
    try:
        # 執行數據處理
        processed_data = processor.process_all_data()
        
        if processed_data:
            print("\n📊 數據處理摘要:")
            print("=" * 40)
            
            # 顯示清理後的數據統計
            sui_clean = processed_data['sui_protocols_clean']
            aptos_clean = processed_data['aptos_protocols_clean']
            
            sui_tvl = sui_clean['tvl_usd'].sum() / 1e9
            aptos_tvl = aptos_clean['tvl_usd'].sum() / 1e9
            
            print(f"✅ Sui協議 (清理後): {len(sui_clean)} 個, TVL: ${sui_tvl:.2f}B")
            print(f"✅ Aptos協議 (清理後): {len(aptos_clean)} 個, TVL: ${aptos_tvl:.2f}B")
            
            leader = "Sui" if sui_tvl > aptos_tvl else "Aptos"
            ratio = max(sui_tvl, aptos_tvl) / min(sui_tvl, aptos_tvl)
            print(f"📈 TVL領先者: {leader} (領先 {ratio:.1f}倍)")
            
            print(f"✅ SUI價格數據: {len(processed_data['sui_price_clean'])} 天")
            print(f"✅ APT價格數據: {len(processed_data['aptos_price_clean'])} 天")
            
            # 顯示一些關鍵洞察
            comp_analysis = processed_data['comparative_analysis']
            if 'ecosystem_scores' in comp_analysis:
                scores = comp_analysis['ecosystem_scores']
                print(f"\n🏆 生態系統評分:")
                print(f"  Sui 綜合評分: {scores['sui_scores']['overall_score']:.1f}")
                print(f"  Aptos 綜合評分: {scores['aptos_scores']['overall_score']:.1f}")
            
            print(f"\n📁 處理後數據保存位置: {processor.output_dir}/")
            print(f"🕐 完成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return 0
        else:
            print("❌ 數據處理失敗")
            return 1
            
    except Exception as e:
        logger.error(f"數據處理過程中發生錯誤: {e}")
        print(f"❌ 執行失敗: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)