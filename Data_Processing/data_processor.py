# Sui vs Aptos 數據處理和清理工具

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import os
import logging
import warnings
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
        """載入原始數據"""
        logger.info("開始載入原始數據...")
        
        data = {}
        
        try:
            # 載入協議數據
            data['sui_protocols'] = pd.read_csv(f"{self.raw_data_dir}/sui_data/sui_protocols_final_20250822.csv")
            data['aptos_protocols'] = pd.read_csv(f"{self.raw_data_dir}/aptos_data/aptos_protocols_final_20250822.csv")
            
            # 載入價格數據
            data['sui_price'] = pd.read_csv(f"{self.raw_data_dir}/sui_data/sui_price_final_20250822.csv")
            data['aptos_price'] = pd.read_csv(f"{self.raw_data_dir}/aptos_data/aptos_price_final_20250822.csv")
            
            # 載入比較分析
            with open(f"{self.raw_data_dir}/comparison_data/sui_vs_aptos_final_20250822.json", 'r', encoding='utf-8') as f:
                data['comparison'] = json.load(f)
            
            logger.info("原始數據載入成功:")
            logger.info(f"  Sui協議: {len(data['sui_protocols'])} 個")
            logger.info(f"  Aptos協議: {len(data['aptos_protocols'])} 個")
            logger.info(f"  SUI價格: {len(data['sui_price'])} 天")
            logger.info(f"  APT價格: {len(data['aptos_price'])} 天")
            
            return data
            
        except Exception as e:
            logger.error(f"數據載入失敗: {e}")
            return None
    
    def clean_protocol_data(self, protocols_df, chain_name):
        """清理協議數據"""
        logger.info(f"開始清理 {chain_name} 協議數據...")
        
        # 複製數據避免修改原始數據
        df = protocols_df.copy()
        
        # 1. 移除明顯的CEX和非DeFi協議
        exclude_keywords = ['Binance', 'CEX', 'Exchange', 'OKX', 'Gate', 'Bybit', 'Coinbase', 'Kraken']
        exclude_pattern = '|'.join(exclude_keywords)
        
        initial_count = len(df)
        df = df[~df['name'].str.contains(exclude_pattern, case=False, na=False)]
        logger.info(f"  移除CEX協議: {initial_count - len(df)} 個")
        
        # 2. 處理TVL異常值
        # 移除TVL為0或負數的協議
        df = df[df['tvl_usd'] > 0]
        
        # 標記TVL異常高的協議 (可能是數據錯誤)
        tvl_q99 = df['tvl_usd'].quantile(0.99)
        df['is_outlier'] = df['tvl_usd'] > tvl_q99
        logger.info(f"  標記TVL異常值: {df['is_outlier'].sum()} 個 (>99分位數: ${tvl_q99:,.0f})")
        
        # 3. 分類標準化
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
            'Unknown': 'Others'
        }
        
        df['category_clean'] = df['category'].map(category_mapping).fillna('Others')
        
        # 4. 添加計算欄位
        df['tvl_millions'] = df['tvl_usd'] / 1_000_000
        df['tvl_billions'] = df['tvl_usd'] / 1_000_000_000
        
        # 5. 計算增長評分 (基於變化率)
        df['growth_score'] = (
            df['change_7d'].fillna(0) * 0.5 + 
            df['change_1m'].fillna(0) * 0.3 + 
            df['change_1d'].fillna(0) * 0.2
        )
        
        # 6. 添加排名
        df['tvl_rank'] = df['tvl_usd'].rank(method='dense', ascending=False)
        df['growth_rank'] = df['growth_score'].rank(method='dense', ascending=False)
        
        logger.info(f"  {chain_name} 清理完成: {len(df)} 個有效協議")
        
        return df
    
    def clean_price_data(self, price_df, chain_name):
        """清理價格數據"""
        logger.info(f"開始清理 {chain_name} 價格數據...")
        
        df = price_df.copy()
        
        # 1. 轉換日期格式
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # 2. 處理缺失值
        numeric_cols = ['price_usd', 'market_cap_usd', 'volume_24h_usd']
        for col in numeric_cols:
            if col in df.columns:
                # 使用前值填充小量缺失值
                df[col] = df[col].fillna(method='ffill')
        
        # 3. 計算技術指標
        df['price_change_1d'] = df['price_usd'].pct_change()
        df['price_change_7d'] = df['price_usd'].pct_change(periods=7)
        df['price_change_30d'] = df['price_usd'].pct_change(periods=30)
        
        # 4. 移動平均
        df['ma_7d'] = df['price_usd'].rolling(window=7).mean()
        df['ma_30d'] = df['price_usd'].rolling(window=30).mean()
        
        # 5. 波動率 (30天滾動標準差)
        df['volatility_30d'] = df['price_change_1d'].rolling(window=30).std()
        
        # 6. 累積回報
        df['cumulative_return'] = (df['price_usd'] / df['price_usd'].iloc[0] - 1) * 100
        
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
        
        # 3. 綜合評分
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
        """計算生態系統綜合評分"""
        
        # TVL評分 (Aptos = 100, Sui相對評分)
        sui_tvl = sui_protocols['tvl_usd'].sum()
        aptos_tvl = aptos_protocols['tvl_usd'].sum()
        tvl_score_sui = (sui_tvl / aptos_tvl) * 100
        tvl_score_aptos = 100
        
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
    print("🔧 Sui vs Aptos 數據處理工具")
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
            print(f"✅ Sui協議 (清理後): {len(processed_data['sui_protocols_clean'])} 個")
            print(f"✅ Aptos協議 (清理後): {len(processed_data['aptos_protocols_clean'])} 個")
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