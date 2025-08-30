# powerbi_data_prep.py
# 為Power BI準備標準化數據表

import pandas as pd
import numpy as np
import json
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PowerBIDataPrep:
    """Power BI數據準備器"""
    
    def __init__(self):
        self.raw_data_dir = "../Data_Collection/raw_data"
        self.processed_data_dir = "../Data_Processing/processed_data"
        self.output_dir = "powerbi_data"
        
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info("Power BI數據準備器初始化完成")
    
    def create_protocols_combined_table(self):
        """創建合併的協議數據表"""
        logger.info("創建合併協議數據表...")
        
        try:
            # 載入清理後的數據
            sui_protocols = pd.read_csv(f"{self.processed_data_dir}/daily/sui_protocols_clean_20250822.csv")
            aptos_protocols = pd.read_csv(f"{self.processed_data_dir}/daily/aptos_protocols_clean_20250822.csv")
            
            # 標準化欄位
            sui_protocols['blockchain'] = 'Sui'
            aptos_protocols['blockchain'] = 'Aptos'
            
            # 確保兩個數據表有相同的欄位
            common_columns = [
                'name', 'category', 'tvl_usd', 'change_1d', 'change_7d', 'change_1m',
                'blockchain', 'category_clean', 'tvl_millions', 'tvl_rank'
            ]
            
            # 選擇共同欄位並合併
            sui_selected = sui_protocols[common_columns].copy()
            aptos_selected = aptos_protocols[common_columns].copy()
            
            protocols_combined = pd.concat([sui_selected, aptos_selected], ignore_index=True)
            
            # 添加計算欄位
            protocols_combined['tvl_billions'] = protocols_combined['tvl_usd'] / 1_000_000_000
            protocols_combined['tvl_log'] = np.log10(protocols_combined['tvl_usd'].replace(0, 1))
            
            # 添加大小分組
            protocols_combined['size_category'] = pd.cut(
                protocols_combined['tvl_usd'],
                bins=[0, 1e6, 10e6, 100e6, 1e9, float('inf')],
                labels=['<$1M', '$1M-$10M', '$10M-$100M', '$100M-$1B', '>$1B']
            )
            
            # 保存
            protocols_combined.to_csv(f"{self.output_dir}/protocols_combined.csv", index=False)
            logger.info(f"協議數據表已保存: {len(protocols_combined)} 條記錄")
            
            return protocols_combined
            
        except Exception as e:
            logger.error(f"創建協議數據表失敗: {e}")
            return None
    
    def create_price_combined_table(self):
        """創建合併的價格數據表"""
        logger.info("創建合併價格數據表...")
        
        try:
            # 載入價格數據
            sui_price = pd.read_csv(f"{self.raw_data_dir}/sui_data/sui_price_final_20250822.csv")
            aptos_price = pd.read_csv(f"{self.raw_data_dir}/aptos_data/aptos_price_final_20250822.csv")
            
            # 標準化欄位
            sui_price['blockchain'] = 'Sui'
            aptos_price['blockchain'] = 'Aptos'
            
            # 合併數據
            price_combined = pd.concat([sui_price, aptos_price], ignore_index=True)
            
            # 轉換日期格式
            price_combined['date'] = pd.to_datetime(price_combined['date'])
            price_combined['year'] = price_combined['date'].dt.year
            price_combined['month'] = price_combined['date'].dt.month
            price_combined['quarter'] = price_combined['date'].dt.quarter
            
            # 計算累積回報
            for blockchain in ['Sui', 'Aptos']:
                mask = price_combined['blockchain'] == blockchain
                prices = price_combined.loc[mask, 'price_usd'].values
                if len(prices) > 0:
                    first_price = prices[0]
                    price_combined.loc[mask, 'cumulative_return'] = (prices / first_price - 1) * 100
            
            # 計算移動平均
            price_combined = price_combined.sort_values(['blockchain', 'date'])
            price_combined['ma_7d'] = price_combined.groupby('blockchain')['price_usd'].transform(lambda x: x.rolling(7).mean())
            price_combined['ma_30d'] = price_combined.groupby('blockchain')['price_usd'].transform(lambda x: x.rolling(30).mean())
            
            # 保存
            price_combined.to_csv(f"{self.output_dir}/price_combined.csv", index=False)
            logger.info(f"價格數據表已保存: {len(price_combined)} 條記錄")
            
            return price_combined
            
        except Exception as e:
            logger.error(f"創建價格數據表失敗: {e}")
            return None
    
    def create_key_metrics_table(self):
        """創建關鍵指標匯總表"""
        logger.info("創建關鍵指標匯總表...")
        
        try:
            # 載入分析結果
            with open(f"{self.processed_data_dir}/analysis_ready/comparative_analysis_20250822.json", 'r') as f:
                analysis = json.load(f)
            
            # 載入流動性分析
            try:
                liquidity_files = [f for f in os.listdir("../03_Analysis/investment_analysis/liquidity_analysis/") if f.endswith('.json')]
                if liquidity_files:
                    with open(f"../03_Analysis/investment_analysis/liquidity_analysis/{liquidity_files[0]}", 'r') as f:
                        liquidity = json.load(f)
                else:
                    liquidity = {}
            except:
                logger.warning("無法載入流動性分析，使用默認值")
                liquidity = {}
            
            # 構建關鍵指標表
            key_metrics = []
            
            # 從正確的數據結構中提取總TVL和協議數
            sui_total_tvl = sum([cat['total_tvl'] for cat in analysis['protocol_comparison']['category_comparison']['sui_categories'].values()])
            aptos_total_tvl = sum([cat['total_tvl'] for cat in analysis['protocol_comparison']['category_comparison']['aptos_categories'].values()])
            sui_protocol_count = sum([cat['protocol_count'] for cat in analysis['protocol_comparison']['category_comparison']['sui_categories'].values()])
            aptos_protocol_count = sum([cat['protocol_count'] for cat in analysis['protocol_comparison']['category_comparison']['aptos_categories'].values()])
            
            # Sui指標
            key_metrics.append({
                'blockchain': 'Sui',
                'protocol_count': sui_protocol_count,
                'total_tvl': sui_total_tvl,
                'avg_protocol_tvl': sui_total_tvl / sui_protocol_count if sui_protocol_count > 0 else 0,
                'ecosystem_score': analysis['ecosystem_scores']['sui_scores']['overall_score'],
                'market_cap': liquidity.get('supply_metrics', {}).get('sui', {}).get('market_cap', 12040660241),
                'mcap_to_tvl': 0.49,  # 從前面的分析結果
                'circulation_ratio': liquidity.get('supply_metrics', {}).get('sui', {}).get('circulation_ratio', 0.35),
                'current_price': liquidity.get('supply_metrics', {}).get('sui', {}).get('price', 3.43)
            })
            
            # Aptos指標
            key_metrics.append({
                'blockchain': 'Aptos',
                'protocol_count': aptos_protocol_count,
                'total_tvl': aptos_total_tvl,
                'avg_protocol_tvl': aptos_total_tvl / aptos_protocol_count if aptos_protocol_count > 0 else 0,
                'ecosystem_score': analysis['ecosystem_scores']['aptos_scores']['overall_score'],
                'market_cap': liquidity.get('supply_metrics', {}).get('aptos', {}).get('market_cap', 2971662109),
                'mcap_to_tvl': 0.06,  # 從前面的分析結果
                'circulation_ratio': liquidity.get('supply_metrics', {}).get('aptos', {}).get('circulation_ratio', 0.59),
                'current_price': liquidity.get('supply_metrics', {}).get('aptos', {}).get('price', 4.33)
            })
            
            key_metrics_df = pd.DataFrame(key_metrics)
            
            # 添加比較指標
            sui_row = key_metrics_df[key_metrics_df['blockchain'] == 'Sui'].iloc[0]
            aptos_row = key_metrics_df[key_metrics_df['blockchain'] == 'Aptos'].iloc[0]
            
            # 計算相對指標
            key_metrics_df['tvl_vs_aptos'] = key_metrics_df['total_tvl'] / aptos_row['total_tvl']
            key_metrics_df['mcap_vs_aptos'] = key_metrics_df['market_cap'] / aptos_row['market_cap']
            key_metrics_df['efficiency_vs_aptos'] = (key_metrics_df['total_tvl'] / key_metrics_df['market_cap']) / (aptos_row['total_tvl'] / aptos_row['market_cap'])
            
            # 保存
            key_metrics_df.to_csv(f"{self.output_dir}/key_metrics.csv", index=False)
            logger.info("關鍵指標表已保存")
            
            return key_metrics_df
            
        except Exception as e:
            logger.error(f"創建關鍵指標表失敗: {e}")
            return None
    
    def create_analysis_results_table(self):
        """創建分析結果匯總表"""
        logger.info("創建分析結果匯總表...")
        
        try:
            # 構建分析結果表
            analysis_results = []
            
            # 價格表現結果
            periods = ['7_days', '30_days', '90_days']
            performance_data = {
                '7_days': {'sui': -7.1, 'aptos': -4.5},
                '30_days': {'sui': -7.6, 'aptos': -7.4},
                '90_days': {'sui': -4.8, 'aptos': -16.3}
            }
            
            for period in periods:
                analysis_results.append({
                    'analysis_type': 'Price Performance',
                    'metric': f'{period}_return',
                    'sui_value': performance_data[period]['sui'],
                    'aptos_value': performance_data[period]['aptos'],
                    'sui_advantage': performance_data[period]['sui'] - performance_data[period]['aptos'],
                    'unit': 'percentage'
                })
            
            # 估值指標
            analysis_results.extend([
                {
                    'analysis_type': 'Valuation',
                    'metric': 'mcap_to_tvl',
                    'sui_value': 0.49,
                    'aptos_value': 0.06,
                    'sui_advantage': 0.49 - 0.06,
                    'unit': 'ratio'
                },
                {
                    'analysis_type': 'Valuation',
                    'metric': 'valuation_premium',
                    'sui_value': 712,
                    'aptos_value': 0,
                    'sui_advantage': 712,
                    'unit': 'percentage'
                },
                {
                    'analysis_type': 'Supply',
                    'metric': 'circulation_ratio',
                    'sui_value': 35,
                    'aptos_value': 59,
                    'sui_advantage': 35 - 59,
                    'unit': 'percentage'
                }
            ])
            
            # 生態指標
            analysis_results.extend([
                {
                    'analysis_type': 'Ecosystem',
                    'metric': 'protocol_count',
                    'sui_value': 77,
                    'aptos_value': 68,
                    'sui_advantage': 9,
                    'unit': 'count'
                },
                {
                    'analysis_type': 'Ecosystem',
                    'metric': 'ecosystem_score',
                    'sui_value': 56.6,
                    'aptos_value': 70.5,
                    'sui_advantage': 56.6 - 70.5,
                    'unit': 'score'
                }
            ])
            
            analysis_df = pd.DataFrame(analysis_results)
            
            # 添加勝負標示
            analysis_df['winner'] = analysis_df.apply(
                lambda row: 'Sui' if row['sui_advantage'] > 0 else 'Aptos' if row['sui_advantage'] < 0 else 'Tie', 
                axis=1
            )
            
            # 保存
            analysis_df.to_csv(f"{self.output_dir}/analysis_results.csv", index=False)
            logger.info("分析結果表已保存")
            
            return analysis_df
            
        except Exception as e:
            logger.error(f"創建分析結果表失敗: {e}")
            return None
    
    def create_powerbi_summary(self):
        """創建Power BI摘要信息"""
        logger.info("創建Power BI摘要信息...")
        
        summary = {
            'project_info': {
                'title': 'Sui vs Aptos Investment Analysis',
                'subtitle': 'Market Inefficiency: APT Trading at 712% Discount',
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'data_period': '365 Days',
                'analyst': 'Amanda - Data Science Portfolio'
            },
            'key_findings': [
                'APT creates 10x more TVL per token than SUI',
                'SUI trades at 712% premium to APT on TVL basis',
                'APT has 2x total TVL but 1/4 market cap of SUI',
                'SUI has 65% token supply locked vs APT 41%',
                'APT scored higher on ecosystem fundamentals (70.5 vs 56.6)'
            ],
            'investment_thesis': {
                'recommendation': 'Strong Buy APT, Reduce SUI',
                'rationale': 'Massive valuation inefficiency with APT fundamentally superior',
                'confidence': 'High',
                'time_horizon': '3-6 months'
            },
            'data_quality': {
                'protocols_analyzed': 145,
                'days_of_price_data': 366,
                'data_sources': ['DefiLlama', 'CoinGecko'],
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        with open(f"{self.output_dir}/powerbi_summary.json", 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info("Power BI摘要已保存")
        return summary
    
    def prepare_all_data(self):
        """準備所有Power BI所需數據"""
        logger.info("=== 開始準備Power BI數據 ===")
        
        results = {}
        
        # 創建各種數據表
        results['protocols'] = self.create_protocols_combined_table()
        results['prices'] = self.create_price_combined_table()
        results['key_metrics'] = self.create_key_metrics_table()
        results['analysis_results'] = self.create_analysis_results_table()
        results['summary'] = self.create_powerbi_summary()
        
        logger.info("=== Power BI數據準備完成 ===")
        return results

def main():
    print("Power BI數據準備工具")
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    prep = PowerBIDataPrep()
    
    try:
        results = prep.prepare_all_data()
        
        print("\n數據準備完成:")
        print("=" * 40)
        
        # 顯示生成的文件
        files = os.listdir(prep.output_dir)
        for file in files:
            file_path = os.path.join(prep.output_dir, file)
            if file.endswith('.csv'):
                df = pd.read_csv(file_path)
                print(f"✅ {file}: {len(df)} 條記錄")
            else:
                print(f"✅ {file}: 配置文件")
        
        print(f"\nPower BI數據已保存到: {prep.output_dir}/")
        print("可以開始創建Power BI儀表板了!")
        
        return 0
        
    except Exception as e:
        print(f"數據準備失敗: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)