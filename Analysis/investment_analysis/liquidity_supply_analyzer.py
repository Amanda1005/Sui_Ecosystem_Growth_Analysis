# liquidity_supply_analyzer.py
# 流動性和供應分析 - 解釋估值差異的根本原因

import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime, timedelta
import os
import logging
import time
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LiquiditySupplyAnalyzer:
    """流動性和供應分析器"""
    
    def __init__(self, processed_data_dir="../../Data_Processing/processed_data"):
        self.processed_data_dir = processed_data_dir
        self.output_dir = "liquidity_analysis"
        self.coingecko_base = "https://api.coingecko.com/api/v3"
        
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info("流動性分析器初始化完成")
    
    def _make_request(self, url, delay=2):
        """API請求"""
        try:
            time.sleep(delay)
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API請求失敗: {e}")
            return None
    
    def get_token_supply_data(self):
        """獲取代幣供應數據"""
        logger.info("獲取代幣供應數據...")
        
        supply_data = {}
        
        # 獲取SUI供應信息
        sui_url = f"{self.coingecko_base}/coins/sui"
        sui_data = self._make_request(sui_url)
        
        if sui_data:
            supply_data['sui'] = {
                'total_supply': sui_data.get('market_data', {}).get('total_supply'),
                'circulating_supply': sui_data.get('market_data', {}).get('circulating_supply'),
                'max_supply': sui_data.get('market_data', {}).get('max_supply'),
                'market_cap': sui_data.get('market_data', {}).get('market_cap', {}).get('usd'),
                'fully_diluted_valuation': sui_data.get('market_data', {}).get('fully_diluted_valuation', {}).get('usd'),
                'current_price': sui_data.get('market_data', {}).get('current_price', {}).get('usd')
            }
        
        # 獲取APT供應信息
        apt_url = f"{self.coingecko_base}/coins/aptos"
        apt_data = self._make_request(apt_url)
        
        if apt_data:
            supply_data['aptos'] = {
                'total_supply': apt_data.get('market_data', {}).get('total_supply'),
                'circulating_supply': apt_data.get('market_data', {}).get('circulating_supply'),
                'max_supply': apt_data.get('market_data', {}).get('max_supply'),
                'market_cap': apt_data.get('market_data', {}).get('market_cap', {}).get('usd'),
                'fully_diluted_valuation': apt_data.get('market_data', {}).get('fully_diluted_valuation', {}).get('usd'),
                'current_price': apt_data.get('market_data', {}).get('current_price', {}).get('usd')
            }
        
        return supply_data
    
    def calculate_supply_metrics(self, supply_data):
        """計算供應指標"""
        logger.info("計算供應指標...")
        
        metrics = {}
        
        for chain in ['sui', 'aptos']:
            data = supply_data.get(chain, {})
            
            if data.get('circulating_supply') and data.get('total_supply'):
                circulation_ratio = data['circulating_supply'] / data['total_supply']
            else:
                circulation_ratio = 1.0  # 假設全流通
            
            metrics[chain] = {
                'circulating_supply': data.get('circulating_supply', 0),
                'total_supply': data.get('total_supply', 0),
                'max_supply': data.get('max_supply', 0),
                'circulation_ratio': circulation_ratio,
                'supply_overhang': 1 - circulation_ratio,  # 未流通比例
                'market_cap': data.get('market_cap', 0),
                'fdv': data.get('fully_diluted_valuation', 0),
                'mcap_fdv_ratio': data.get('market_cap', 0) / data.get('fully_diluted_valuation', 1),
                'price': data.get('current_price', 0)
            }
        
        return metrics
    
    def analyze_valuation_efficiency(self, supply_metrics):
        """分析估值效率"""
        logger.info("分析估值效率...")
        
        # 載入TVL數據
        try:
            sui_protocols = pd.read_csv(f"{self.processed_data_dir}/daily/sui_protocols_clean_20250822.csv")
            aptos_protocols = pd.read_csv(f"{self.processed_data_dir}/daily/aptos_protocols_clean_20250822.csv")
            
            sui_tvl = sui_protocols['tvl_usd'].sum()
            aptos_tvl = aptos_protocols['tvl_usd'].sum()
        except Exception as e:
            logger.error(f"載入TVL數據失敗: {e}")
            sui_tvl = aptos_tvl = 0
        
        efficiency_analysis = {}
        
        for chain in ['sui', 'aptos']:
            metrics = supply_metrics[chain]
            tvl = sui_tvl if chain == 'sui' else aptos_tvl
            
            efficiency_analysis[chain] = {
                'tvl': tvl,
                'market_cap': metrics['market_cap'],
                'fdv': metrics['fdv'],
                'mcap_to_tvl': metrics['market_cap'] / tvl if tvl > 0 else 0,
                'fdv_to_tvl': metrics['fdv'] / tvl if tvl > 0 else 0,
                'tvl_per_token_circulating': tvl / metrics['circulating_supply'] if metrics['circulating_supply'] > 0 else 0,
                'tvl_per_token_total': tvl / metrics['total_supply'] if metrics['total_supply'] > 0 else 0,
                'price_to_tvl_per_token': metrics['price'] / (tvl / metrics['circulating_supply']) if metrics['circulating_supply'] > 0 and tvl > 0 else 0
            }
        
        # 相對比較
        efficiency_analysis['comparison'] = {
            'sui_mcap_premium': (efficiency_analysis['sui']['mcap_to_tvl'] / efficiency_analysis['aptos']['mcap_to_tvl'] - 1) * 100 if efficiency_analysis['aptos']['mcap_to_tvl'] > 0 else 0,
            'sui_fdv_premium': (efficiency_analysis['sui']['fdv_to_tvl'] / efficiency_analysis['aptos']['fdv_to_tvl'] - 1) * 100 if efficiency_analysis['aptos']['fdv_to_tvl'] > 0 else 0,
            'tvl_efficiency_ratio': efficiency_analysis['sui']['tvl_per_token_circulating'] / efficiency_analysis['aptos']['tvl_per_token_circulating'] if efficiency_analysis['aptos']['tvl_per_token_circulating'] > 0 else 0,
            'valuation_explanation': self._explain_valuation_gap(efficiency_analysis)
        }
        
        return efficiency_analysis
    
    def _explain_valuation_gap(self, efficiency_analysis):
        """解釋估值差異"""
        sui_mcap_tvl = efficiency_analysis['sui']['mcap_to_tvl']
        aptos_mcap_tvl = efficiency_analysis['aptos']['mcap_to_tvl']
        
        ratio = sui_mcap_tvl / aptos_mcap_tvl if aptos_mcap_tvl > 0 else 0
        
        if ratio > 5:
            return f"SUI市值/TVL比率高出APT {ratio:.1f}倍，顯示嚴重估值溢價"
        elif ratio > 2:
            return f"SUI估值溢價{ratio:.1f}倍，可能存在過度炒作"
        elif ratio < 0.5:
            return f"APT估值溢價{1/ratio:.1f}倍，SUI可能被低估"
        else:
            return "兩者估值相對合理"
    
    def calculate_liquidity_metrics(self):
        """計算流動性指標"""
        logger.info("計算流動性指標...")
        
        # 獲取交易所數據
        sui_exchanges_url = f"{self.coingecko_base}/coins/sui/tickers"
        aptos_exchanges_url = f"{self.coingecko_base}/coins/aptos/tickers"
        
        sui_tickers = self._make_request(sui_exchanges_url)
        aptos_tickers = self._make_request(aptos_exchanges_url)
        
        liquidity_metrics = {}
        
        for chain, tickers in [('sui', sui_tickers), ('aptos', aptos_tickers)]:
            if not tickers or 'tickers' not in tickers:
                continue
            
            volumes = []
            spreads = []
            exchanges = set()
            
            for ticker in tickers['tickers'][:20]:  # 前20個交易對
                if ticker.get('volume'):
                    volumes.append(ticker['volume'])
                
                if ticker.get('bid_ask_spread_percentage'):
                    spreads.append(ticker['bid_ask_spread_percentage'])
                
                if ticker.get('market', {}).get('name'):
                    exchanges.add(ticker['market']['name'])
            
            liquidity_metrics[chain] = {
                'total_volume_24h': sum(volumes),
                'avg_spread': np.mean(spreads) if spreads else 0,
                'exchange_count': len(exchanges),
                'top_exchanges': list(exchanges)[:10],
                'volume_distribution': {
                    'top_1': max(volumes) if volumes else 0,
                    'top_3': sum(sorted(volumes, reverse=True)[:3]) if len(volumes) >= 3 else sum(volumes),
                    'concentration_ratio': max(volumes) / sum(volumes) if volumes else 0
                }
            }
        
        return liquidity_metrics
    
    def analyze_supply_inflation_risk(self, supply_metrics):
        """分析供應通脹風險"""
        logger.info("分析供應通脹風險...")
        
        inflation_analysis = {}
        
        for chain in ['sui', 'aptos']:
            metrics = supply_metrics[chain]
            
            # 計算潛在稀釋
            potential_dilution = (metrics['total_supply'] - metrics['circulating_supply']) / metrics['circulating_supply'] if metrics['circulating_supply'] > 0 else 0
            
            # 風險等級
            if potential_dilution > 2:
                risk_level = "Very High"
            elif potential_dilution > 1:
                risk_level = "High"
            elif potential_dilution > 0.5:
                risk_level = "Medium"
            else:
                risk_level = "Low"
            
            inflation_analysis[chain] = {
                'potential_dilution': potential_dilution,
                'supply_overhang': metrics['supply_overhang'],
                'mcap_fdv_ratio': metrics['mcap_fdv_ratio'],
                'inflation_risk_level': risk_level,
                'price_impact_estimate': -potential_dilution / (1 + potential_dilution) if potential_dilution > 0 else 0
            }
        
        return inflation_analysis
    
    def generate_investment_implications(self, efficiency_analysis, liquidity_metrics, inflation_analysis):
        """生成投資含義"""
        logger.info("生成投資含義...")
        
        implications = {
            'valuation_conclusion': {},
            'liquidity_assessment': {},
            'risk_factors': {},
            'investment_recommendation': {}
        }
        
        # 估值結論
        sui_premium = efficiency_analysis['comparison']['sui_mcap_premium']
        
        implications['valuation_conclusion'] = {
            'main_finding': f"SUI市值/TVL溢價{sui_premium:.0f}%",
            'explanation': efficiency_analysis['comparison']['valuation_explanation'],
            'fair_value_assessment': self._assess_fair_value(efficiency_analysis),
            'valuation_justification': self._justify_valuation_gap(efficiency_analysis, inflation_analysis)
        }
        
        # 流動性評估
        if liquidity_metrics:
            implications['liquidity_assessment'] = {
                'sui_liquidity_advantage': liquidity_metrics.get('sui', {}).get('total_volume_24h', 0) > liquidity_metrics.get('aptos', {}).get('total_volume_24h', 0),
                'spread_comparison': f"SUI平均價差: {liquidity_metrics.get('sui', {}).get('avg_spread', 0):.2f}%, APT: {liquidity_metrics.get('aptos', {}).get('avg_spread', 0):.2f}%",
                'liquidity_premium_justified': liquidity_metrics.get('sui', {}).get('total_volume_24h', 0) / liquidity_metrics.get('aptos', {}).get('total_volume_24h', 1) > 2
            }
        
        # 風險因素
        implications['risk_factors'] = {
            'sui_dilution_risk': inflation_analysis['sui']['inflation_risk_level'],
            'aptos_dilution_risk': inflation_analysis['aptos']['inflation_risk_level'],
            'valuation_sustainability': "Low" if sui_premium > 500 else "Medium" if sui_premium > 200 else "High"
        }
        
        # 投資建議
        implications['investment_recommendation'] = self._generate_final_recommendation(
            efficiency_analysis, liquidity_metrics, inflation_analysis, sui_premium
        )
        
        return implications
    
    def _assess_fair_value(self, efficiency_analysis):
        """評估合理價值"""
        sui_ratio = efficiency_analysis['sui']['mcap_to_tvl']
        aptos_ratio = efficiency_analysis['aptos']['mcap_to_tvl']
        
        if sui_ratio > aptos_ratio * 3:
            return "SUI可能被高估50-70%"
        elif sui_ratio > aptos_ratio * 2:
            return "SUI可能被高估30-50%"
        elif aptos_ratio > sui_ratio * 2:
            return "APT可能被低估30-50%"
        else:
            return "估值相對合理"
    
    def _justify_valuation_gap(self, efficiency_analysis, inflation_analysis):
        """為估值差異尋找合理性"""
        reasons = []
        
        # 檢查供應通脹差異
        sui_dilution = inflation_analysis['sui']['potential_dilution']
        aptos_dilution = inflation_analysis['aptos']['potential_dilution']
        
        if abs(sui_dilution - aptos_dilution) > 0.5:
            reasons.append(f"供應通脹風險差異 (SUI: {sui_dilution:.1f}, APT: {aptos_dilution:.1f})")
        
        # 檢查生態效率
        sui_tvl_per_token = efficiency_analysis['sui']['tvl_per_token_circulating']
        aptos_tvl_per_token = efficiency_analysis['aptos']['tvl_per_token_circulating']
        
        if sui_tvl_per_token < aptos_tvl_per_token * 0.5:
            reasons.append("APT單位代幣創造更多TVL價值")
        
        return reasons if reasons else ["估值差異難以用基本面解釋，可能存在市場失效"]
    
    def _generate_final_recommendation(self, efficiency_analysis, liquidity_metrics, inflation_analysis, premium):
        """生成最終投資建議"""
        
        # 評分系統
        score = 5  # 基準分
        
        # 估值因素 (40%權重)
        if premium > 500:
            score -= 2
        elif premium > 300:
            score -= 1
        elif premium < 100:
            score += 1
        
        # 供應通脹風險 (30%權重)
        if inflation_analysis['sui']['inflation_risk_level'] == "High":
            score -= 1
        if inflation_analysis['aptos']['inflation_risk_level'] == "Low":
            score += 1
        
        # 流動性優勢 (30%權重)
        if liquidity_metrics and liquidity_metrics.get('sui', {}).get('total_volume_24h', 0) > liquidity_metrics.get('aptos', {}).get('total_volume_24h', 0):
            score += 0.5
        
        # 生成建議
        if score >= 6:
            recommendation = "Strong Buy APT, Avoid SUI"
            rationale = "APT被嚴重低估，SUI估值不可持續"
        elif score >= 5.5:
            recommendation = "Buy APT, Reduce SUI"
            rationale = "APT價值回歸機會大，SUI估值風險高"
        elif score <= 4:
            recommendation = "Hold SUI, Cautious on APT"
            rationale = "SUI溢價可能有其合理性"
        else:
            recommendation = "Balanced Approach"
            rationale = "兩者各有優劣，建議分散配置"
        
        return {
            'recommendation': recommendation,
            'rationale': rationale,
            'confidence': "High" if abs(score - 5) > 1 else "Medium",
            'key_risk': "估值差異可能持續，市場可能長期失效"
        }
    
    def run_complete_analysis(self):
        """運行完整分析"""
        logger.info("=== 開始流動性和供應分析 ===")
        
        # 1. 獲取供應數據
        supply_data = self.get_token_supply_data()
        
        # 2. 計算供應指標
        supply_metrics = self.calculate_supply_metrics(supply_data)
        
        # 3. 分析估值效率
        efficiency_analysis = self.analyze_valuation_efficiency(supply_metrics)
        
        # 4. 分析流動性
        liquidity_metrics = self.calculate_liquidity_metrics()
        
        # 5. 分析通脹風險
        inflation_analysis = self.analyze_supply_inflation_risk(supply_metrics)
        
        # 6. 生成投資含義
        investment_implications = self.generate_investment_implications(
            efficiency_analysis, liquidity_metrics, inflation_analysis
        )
        
        # 整合結果
        complete_analysis = {
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'supply_metrics': supply_metrics,
            'efficiency_analysis': efficiency_analysis,
            'liquidity_metrics': liquidity_metrics,
            'inflation_analysis': inflation_analysis,
            'investment_implications': investment_implications
        }
        
        # 保存結果
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        with open(f"{self.output_dir}/liquidity_supply_analysis_{timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(complete_analysis, f, ensure_ascii=False, indent=2)
        
        logger.info("=== 流動性和供應分析完成 ===")
        return complete_analysis

def main():
    print("🔄 流動性和供應分析")
    print("🎯 目標: 解釋8倍估值差異的根本原因")
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    analyzer = LiquiditySupplyAnalyzer()
    
    try:
        results = analyzer.run_complete_analysis()
        
        if results:
            print("\n核心發現:")
            print("=" * 40)
            
            # 顯示關鍵指標
            efficiency = results['efficiency_analysis']
            print(f"SUI 市值/TVL: {efficiency['sui']['mcap_to_tvl']:.2f}")
            print(f"APT 市值/TVL: {efficiency['aptos']['mcap_to_tvl']:.2f}")
            print(f"SUI估值溢價: {efficiency['comparison']['sui_mcap_premium']:.0f}%")
            
            # 顯示投資建議
            recommendation = results['investment_implications']['investment_recommendation']
            print(f"\n投資建議: {recommendation['recommendation']}")
            print(f"理由: {recommendation['rationale']}")
            
            print(f"\n詳細分析已保存到: {analyzer.output_dir}/")
            
        return 0
    except Exception as e:
        print(f"分析失敗: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)