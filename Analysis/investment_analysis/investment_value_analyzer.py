# investment_value_analyzer.py
# 投資價值悖論深度分析：為什麼Aptos評分高但SUI價格表現更好？

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import os
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InvestmentValueAnalyzer:
    """投資價值悖論分析器"""
    
    def __init__(self, processed_data_dir="../Data_Processing/processed_data"):
        self.processed_data_dir = processed_data_dir
        self.output_dir = "investment_analysis"
        
        # 創建輸出目錄
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(f"{self.output_dir}/charts", exist_ok=True)
        os.makedirs(f"{self.output_dir}/models", exist_ok=True)
        
        logger.info("投資價值分析器初始化完成")
    
    def load_processed_data(self):
        """載入處理後的數據"""
        logger.info("載入處理後的數據...")
        
        try:
            # 載入清理後的數據
            data = {}
            data['sui_protocols'] = pd.read_csv(f"{self.processed_data_dir}/daily/sui_protocols_clean_20250822.csv")
            data['aptos_protocols'] = pd.read_csv(f"{self.processed_data_dir}/daily/aptos_protocols_clean_20250822.csv")
            data['sui_price'] = pd.read_csv(f"{self.processed_data_dir}/daily/sui_price_clean_20250822.csv")
            data['aptos_price'] = pd.read_csv(f"{self.processed_data_dir}/daily/aptos_price_clean_20250822.csv")
            
            # 載入比較分析
            with open(f"{self.processed_data_dir}/analysis_ready/comparative_analysis_20250822.json", 'r') as f:
                data['comparative_analysis'] = json.load(f)
            
            # 轉換日期格式
            data['sui_price']['date'] = pd.to_datetime(data['sui_price']['date'])
            data['aptos_price']['date'] = pd.to_datetime(data['aptos_price']['date'])
            
            logger.info("數據載入成功")
            return data
            
        except Exception as e:
            logger.error(f"數據載入失敗: {e}")
            return None
    
    def calculate_valuation_metrics(self, data):
        """計算估值指標"""
        logger.info("計算估值指標...")
        
        # 獲取最新價格和市值
        sui_latest = data['sui_price'].iloc[-1]
        aptos_latest = data['aptos_price'].iloc[-1]
        
        # 計算TVL
        sui_tvl = data['sui_protocols']['tvl_usd'].sum()
        aptos_tvl = data['aptos_protocols']['tvl_usd'].sum()
        
        # 估值指標
        valuation_metrics = {
            'sui': {
                'current_price': float(sui_latest['price_usd']),
                'market_cap': float(sui_latest['market_cap_usd']),
                'total_tvl': float(sui_tvl),
                'mcap_to_tvl_ratio': float(sui_latest['market_cap_usd'] / sui_tvl),
                'protocol_count': len(data['sui_protocols']),
                'avg_protocol_tvl': float(sui_tvl / len(data['sui_protocols'])),
                'tvl_per_billion_mcap': float(sui_tvl / (sui_latest['market_cap_usd'] / 1e9))
            },
            'aptos': {
                'current_price': float(aptos_latest['price_usd']),
                'market_cap': float(aptos_latest['market_cap_usd']),
                'total_tvl': float(aptos_tvl),
                'mcap_to_tvl_ratio': float(aptos_latest['market_cap_usd'] / aptos_tvl),
                'protocol_count': len(data['aptos_protocols']),
                'avg_protocol_tvl': float(aptos_tvl / len(data['aptos_protocols'])),
                'tvl_per_billion_mcap': float(aptos_tvl / (aptos_latest['market_cap_usd'] / 1e9))
            }
        }
        
        # 相對估值
        valuation_metrics['relative'] = {
            'sui_mcap_premium': ((valuation_metrics['sui']['mcap_to_tvl_ratio'] / 
                                 valuation_metrics['aptos']['mcap_to_tvl_ratio']) - 1) * 100,
            'tvl_efficiency_ratio': (valuation_metrics['sui']['tvl_per_billion_mcap'] / 
                                   valuation_metrics['aptos']['tvl_per_billion_mcap']),
            'protocol_efficiency': (valuation_metrics['sui']['avg_protocol_tvl'] / 
                                  valuation_metrics['aptos']['avg_protocol_tvl'])
        }
        
        return valuation_metrics
    
    def analyze_price_performance_anomaly(self, data):
        """分析價格表現異常"""
        logger.info("分析價格表現與基本面的不一致性...")
        
        # 計算不同時間段的表現
        periods = [7, 30, 90, 180]
        performance_analysis = {}
        
        for period in periods:
            if len(data['sui_price']) > period and len(data['aptos_price']) > period:
                # 計算回報率
                sui_start = data['sui_price'].iloc[-period-1]['price_usd']
                sui_end = data['sui_price'].iloc[-1]['price_usd']
                sui_return = ((sui_end - sui_start) / sui_start) * 100
                
                aptos_start = data['aptos_price'].iloc[-period-1]['price_usd']
                aptos_end = data['aptos_price'].iloc[-1]['price_usd']
                aptos_return = ((aptos_end - aptos_start) / aptos_start) * 100
                
                # 計算波動率
                sui_vol = data['sui_price'].tail(period)['price_change_1d'].std() * np.sqrt(365)
                aptos_vol = data['aptos_price'].tail(period)['price_change_1d'].std() * np.sqrt(365)
                
                # 風險調整收益 (簡化的夏普比率)
                sui_sharpe = sui_return / (sui_vol * 100) if sui_vol > 0 else 0
                aptos_sharpe = aptos_return / (aptos_vol * 100) if aptos_vol > 0 else 0
                
                performance_analysis[f'{period}d'] = {
                    'sui_return': float(sui_return),
                    'aptos_return': float(aptos_return),
                    'sui_outperformance': float(sui_return - aptos_return),
                    'sui_volatility': float(sui_vol),
                    'aptos_volatility': float(aptos_vol),
                    'sui_sharpe': float(sui_sharpe),
                    'aptos_sharpe': float(aptos_sharpe),
                    'sharpe_advantage': float(sui_sharpe - aptos_sharpe)
                }
        
        return performance_analysis
    
    def analyze_fundamental_vs_technical(self, data, valuation_metrics):
        """分析基本面vs技術面的背離"""
        logger.info("分析基本面與技術面的背離...")
        
        # 基本面評分 (來自之前的分析)
        ecosystem_scores = data['comparative_analysis']['ecosystem_scores']
        
        fundamental_analysis = {
            'fundamental_scores': {
                'sui_score': ecosystem_scores['sui_scores']['overall_score'],
                'aptos_score': ecosystem_scores['aptos_scores']['overall_score'],
                'aptos_advantage': ecosystem_scores['aptos_scores']['overall_score'] - ecosystem_scores['sui_scores']['overall_score']
            },
            'valuation_scores': valuation_metrics,
            'disconnect_analysis': {}
        }
        
        # 計算基本面與市場表現的背離度
        fundamental_advantage = fundamental_analysis['fundamental_scores']['aptos_advantage']  # Aptos基本面優勢
        market_advantage = valuation_metrics['sui']['mcap_to_tvl_ratio'] - valuation_metrics['aptos']['mcap_to_tvl_ratio']  # 市場估值差異
        
        fundamental_analysis['disconnect_analysis'] = {
            'fundamental_advantage_aptos': float(fundamental_advantage),  # 基本面：Aptos更好
            'market_valuation_gap': float(market_advantage),  # 市場：估值差異
            'disconnect_severity': abs(fundamental_advantage - market_advantage),  # 背離程度
            'interpretation': self._interpret_disconnect(fundamental_advantage, market_advantage)
        }
        
        return fundamental_analysis
    
    def _interpret_disconnect(self, fundamental_gap, market_gap):
        """解釋基本面與市場的背離"""
        if fundamental_gap > 5 and market_gap < 0:
            return "Strong Disconnect: Aptos fundamentally stronger but Sui valued higher - potential Sui overvaluation"
        elif fundamental_gap > 5 and market_gap > 0:
            return "Aligned: Both fundamentals and market favor Aptos"
        elif fundamental_gap < -5 and market_gap > 0:
            return "Strong Disconnect: Sui fundamentally stronger but Aptos valued higher - potential Aptos overvaluation"
        else:
            return "Mild Disconnect: Fundamentals and market roughly aligned"
    
    def calculate_investment_opportunities(self, valuation_metrics, performance_analysis, fundamental_analysis):
        """計算投資機會"""
        logger.info("計算投資機會和建議...")
        
        investment_analysis = {
            'value_opportunities': {},
            'risk_assessment': {},
            'investment_recommendation': {}
        }
        
        # 價值機會分析
        mcap_ratio = valuation_metrics['sui']['market_cap'] / valuation_metrics['aptos']['market_cap']
        tvl_ratio = valuation_metrics['sui']['total_tvl'] / valuation_metrics['aptos']['total_tvl']
        
        investment_analysis['value_opportunities'] = {
            'tvl_vs_mcap_analysis': {
                'sui_tvl_efficiency': float(valuation_metrics['sui']['tvl_per_billion_mcap']),
                'aptos_tvl_efficiency': float(valuation_metrics['aptos']['tvl_per_billion_mcap']),
                'efficiency_ratio': float(valuation_metrics['relative']['tvl_efficiency_ratio']),
                'interpretation': "Sui more TVL-efficient" if valuation_metrics['relative']['tvl_efficiency_ratio'] > 1 else "Aptos more TVL-efficient"
            },
            'growth_vs_valuation': {
                'sui_90d_performance': performance_analysis.get('90d', {}).get('sui_return', 0),
                'aptos_90d_performance': performance_analysis.get('90d', {}).get('aptos_return', 0),
                'performance_gap': performance_analysis.get('90d', {}).get('sui_outperformance', 0),
                'valuation_justification': self._assess_valuation_justification(performance_analysis, valuation_metrics)
            }
        }
        
        # 風險評估
        investment_analysis['risk_assessment'] = {
            'volatility_comparison': {
                'sui_30d_vol': performance_analysis.get('30d', {}).get('sui_volatility', 0),
                'aptos_30d_vol': performance_analysis.get('30d', {}).get('aptos_volatility', 0),
                'risk_adjusted_preference': "Sui" if performance_analysis.get('30d', {}).get('sui_sharpe', 0) > performance_analysis.get('30d', {}).get('aptos_sharpe', 0) else "Aptos"
            },
            'concentration_risk': {
                'sui_protocol_diversity': len(valuation_metrics['sui']) if 'sui' in valuation_metrics else 0,
                'aptos_protocol_diversity': len(valuation_metrics['aptos']) if 'aptos' in valuation_metrics else 0,
                'diversification_advantage': "Sui" if valuation_metrics['sui']['protocol_count'] > valuation_metrics['aptos']['protocol_count'] else "Aptos"
            }
        }
        
        # 投資建議
        investment_analysis['investment_recommendation'] = self._generate_investment_recommendation(
            valuation_metrics, performance_analysis, fundamental_analysis
        )
        
        return investment_analysis
    
    def _assess_valuation_justification(self, performance_analysis, valuation_metrics):
        """評估估值是否合理"""
        sui_90d_perf = performance_analysis.get('90d', {}).get('sui_return', 0)
        aptos_90d_perf = performance_analysis.get('90d', {}).get('aptos_return', 0)
        
        mcap_ratio = valuation_metrics['sui']['market_cap'] / valuation_metrics['aptos']['market_cap']
        
        if sui_90d_perf > aptos_90d_perf and mcap_ratio < 1:
            return "Sui outperforming but lower valuation - potential undervaluation"
        elif sui_90d_perf < aptos_90d_perf and mcap_ratio > 1:
            return "Sui underperforming but higher valuation - potential overvaluation"
        else:
            return "Performance and valuation relatively aligned"
    
    def _generate_investment_recommendation(self, valuation_metrics, performance_analysis, fundamental_analysis):
        """生成投資建議"""
        
        # 評分系統 (1-10)
        sui_score = 0
        aptos_score = 0
        
        # 基本面評分權重 30%
        fundamental_weight = 0.3
        if fundamental_analysis['fundamental_scores']['sui_score'] > fundamental_analysis['fundamental_scores']['aptos_score']:
            sui_score += 3 * fundamental_weight * 10
        else:
            aptos_score += 3 * fundamental_weight * 10
        
        # 價格表現權重 40%
        performance_weight = 0.4
        sui_90d = performance_analysis.get('90d', {}).get('sui_return', 0)
        aptos_90d = performance_analysis.get('90d', {}).get('aptos_return', 0)
        if sui_90d > aptos_90d:
            sui_score += 4 * performance_weight * 10
        else:
            aptos_score += 4 * performance_weight * 10
        
        # 估值效率權重 30%
        valuation_weight = 0.3
        if valuation_metrics['relative']['tvl_efficiency_ratio'] > 1:
            sui_score += 3 * valuation_weight * 10
        else:
            aptos_score += 3 * valuation_weight * 10
        
        # 標準化評分
        total_score = sui_score + aptos_score
        sui_final_score = (sui_score / total_score) * 10 if total_score > 0 else 5
        aptos_final_score = (aptos_score / total_score) * 10 if total_score > 0 else 5
        
        # 生成建議
        if sui_final_score > 6.5:
            recommendation = "Strong Buy SUI"
            rationale = "Superior price performance and TVL efficiency despite lower fundamental score"
        elif aptos_final_score > 6.5:
            recommendation = "Strong Buy APT"
            rationale = "Superior fundamentals justify higher valuation"
        elif sui_final_score > aptos_final_score:
            recommendation = "Moderate Buy SUI"
            rationale = "Slight advantage in performance and value"
        else:
            recommendation = "Moderate Buy APT"
            rationale = "Fundamental strength outweighs recent underperformance"
        
        return {
            'sui_investment_score': float(sui_final_score),
            'aptos_investment_score': float(aptos_final_score),
            'recommendation': recommendation,
            'rationale': rationale,
            'confidence_level': "High" if abs(sui_final_score - aptos_final_score) > 2 else "Medium",
            'time_horizon': "3-6 months",
            'key_risks': [
                "Market volatility in crypto sector",
                "Regulatory changes affecting DeFi",
                "Competition from other L1 blockchains",
                "Technical risks in smart contracts"
            ]
        }
    
    def save_analysis_results(self, analysis_results):
        """保存分析結果"""
        logger.info("保存投資價值分析結果...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 保存完整分析結果
        with open(f"{self.output_dir}/investment_value_analysis_{timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2)
        
        # 生成投資報告摘要
        self._generate_investment_summary(analysis_results, timestamp)
        
        logger.info(f"分析結果已保存到 {self.output_dir}/")
    
    def _generate_investment_summary(self, analysis_results, timestamp):
        """生成投資報告摘要"""
        
        summary = f"""
# Sui vs Aptos 投資價值分析報告
生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🎯 核心發現
{analysis_results['investment_opportunities']['investment_recommendation']['recommendation']}
置信度: {analysis_results['investment_opportunities']['investment_recommendation']['confidence_level']}

## 📊 投資評分
- SUI 投資評分: {analysis_results['investment_opportunities']['investment_recommendation']['sui_investment_score']:.1f}/10
- APT 投資評分: {analysis_results['investment_opportunities']['investment_recommendation']['aptos_investment_score']:.1f}/10

## 💡 投資邏輯
{analysis_results['investment_opportunities']['investment_recommendation']['rationale']}

## 📈 關鍵指標
### 估值效率
- SUI TVL效率: {analysis_results['valuation_metrics']['sui']['tvl_per_billion_mcap']:.1f}
- APT TVL效率: {analysis_results['valuation_metrics']['aptos']['tvl_per_billion_mcap']:.1f}

### 90天表現
- SUI 回報: {analysis_results['performance_analysis'].get('90d', {}).get('sui_return', 0):.1f}%
- APT 回報: {analysis_results['performance_analysis'].get('90d', {}).get('aptos_return', 0):.1f}%

## ⚠️ 風險提醒
{chr(10).join(['- ' + risk for risk in analysis_results['investment_opportunities']['investment_recommendation']['key_risks']])}

## 🕐 建議持有期間
{analysis_results['investment_opportunities']['investment_recommendation']['time_horizon']}
"""
        
        with open(f"{self.output_dir}/investment_summary_{timestamp}.md", 'w', encoding='utf-8') as f:
            f.write(summary)
    
    def run_complete_analysis(self):
        """執行完整的投資價值分析"""
        logger.info("=== 開始投資價值悖論分析 ===")
        
        # 1. 載入數據
        data = self.load_processed_data()
        if data is None:
            logger.error("數據載入失敗，終止分析")
            return None
        
        # 2. 計算估值指標
        valuation_metrics = self.calculate_valuation_metrics(data)
        
        # 3. 分析價格表現異常
        performance_analysis = self.analyze_price_performance_anomaly(data)
        
        # 4. 分析基本面vs技術面
        fundamental_analysis = self.analyze_fundamental_vs_technical(data, valuation_metrics)
        
        # 5. 計算投資機會
        investment_opportunities = self.calculate_investment_opportunities(
            valuation_metrics, performance_analysis, fundamental_analysis
        )
        
        # 6. 整合分析結果
        analysis_results = {
            'analysis_metadata': {
                'analysis_type': 'Investment Value Paradox Analysis',
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'core_question': 'Why does Aptos score higher (70.5 vs 56.6) but SUI performs better in price?'
            },
            'valuation_metrics': valuation_metrics,
            'performance_analysis': performance_analysis,
            'fundamental_analysis': fundamental_analysis,
            'investment_opportunities': investment_opportunities
        }
        
        # 7. 保存結果
        self.save_analysis_results(analysis_results)
        
        logger.info("=== 投資價值分析完成 ===")
        return analysis_results

def main():
    """主執行函數"""
    print("💰 Sui vs Aptos 投資價值悖論分析")
    print("🔍 核心問題: 為什麼Aptos評分高但SUI價格表現更好？")
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    # 創建分析器
    analyzer = InvestmentValueAnalyzer()
    
    try:
        # 執行完整分析
        results = analyzer.run_complete_analysis()
        
        if results:
            print("\n💡 核心發現:")
            print("=" * 50)
            
            # 顯示投資建議
            recommendation = results['investment_opportunities']['investment_recommendation']
            print(f"🎯 投資建議: {recommendation['recommendation']}")
            print(f"📊 SUI 評分: {recommendation['sui_investment_score']:.1f}/10")
            print(f"📊 APT 評分: {recommendation['aptos_investment_score']:.1f}/10")
            print(f"🎪 置信度: {recommendation['confidence_level']}")
            
            print(f"\n💡 投資邏輯:")
            print(f"   {recommendation['rationale']}")
            
            # 顯示關鍵指標
            valuation = results['valuation_metrics']
            print(f"\n📈 關鍵估值指標:")
            print(f"   SUI 市值/TVL: {valuation['sui']['mcap_to_tvl_ratio']:.2f}")
            print(f"   APT 市值/TVL: {valuation['aptos']['mcap_to_tvl_ratio']:.2f}")
            print(f"   TVL效率比: {valuation['relative']['tvl_efficiency_ratio']:.2f}")
            
            print(f"\n📁 詳細分析報告已保存到: {analyzer.output_dir}/")
            print(f"🕐 完成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return 0
        else:
            print("❌ 分析失敗")
            return 1
            
    except Exception as e:
        logger.error(f"分析過程中發生錯誤: {e}")
        print(f"❌ 執行失敗: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)