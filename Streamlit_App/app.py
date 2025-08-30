# app.py - Sui vs Aptos

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime
import numpy as np
from pathlib import Path
import pandas as pd
import json
import streamlit as st

# 頁面配置
st.set_page_config(
    page_title="Sui vs Aptos Ecosystem Analysis",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定義CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    .highlight-text {
        color: #ff6b6b;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """從 Power_BI/powerbi_data 讀取資料（從 app.py 往上找）"""
    try:
        # 這裡才是關鍵：從 app.py 的位置往上跳一層，再進入 Power_BI/powerbi_data
        base_path = Path(__file__).resolve().parent.parent / "Power_BI" / "powerbi_data"

        protocols = pd.read_csv(base_path / "protocols_combined.csv")
        prices = pd.read_csv(base_path / "price_combined.csv")
        key_metrics = pd.read_csv(base_path / "key_metrics.csv")
        analysis_results = pd.read_csv(base_path / "analysis_results.csv")

        with open(base_path / "powerbi_summary.json", 'r') as f:
            summary = json.load(f)

        return protocols, prices, key_metrics, analysis_results, summary
    except Exception as e:
        st.error(f"❌ 數據載入失敗: {e}")
        return None, None, None, None, None

def main():
    """主應用程序"""
    
    # 載入數據
    protocols, prices, key_metrics, analysis_results, summary = load_data()
    
    if protocols is None:
        st.error("無法載入數據，請確認數據文件存在")
        return
    
    # 主標題
    st.markdown('<h1 class="main-header">Sui vs Aptos Ecosystem Analysis</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; font-size: 1.2rem; color: #666;">Move Language Blockchain Ecosystem Comparison</p>', unsafe_allow_html=True)
    
    # 側邊欄
    st.sidebar.title("分析導航")
    
    # 頁面選擇
    pages = {
        "📊 執行摘要": "executive_summary",
        "🔍 深度分析": "deep_analysis", 
        "💹 價格分析": "price_analysis",
        "🏗️ 協議生態": "protocol_ecosystem"
    }
    
    selected_page = st.sidebar.selectbox("選擇頁面", list(pages.keys()))
    page_key = pages[selected_page]
    
    # 側邊欄關鍵指標
    st.sidebar.markdown("### 關鍵指標")
    
    sui_metrics = key_metrics[key_metrics['blockchain'] == 'Sui'].iloc[0]
    aptos_metrics = key_metrics[key_metrics['blockchain'] == 'Aptos'].iloc[0]
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.metric("SUI 市值/TVL", f"{sui_metrics['mcap_to_tvl']:.2f}")
    with col2:
        st.metric("APT 市值/TVL", f"{aptos_metrics['mcap_to_tvl']:.2f}")
    
    st.sidebar.metric("估值差異", "712%", delta="顯著差異")
    
    # 根據選擇的頁面渲染內容
    if page_key == "executive_summary":
        render_executive_summary(key_metrics, analysis_results)
    elif page_key == "deep_analysis":
        render_deep_analysis(protocols, key_metrics, analysis_results)
    elif page_key == "price_analysis":
        render_price_analysis(prices, analysis_results)
    elif page_key == "protocol_ecosystem":
        render_protocol_ecosystem(protocols)

def render_executive_summary(key_metrics, analysis_results):
    """渲染執行摘要頁面"""
    st.header("📊 執行摘要")
    
    # 核心發現
    col1, col2, col3, col4 = st.columns(4)
    
    sui_metrics = key_metrics[key_metrics['blockchain'] == 'Sui'].iloc[0]
    aptos_metrics = key_metrics[key_metrics['blockchain'] == 'Aptos'].iloc[0]
    
    with col1:
        st.metric(
            label="APT TVL優勢", 
            value=f"{aptos_metrics['total_tvl']/1e9:.1f}B",
            delta=f"+{(aptos_metrics['total_tvl']/sui_metrics['total_tvl']-1)*100:.0f}%"
        )
    
    with col2:
        st.metric(
            label="SUI 估值溢價", 
            value="712%",
            delta="過高估值", 
            delta_color="inverse"
        )
    
    with col3:
        st.metric(
            label="APT 協議效率", 
            value="10倍",
            delta="每代幣TVL創造力"
        )
    
    with col4:
        st.metric(
            label="主要發現", 
            value="估值差異",
            delta="數據分析洞察"
        )
    
    # 估值比較圖
    st.subheader("市值 vs TVL 比較")
    
    fig = go.Figure()
    
    # 市值條
    fig.add_trace(go.Bar(
        name='市值',
        x=['Sui', 'Aptos'],
        y=[sui_metrics['market_cap']/1e9, aptos_metrics['market_cap']/1e9],
        marker_color='lightblue'
    ))
    
    # TVL條
    fig.add_trace(go.Bar(
        name='TVL',
        x=['Sui', 'Aptos'],
        y=[sui_metrics['total_tvl']/1e9, aptos_metrics['total_tvl']/1e9],
        marker_color='lightgreen'
    ))
    
    fig.update_layout(
        title="市值 vs TVL 對比 (十億美元)",
        xaxis_title="區塊鏈",
        yaxis_title="價值 (十億美元)",
        barmode='group',
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 關鍵洞察
    st.subheader("🎯 關鍵洞察")
    
    insights = [
        "APT擁有2倍於SUI的TVL，但市值僅為SUI的1/4",
        "APT每個代幣創造的TVL價值是SUI的10倍以上",
        "SUI的712%估值溢價無法被基本面支撐"
    ]
    
    for insight in insights:
        st.info(f"💡 {insight}")

def render_deep_analysis(protocols, key_metrics, analysis_results):
    """渲染深度分析頁面"""
    st.header("🔍 深度分析")
    
    # 協議數量分析
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("協議數量分佈")
        
        protocol_counts = protocols.groupby('blockchain').size().reset_index(name='count')
        
        fig = px.pie(
            protocol_counts, 
            values='count', 
            names='blockchain',
            title="協議數量分佈",
            color_discrete_map={'Sui': '#4A90E2', 'Aptos': '#7ED321'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("TVL分佈")
        
        tvl_by_chain = protocols.groupby('blockchain')['tvl_usd'].sum().reset_index()
        tvl_by_chain['tvl_billions'] = tvl_by_chain['tvl_usd'] / 1e9
        
        fig = px.pie(
            tvl_by_chain, 
            values='tvl_billions', 
            names='blockchain',
            title="總TVL分佈 (十億美元)",
            color_discrete_map={'Sui': '#4A90E2', 'Aptos': '#7ED321'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # 分類分析
    st.subheader("協議分類分析")
    
    category_analysis = protocols.groupby(['blockchain', 'category_clean']).agg({
        'tvl_usd': ['sum', 'count', 'mean']
    }).reset_index()
    
    category_analysis.columns = ['blockchain', 'category', 'total_tvl', 'protocol_count', 'avg_tvl']
    
    # 選擇顯示方式
    analysis_type = st.selectbox(
        "選擇分析維度",
        ["總TVL", "協議數量", "平均TVL"]
    )
    
    if analysis_type == "總TVL":
        y_col = 'total_tvl'
        title = "各類別總TVL對比"
    elif analysis_type == "協議數量":
        y_col = 'protocol_count'
        title = "各類別協議數量對比"
    else:
        y_col = 'avg_tvl'
        title = "各類別平均TVL對比"
    
    fig = px.bar(
        category_analysis,
        x='category',
        y=y_col,
        color='blockchain',
        barmode='group',
        title=title,
        color_discrete_map={'Sui': '#4A90E2', 'Aptos': '#7ED321'}
    )
    
    fig.update_layout(xaxis_tickangle=45)
    st.plotly_chart(fig, use_container_width=True)

def render_price_analysis(prices, analysis_results):
    """渲染價格分析頁面"""
    st.header("💹 價格分析")
    
    # 轉換日期格式
    prices['date'] = pd.to_datetime(prices['date'])
    
    # 價格走勢圖
    st.subheader("365天價格走勢")
    
    fig = go.Figure()
    
    for blockchain in ['Sui', 'Aptos']:
        data = prices[prices['blockchain'] == blockchain].sort_values('date')
        
        fig.add_trace(go.Scatter(
            x=data['date'],
            y=data['price_usd'],
            mode='lines',
            name=blockchain,
            line=dict(width=2)
        ))
    
    fig.update_layout(
        title="SUI vs APT 價格走勢",
        xaxis_title="日期",
        yaxis_title="價格 (USD)",
        height=500,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 回報率比較
    st.subheader("回報率比較")
    
    performance_data = analysis_results[analysis_results['analysis_type'] == 'Price Performance']
    
    col1, col2, col3 = st.columns(3)
    
    periods = ['7_days', '30_days', '90_days']
    period_names = ['7天', '30天', '90天']
    
    for i, (period, name) in enumerate(zip(periods, period_names)):
        with [col1, col2, col3][i]:
            period_data = performance_data[performance_data['metric'] == f'{period}_return']
            if not period_data.empty:
                row = period_data.iloc[0]
                
                st.metric(
                    label=f"{name}回報率",
                    value=f"SUI: {row['sui_value']:.1f}%",
                    delta=f"vs APT: {row['sui_advantage']:+.1f}%"
                )
    
    # 累積回報圖
    st.subheader("累積回報比較")
    
    fig = go.Figure()
    
    for blockchain in ['Sui', 'Aptos']:
        data = prices[prices['blockchain'] == blockchain].sort_values('date')
        if 'cumulative_return' in data.columns:
            fig.add_trace(go.Scatter(
                x=data['date'],
                y=data['cumulative_return'],
                mode='lines',
                name=f'{blockchain} 累積回報',
                line=dict(width=2)
            ))
    
    fig.update_layout(
        title="累積回報比較 (%)",
        xaxis_title="日期",
        yaxis_title="累積回報 (%)",
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)

def render_protocol_ecosystem(protocols):
    """渲染協議生態頁面"""
    st.header("🏗️ 協議生態分析")
    
    # 篩選器
    col1, col2 = st.columns(2)
    
    with col1:
        selected_blockchain = st.selectbox(
            "選擇區塊鏈",
            ["全部", "Sui", "Aptos"]
        )
    
    with col2:
        selected_category = st.selectbox(
            "選擇分類",
            ["全部"] + list(protocols['category_clean'].unique())
        )
    
    # 應用篩選器
    filtered_data = protocols.copy()
    
    if selected_blockchain != "全部":
        filtered_data = filtered_data[filtered_data['blockchain'] == selected_blockchain]
    
    if selected_category != "全部":
        filtered_data = filtered_data[filtered_data['category_clean'] == selected_category]
    
    # 前20大協議
    st.subheader("頂級協議排行")
    
    top_protocols = filtered_data.nlargest(20, 'tvl_usd')
    
    fig = px.bar(
        top_protocols,
        x='tvl_usd',
        y='name',
        color='blockchain',
        title="前20大協議 (按TVL排序)",
        orientation='h',
        color_discrete_map={'Sui': '#4A90E2', 'Aptos': '#7ED321'}
    )
    
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # 協議規模分佈
    st.subheader("協議規模分佈")
    
    fig = px.histogram(
        filtered_data,
        x='tvl_log',
        color='blockchain',
        nbins=20,
        title="協議規模分佈 (對數尺度)",
        barmode='overlay',
        opacity=0.7,
        color_discrete_map={'Sui': '#4A90E2', 'Aptos': '#7ED321'}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 詳細數據表
    st.subheader("協議詳細資料")
    
    display_columns = ['name', 'blockchain', 'category_clean', 'tvl_millions', 'change_7d', 'change_1m']
    
    st.dataframe(
        filtered_data[display_columns].sort_values('tvl_millions', ascending=False),
        use_container_width=True
    )

# 運行應用
if __name__ == "__main__":
    main()