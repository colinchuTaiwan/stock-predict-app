import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, base64, requests, time, math, uuid
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 系統環境與核心配置 (v10.0 SSOT)
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)

st_autorefresh(interval=5000, key="v10_hf_engine")

state_schema = {
    "raw_signals": {},         
    "df_portfolio": pd.DataFrame(),
    "last_run_key": "",
    "is_scanning": False,
    "scan_idx": 0,
    "market_regime": {"status": "Neutral", "score": 1.0},
    "execution_token": ""
}
for key, val in state_schema.items():
    if key not in st.session_state: st.session_state[key] = val

# ==============================
# 1. 因子提取與擠壓診斷 (Squeeze Logic)
# ==============================
def extract_v10_factors(df):
    try:
        c, v, o = df['Close'], df['Volume'], df['Open']
        
        # A. 均線系統 (5, 10, 20, 60, 100, 200)
        ma_periods = [5, 10, 20, 60, 100, 200]
        ma_s = {f'ma{p}': c.rolling(p).mean() for p in ma_periods}
        ma_l = [ma_s[f'ma{p}'].iloc[-1] for p in ma_periods]
        
        # B. 布林帶寬擠壓 (ma_b logic)
        # ma_b[period] = (UpperBand - LowerBand) / MidBand
        def get_bandwidth(p):
            mid = ma_s[f'ma{p}']
            std = c.rolling(p).std()
            bandwidth = (4 * std) / mid
            return bandwidth.iloc[-1]
        
        ma_b = {p: get_bandwidth(p) for p in [20, 60, 100, 200]}
        
        # C. 🔥 核心糾結判斷 (用戶邏輯整合)
        res_type = "分散"
        squeeze_bonus = 1.0
        
        # 邏輯優先序：從最嚴格的六線開始
        if (max(ma_l)/min(ma_l) < 1.06) and ma_b[200] < 0.12:
            res_type, squeeze_bonus = "六線糾結", 2.0
        elif (max(ma_l[:5])/min(ma_l[:5]) < 1.06) and ma_b[100] < 0.12:
            res_type, squeeze_bonus = "五線糾結", 1.8
        elif (max(ma_l[:4])/min(ma_l[:4]) < 1.06) and ma_b[60] < 0.12:
            res_type, squeeze_bonus = "四線糾結", 1.6
        elif (max(ma_l[:3])/min(ma_l[:3]) < 1.06) and ma_b[20] < 0.12:
            res_type, squeeze_bonus = "三線糾結", 1.3

        # D. 統計因子
        ret = c.pct_change()
        # 下行風險 (Downside Deviation)
        down_risk = ret[ret < 0].tail(20).std() * math.sqrt(252)
        v_ratio = v.iloc[-1] / v.rolling(20).mean().iloc[-1]
        
        return {
            "mom": ((c.iloc[-1] - o.iloc[-1]) * 100 / o.iloc[-1]), # 當日動能
            "risk": down_risk if not math.isnan(down_risk) else 0.5,
            "v_score": math.log1p(v_ratio),
            "squeeze_type": res_type,
            "squeeze_bonus": squeeze_bonus,
            "price": round(c.iloc[-1], 2),
            "ts": time.time()
        }
    except: return None

# ==============================
# 2. 截面標準化與優化 (Portfolio Optimization)
# ==============================
def build_v10_portfolio(raw_signals):
    if not raw_signals: return pd.DataFrame()
    
    df = pd.DataFrame.from_dict(raw_signals, orient='index')
    
    # A. Z-Score 正規化 (Cross-sectional)
    for col in ['mom', 'v_score']:
        df[f'z_{col}'] = (df[col] - df[col].mean()) / (df[col].std() + 1e-6)
    
    # B. 風險倒數標準化
    df['inv_risk'] = 1 / (df['risk'] + 0.05)
    df['z_risk'] = (df['inv_risk'] - df['inv_risk'].mean()) / (df['inv_risk'].std() + 1e-6)
    
    # C. 🔥 綜合權重 + Squeeze Bonus
    # 權重分配：動能(35%) + 風險(35%) + 量能(30%)
    df['composite_score'] = (df['z_mom'] * 0.35 + df['z_risk'] * 0.35 + df['z_v_score'] * 0.30) * df['squeeze_bonus']
    
    # D. Softmax 資本分配 (Position Sizing)
    top_k = df.sort_values('composite_score', ascending=False).head(12).copy()
    exp_s = np.exp(top_k['composite_score'] - top_k['composite_score'].max()) # 數值穩定處理
    top_k['建議配置%'] = (exp_s / exp_s.sum()) * 100
    
    top_k['股票代號'] = top_k.index
    return top_k[['股票代號', 'price', 'squeeze_type', 'composite_score', '建議配置%']]

# ==============================
# 3. 數據抓取與任務控制
# ==============================
def fetch_v10_data(codes, token):
    if token != st.session_state.execution_token: return None
    try:
        # 抓取 350 天確保包含 MA200 運算窗口
        raw = yf.download(tickers=codes, period="350d", group_by="ticker", threads=True, progress=False)
        results = {}
        for code in codes:
            try:
                df = raw.xs(code, level=0, axis=1).dropna() if len(codes) > 1 else raw.dropna()
                if len(df) < 240: continue 
                f = extract_v10_factors(df)
                if f: results[code] = f
            except: continue
        return results
    except: return None

# [排程與自動執行模組]
SCHEDULE = ["09:30", "10:50", "12:20", "13:10", "15:00", "22:46"]
now_t = now_taipei()
for t_str in SCHEDULE:
    sched = datetime.combine(now_t.date(), datetime.strptime(t_str, "%H:%M").time()).replace(tzinfo=tz)
    if abs((now_t - sched).total_seconds()) <= 60:
        if st.session_state.last_run_key != t_str and not st.session_state.is_scanning:
            st.session_state.update({"execution_token": str(uuid.uuid4()), "is_scanning": True, "last_run_key": t_str, "scan_idx": 0, "raw_signals": {}})
            break

if st.session_state.is_scanning:
    try:
        with open("db/taiwan_Full.json", "r") as f: universe = json.load(f)['stocks']
    except: universe = ["2330.TW", "2454.TW", "2317.TW"]
    
    idx = st.session_state.scan_idx
    if idx >= len(universe):
        st.session_state.is_scanning = False
        st.session_state.df_portfolio = build_v10_portfolio(st.session_state.raw_signals)
    else:
        batch = universe[idx : idx + 35]
        st.info(f"🏛️ v10 核心引擎運算中: {idx}/{len(universe)}")
        res = fetch_v10_data(batch, st.session_state.execution_token)
        if res:
            st.session_state.raw_signals.update(res)
            st.session_state.scan_idx += 35

# ==============================
# 4. 專業資產配置看板 (Dashboard)
# ==============================
st.title("🏦 Quant Hedge Fund Engine v10.0")
st.markdown("---")

c1, c2, c3 = st.columns(3)
c1.metric("⏰ 台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("🧬 擠壓偵測數", len([v for v in st.session_state.raw_signals.values() if v['squeeze_type'] != "分散"]))
c3.metric("💼 投資組合標的", len(st.session_state.df_portfolio))

if not st.session_state.df_portfolio.empty:
    st.subheader("🏆 最優資本分配組合 (Squeeze-Driven Allocation)")
    
    display_df = st.session_state.df_portfolio.copy()
    display_df.columns = ["股票代號", "結算價", "糾結級別", "統計總分", "建議配置%"]
    
    # 視覺化邏輯：針對高級別糾結顯示特殊背景
    st.dataframe(
        display_df.style.background_gradient(cmap='YlGn', subset=['建議配置%'])
        .apply(lambda x: ['background-color: #1b5e20; color: white; font-weight: bold' if v == '六線糾結' else '' for v in x], subset=['糾結級別'])
        .format({"建議配置%": "{:.2f}%", "統計總分": "{:.2f}"}),
        use_container_width=True, hide_index=True
    )
    
    # 資金權重圖
    st.bar_chart(display_df.set_index("股票代號")["建議配置%"])
    
    st.success("🎯 **策略提示**：當前組合聚焦於具備『長週期均線糾結』且『波動率極度壓縮』之標的，通常預示重大變盤在即。")
else:
    st.info("⌛ 正在收集市場數據進行六線糾結診斷與 Z-Score 標準化優化...")
