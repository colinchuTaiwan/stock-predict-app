import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta, timezone
import glob
import time
from streamlit_autorefresh import st_autorefresh

# =============================
# 0. 時區設定與工具函數
# =============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

# =============================
# 1. 頁面配置與排程
# =============================
st.set_page_config(page_title="台股多頭排列自動掃描", layout="wide")

SCHEDULE_TIMES = [
    "07:00","09:20","10:30","11:20","12:20",
    "13:20","15:00","18:00","22:30","23:30"
]

# =============================
# 2. 指標計算與資料處理
# =============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5,10,20,60,100,200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"])/df[f"ma{w}"]
    for w in [5,10,20,60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)

    df["pre_close"] = df['Close'].shift(1)
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    df["mv20"] = df['Volume'].rolling(20).mean()/1000
    df["RK_p"] = (close - df['Open'])*100/df['Open']
    return df

def extract_latest(df, ind):
    if len(df)<3: return None
    last_3 = df.tail(3)
    d = {
        'price': last_3['Close'].iloc[-1],
        'open': last_3['Open'].iloc[-1],
        'high': last_3['High'].iloc[-1],
        'vol': last_3['Volume'].iloc[-1]/1000,
        'pre_close': last_3['Close'].iloc[-2],
        'pre_high': last_3['High'].iloc[-2],
        'pre_vol': last_3['Volume'].iloc[-2]/1000,
    }
    if not ind.empty:
        d.update(ind.iloc[-1].to_dict())
    return d

# =============================
# 3. 掃描核心函數 (維持原邏輯)
# =============================
def run_scan_logic(stock_codes):
    # 使用分批下載以提升穩定性
    all_found = []
    batch_size = 50
    total = len(stock_codes)
    pbar = st.progress(0)
    status_text = st.empty()

    for i in range(0, total, batch_size):
        batch = stock_codes[i : i + batch_size]
        status_text.text(f"⏳ 正在下載並分析: {i}/{total} 檔...")
        try:
            raw = yf.download(tickers=batch, period="300d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
            if raw.empty: continue

            for code in batch:
                try:
                    df = raw[code].dropna() if len(batch)>1 else raw.dropna()
                    if len(df)<200: continue
                    ind = calc_indicators(df)
                    d = extract_latest(df, ind)
                    if not d: continue

                    price, RK_p, vol = round(d['price'],2), round(d['RK_p'],1), int(d['vol'])
                    ma = {w:d[f'ma{w}'] for w in [5,10,20,60,100,200]}
                    ma_d = {w:d[f'ma{w}_d'] for w in [5,10,20,60,100,200]}
                    pre_ma = {w:ind[f'ma{w}'].iloc[-2] for w in [5,10,20,60]}

                    if not (1<RK_p<7): continue

                    cond_basic = (
                        price > d['pre_high'] and d['mv20']>100 and vol>100 and
                        price>ma[20] and price>ma[60] and price>ma[100] and price>ma[200] and
                        all(v>0 for v in ma_d.values()) and vol > d['pre_vol']*1.5
                    )
                    if not cond_basic: continue

                    is_breakout = any(d['pre_close'] < pre_ma[w] for w in [5,10,20,60])
                    if not is_breakout: continue

                    res_type=""
                    mv = list(ma.values())
                    if max(mv)/min(mv)<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60]>ma[100]>ma[200] and d['ma200_b']<0.08:
                        res_type="六線多排"
                    elif max(mv[:-1])/min(mv[:-1])<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60]>ma[100] and d['ma100_b']<0.08:
                        res_type="五線多排"
                    elif max(mv[:4])/min(mv[:4])<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60] and d['ma60_b']<0.08:
                        res_type="四線多排"
                    elif max(mv[:3])/min(mv[:3])<1.06 and price>ma[5]>ma[10]>ma[20] and d['ma20_b']<0.08:
                        res_type="三線多排"
                    elif max(mv[:2])/min(mv[:2])<1.06 and price>ma[5]>ma[10] and d['ma10_b']<0.15:
                        res_type="二線多排"

                    if res_type:
                        all_found.append({
                            "股票代號": code, "價格": price, "漲幅%": RK_p,
                            "成交量": vol, "型態": res_type, "更新時間": now_taipei().strftime("%H:%M:%S")
                        })
                except: continue
        except: continue
        pbar.progress(min((i + batch_size) / total, 1.0))
    
    status_text.empty()
    pbar.empty()
    return pd.DataFrame(all_found)

# =============================
# 4. Streamlit 執行與 UI 顯示
# =============================

# 1. 啟動秒級自動刷新 (取代舊版 experimental_rerun)
st_autorefresh(interval=1000, key="global_refresh")

# 2. 初始化 Session State
if "df_results" not in st.session_state: st.session_state.df_results = pd.DataFrame()
if "last_update" not in st.session_state: st.session_state.last_update = "尚未執行"
if "last_run_min" not in st.session_state: st.session_state.last_run_min = ""
if "seen_keys" not in st.session_state: st.session_state.seen_keys = set()

# 3. 讀取清單與初始化紀錄
os.makedirs("db/results", exist_ok=True)
json_path = os.path.join('db','taiwan_full.json')
try:
    with open(json_path,'r',encoding='utf-8') as f:
        stock_list = json.load(f)['stocks']
except:
    stock_list=["2330.TW","2303.TW","2454.TW"]

# 4. 排程觸發檢查 (在 UI 渲染前先處理數據)
curr_min = now_taipei().strftime("%H:%M")
if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    new_results = run_scan_logic(stock_list)
    
    if not new_results.empty:
        new_results["key"] = new_results["股票代號"] + "_" + new_results["型態"]
        filtered = new_results[~new_results["key"].isin(st.session_state.seen_keys)]
        st.session_state.seen_keys.update(new_results["key"].tolist())
        st.session_state.df_results = filtered.drop(columns=["key"])
        st.session_state.df_results.to_csv("db/results/latest_new_signals.csv", index=False, encoding="utf-8-sig")
    else:
        st.session_state.df_results = pd.DataFrame()
    
    st.session_state.last_update = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
    st.rerun() # 掃描完立刻刷新以同步 UI

# 5. UI 介面繪製
st.title("🚀 台股多頭排列定時掃描器")

with st.container():
    c1, c2, c3 = st.columns(3)
    c1.metric("⏰ 系統目前時間", now_taipei().strftime("%H:%M:%S"))
    c2.metric("📡 最後掃描完成", st.session_state.last_update)
    c3.metric("📊 監控總檔數", len(stock_list))

st.divider()

with st.sidebar:
    st.header("⚙️ 監控參數")
    st.info(f"當前監控: {len(stock_list)} 檔")
    st.write("預定排程:", SCHEDULE_TIMES)
    if st.button("🔄 重置新訊號紀錄"):
        st.session_state.seen_keys = set()
        st.session_state.df_results = pd.DataFrame()
        st.rerun()

st.subheader("📊 掃描結果清單")
st.caption("只顯示本輪新出現訊號")

if not st.session_state.df_results.empty:
    tab1, tab2 = st.tabs(["📋 所有結果", "🔍 依型態篩選"])
    with tab1:
        st.dataframe(st.session_state.df_results, use_container_width=True)
    with tab2:
        types = st.session_state.df_results['型態'].unique()
        selected_type = st.selectbox("選擇顯示型態", types)
        st.table(st.session_state.df_results[st.session_state.df_results['型態'] == selected_type])
else:
    st.info("⌛ 目前尚無新出現的符合條件股票，等待排程觸發中。")
