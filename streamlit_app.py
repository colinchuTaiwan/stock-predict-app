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
# 1. 頁面配置與初始化
# =============================
st.set_page_config(page_title="台股多頭排列自動掃描", layout="wide")

# 初始化 Session State
if "df_results" not in st.session_state:
    st.session_state.df_results = pd.DataFrame()
if "last_update" not in st.session_state:
    st.session_state.last_update = "尚未執行"
if "last_run_min" not in st.session_state:
    st.session_state.last_run_min = ""
if "seen_keys" not in st.session_state:
    st.session_state.seen_keys = set()

SCHEDULE_TIMES = [
    "07:00", "09:20", "10:30", "11:20", "12:20", 
    "13:20", "15:00", "18:00", "22:30", "23:30"
]

# =============================
# 2. 指標計算與資料處理
# =============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"]) / df[f"ma{w}"]
    for w in [5, 10, 20, 60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)
    
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    df["mv20"] = df['Volume'].rolling(20).mean() / 1000
    df["RK_p"] = (close - df['Open']) * 100 / df['Open']
    return df

def extract_latest(df, ind):
    if len(df) < 3: return None
    last_idx = -1
    pre_idx = -2
    d = {
        'price':     df['Close'].iloc[last_idx],
        'open':      df['Open'].iloc[last_idx],
        'high':      df['High'].iloc[last_idx],
        'vol':       df['Volume'].iloc[last_idx] / 1000,
        'pre_close': df['Close'].iloc[pre_idx],
        'pre_high':  df['High'].iloc[pre_idx],
        'pre_vol':   df['Volume'].iloc[pre_idx] / 1000,
    }
    if not ind.empty:
        d.update(ind.iloc[last_idx].to_dict())
    return d

# =============================
# 3. 掃描核心 (分批下載版)
# =============================
def run_scan_logic(stock_codes, batch_size=50):
    all_found = []
    total_stocks = len(stock_codes)
    
    pbar = st.progress(0)
    status_text = st.empty()

    for i in range(0, total_stocks, batch_size):
        batch = stock_codes[i : i + batch_size]
        current_progress = min((i + batch_size) / total_stocks, 1.0)
        status_text.text(f"⏳ 正在掃描: {i}/{total_stocks} 檔股票...")
        
        try:
            raw = yf.download(tickers=batch, period="300d", group_by="ticker", 
                             auto_adjust=False, threads=True, progress=False)
            if raw.empty: continue

            for code in batch:
                try:
                    df = raw[code].dropna() if len(batch) > 1 else raw.dropna()
                    if len(df) < 200: continue

                    ind = calc_indicators(df)
                    d = extract_latest(df, ind)
                    if not d: continue

                    # 篩選條件
                    price, RK_p, vol = round(d['price'], 2), round(d['RK_p'], 1), int(d['vol'])
                    ma = {w: d[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
                    ma_d = {w: d[f'ma{w}_d'] for w in [5, 10, 20, 60, 100, 200]}
                    
                    if not (1 < RK_p < 7): continue
                    
                    cond_basic = (
                        price > d['pre_high'] and d['mv20'] > 100 and vol > 100 and
                        price > ma[20] and price > ma[60] and price > ma[200] and
                        all(v > 0 for v in ma_d.values()) and vol > d['pre_vol'] * 1.5
                    )
                    if not cond_basic: continue

                    # 型態分類
                    res_type = ""
                    ma_list = list(ma.values())
                    if (max(ma_list)/min(ma_list) < 1.06) and (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100] > ma[200]):
                        res_type = "六線多排"
                    elif (max(ma_list[:-1])/min(ma_list[:-1]) < 1.06) and (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100]):
                        res_type = "五線多排"
                    elif (max(ma_list[:4])/min(ma_list[:4]) < 1.06) and (price > ma[5] > ma[10] > ma[20] > ma[60]):
                        res_type = "四線多排"

                    if res_type:
                        all_found.append({
                            "股票代號": code, "價格": price, "漲幅%": RK_p,
                            "成交量": vol, "型態": res_type, "更新時間": now_taipei().strftime("%H:%M:%S")
                        })
                except: continue
        except: continue
        pbar.progress(current_progress)
        time.sleep(0.5) # 微調避免 API 拒絕

    status_text.empty()
    pbar.empty()
    return pd.DataFrame(all_found)

# =============================
# 4. Streamlit UI 佈局
# =============================
st_autorefresh(interval=1000, key="sec_refresh")

# 載入股票清單
json_path = os.path.join('db', 'taiwan_full.json')
try:
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        stock_list = data['stocks'] if isinstance(data, dict) else data
except:
    stock_list = ["2330.TW", "2303.TW", "2454.TW"]

# 頂部狀態列
st.title("🚀 台股多頭排列定時掃描器")
time_placeholder = st.empty()
with time_placeholder.container():
    c1, c2, c3 = st.columns(3)
    c1.metric("系統目前時間", now_taipei().strftime("%H:%M:%S"))
    c2.metric("最後掃描任務", st.session_state.last_update)
    c3.metric("監控總檔數", len(stock_list))

status_placeholder = st.empty()

# 側邊欄
with st.sidebar:
    st.header("⚙️ 監控設定")
    st.info(f"當前載入: {len(stock_list)} 檔")
    st.write("預定排程:", SCHEDULE_TIMES)
    if st.button("🔄 重置新訊號紀錄"):
        st.session_state.seen_keys = set()
        st.toast("已重置過濾器")

# =============================
# 5. 排程執行邏輯
# =============================
curr_min = now_taipei().strftime("%H:%M")
if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    
    with status_placeholder.container():
        st.warning(f"正在執行 {curr_min} 大規模掃描，請勿關閉視窗...")
        new_df = run_scan_logic(stock_list)
        
        if not new_df.empty:
            new_df["key"] = new_df["股票代號"] + "_" + new_df["型態"]
            filtered = new_df[~new_df["key"].isin(st.session_state.seen_keys)]
            st.session_state.seen_keys.update(filtered["key"].tolist())
            st.session_state.df_results = filtered.drop(columns
