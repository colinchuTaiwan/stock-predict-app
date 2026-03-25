import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta, timezone
import glob
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
    for w in [5, 10, 20, 60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)
    
    df["pre_close"] = df['Close'].shift(1)
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    
    df["mv5"] = df['Volume'].rolling(5).mean() / 1000
    df["mv20"] = df['Volume'].rolling(20).mean() / 1000
    
    for w in [10, 20, 60, 100, 200]:
        df[f"ma{w}_b"] = (close - df[f"ma{w}"]) / df[f"ma{w}"]
        
    df["RK_p"] = (close - df['Open']) * 100 / df['Open']
    return df

def extract_latest(df, ind):
    if len(df) < 3: return None
    last_3_days = df.tail(3)
    d = {
        'price':     last_3_days['Close'].iloc[-1],
        'open':      last_3_days['Open'].iloc[-1],
        'high':      last_3_days['High'].iloc[-1],
        'vol':       last_3_days['Volume'].iloc[-1] / 1000,
        'pre_close': last_3_days['Close'].iloc[-2],
        'pre_high':  last_3_days['High'].iloc[-2],
        'pre_vol':   last_3_days['Volume'].iloc[-2] / 1000,
    }
    if not ind.empty:
        d.update(ind.iloc[-1].to_dict())
    return d

# =============================
# 3. 掃描核心
# =============================

def run_scan_logic(stock_codes, batch_size=50):
    """
    分批下載股票數據並進行多頭排列掃描
    :param stock_codes: 完整的股票代碼列表 (如 1700 檔)
    :param batch_size: 每批次下載的數量，建議 50-100 檔
    """
    all_found = []
    total_stocks = len(stock_codes)
    
    # 在 Streamlit UI 建立進度條與狀態文字
    st.write(f"🔍 開始掃描全台股任務 (總計 {total_stocks} 檔)...")
    pbar = st.progress(0)
    status_text = st.empty()

    # 開始分批 (Batching)
    for i in range(0, total_stocks, batch_size):
        batch = stock_codes[i : i + batch_size]
        current_progress = min((i + batch_size) / total_stocks, 1.0)
        
        status_text.text(f"正在處理第 {i} ~ {min(i + batch_size, total_stocks)} 檔...")
        
        try:
            # 1. 批量下載這一組數據
            # threads=True 加速，period="300d" 確保有足夠數據算 MA200
            raw = yf.download(
                tickers=batch, 
                period="300d", 
                group_by="ticker", 
                auto_adjust=False, 
                threads=True,
                progress=False  # 關閉 yf 自帶進度條避免干擾 Streamlit
            )

            if raw.empty:
                continue

            # 2. 遍歷這一組中的每一檔股票
            for code in batch:
                try:
                    # 判斷 MultiIndex 結構 (單檔與多檔下載結構不同)
                    if len(batch) > 1:
                        df = raw[code].dropna()
                    else:
                        df = raw.dropna()

                    if len(df) < 200: continue # 數據不足跳過

                    # 3. 計算指標 (沿用你原本的函數)
                    ind = calc_indicators(df)
                    d = extract_latest(df, ind)
                    if not d: continue

                    # --- 判斷邏輯開始 ---
                    price = round(d['price'], 2)
                    pre_high = round(d['pre_high'], 2)
                    RK_p = round(d['RK_p'], 1)
                    stock_cap = int(d['vol'])
                    prev_cap = d['pre_vol']
                    mv20 = d['mv20']
                    
                    ma = {w: d[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
                    ma_d = {w: d[f'ma{w}_d'] for w in [5, 10, 20, 60, 100, 200]}
                    pre_ma = {w: ind[f'ma{w}'].iloc[-2] for w in [5, 10, 20, 60]}

                    # 漲幅過濾 (1%~7%)
                    if not (1 < RK_p < 7): continue
                    
                    # 基本多頭排列與放量條件
                    cond_basic = (
                        price > pre_high and mv20 > 100 and stock_cap > 100 and
                        price > ma[20] and price > ma[60] and price > ma[100] and price > ma[200] and
                        all(v > 0 for v in ma_d.values()) and
                        stock_cap > prev_cap * 1.5
                    )
                    
                    if not cond_basic: continue

                    # 判斷是否為「剛突破」
                    is_breakout = any(df['Close'].iloc[-2] < pre_ma[w] for w in [5, 10, 20, 60])
                    if not is_breakout: continue

                    # 分類多排型態
                    res_type = ""
                    if (max(ma.values())/min(ma.values()) < 1.06) and (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100] > ma[200]):
                        res_type = "六線多排"
                    elif (max(list(ma.values())[:-1])/min(list(ma.values())[:-1]) < 1.06) and (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100]):
                        res_type = "五線多排"
                    elif (max(list(ma.values())[:4])/min(list(ma.values())[:4]) < 1.06) and (price > ma[5] > ma[10] > ma[20] > ma[60]):
                        res_type = "四線多排"
                    elif (max(list(ma.values())[:3])/min(list(ma.values())[:3]) < 1.06) and (price > ma[5] > ma[10] > ma[20]) and (d['ma20_b'] < 0.08):
                        res_type = "三線多排"
                    elif (max(list(ma.values())[:2])/min(list(ma.values())[:2]) < 1.06) and (price > ma[5] > ma[10]) and (d['ma10_b'] < 0.15):
                        res_type = "二線多排"      
                        
                    if res_type:
                        all_found.append({
                            "股票代號": code,
                            "價格": price,
                            "漲幅%": RK_p,
                            "成交量": stock_cap,
                            "型態": res_type,
                            "更新時間": now_taipei().strftime("%H:%M:%S")
                        })
                except Exception:
                    continue # 單檔錯誤不影響整批

        except Exception as e:
            st.warning(f"批次下載失敗 (索引 {i}): {e}")
        
        # 更新總進度條
        pbar.progress(current_progress)

    status_text.text("✅ 掃描完成！")
    return pd.DataFrame(all_found)
# =============================
# 4. Streamlit UI
# =============================

st_autorefresh(interval=1000, key="sec_refresh") # 每 1000ms 刷新一次

# 建立固定容器
time_placeholder = st.empty()
status_placeholder = st.empty()

with time_placeholder.container():
    c1, c2, c3 = st.columns(3)
    c1.metric("系統目前時間", now_taipei().strftime("%H:%M:%S"))
    c2.metric("最後掃描時間", st.session_state.last_update)
    c3.metric("監控總檔數", len(stock_list))

# 讀取最後一次存檔，初始化 seen_keys
os.makedirs("db/results", exist_ok=True)
last_files = sorted(glob.glob("db/results/latest_new_signals*.csv"))
if last_files:
    df_last = pd.read_csv(last_files[-1], encoding="utf-8-sig")
    st.session_state.seen_keys.update(df_last["股票代號"] + "_" + df_last["型態"])

# 狀態顯示欄
c1, c2 = st.columns(2)
c1_time = c1.empty()
c2_time = c2.empty()
c1_time.metric("系統目前時間", now_taipei().strftime("%H:%M:%S"))
c2_time.metric("最後資料更新時間", st.session_state.last_update)

# 讀取股號
json_path = os.path.join('db', 'taiwan_full.json')
try:
    with open(json_path, 'r', encoding='utf-8') as f:
        stock_list = json.load(f)['stocks']
except:
    stock_list = ["2330.TW", "2303.TW", "2454.TW"]

# 側邊欄控制
with st.sidebar:
    st.header("監控參數")
    st.write(f"當前監控檔數: {len(stock_list)}")
    st.write("預定排程:", SCHEDULE_TIMES)
    if st.button("🔄 重置新訊號"):
        st.session_state.seen_keys = set()

# 排程觸發邏輯

curr_min = now_taipei().strftime("%H:%M")
should_trigger = (curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min)

if should_trigger:
    # 先更新標記，避免重複觸發
    st.session_state.last_run_min = curr_min
    
    with status_placeholder:
        with st.spinner(f"正在執行 {curr_min} 排程大規模掃描，秒數顯示將暫時暫停..."):
            # 這裡執行耗時任務
            new_results = run_scan_logic(stock_list)
            
            if not new_results.empty:
                # ... (處理資料與儲存 CSV 的邏輯) ...
                st.session_state.df_results = filtered.drop(columns=["key"])
                st.session_state.last_update = now_taipei().strftime("%H:%M:%S")
    
    st.rerun() # 掃描完強制刷新一次，恢復秒數跳動

# 顯示結果表格
st.subheader("📊 掃描結果清單")
st.caption("只顯示本輪新出現訊號")

if not st.session_state.df_results.empty:
    tab1, tab2 = st.tabs(["所有結果", "依型態篩選"])
    with tab1:
        st.dataframe(st.session_state.df_results, use_container_width=True)
    with tab2:
        selected_type = st.selectbox(
            "選擇型態",
            st.session_state.df_results['型態'].unique()
        )
        st.table(
            st.session_state.df_results[
                st.session_state.df_results['型態'] == selected_type
            ]
        )
else:
    st.info("目前沒有『新出現』的多頭排列股票")
