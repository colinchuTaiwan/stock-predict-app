import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta, timezone
import glob

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
    for w in [5,10,20,60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)

    df["pre_close"] = df['Close'].shift(1)
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)

    df["mv5"] = df['Volume'].rolling(5).mean()/1000
    df["mv20"] = df['Volume'].rolling(20).mean()/1000

    for w in [10,20,60,100,200]:
        df[f"ma{w}_b"] = (close - df[f"ma{w}"])/df[f"ma{w}"]

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
# 3. 掃描核心函數
# =============================
def run_scan_logic(stock_codes):
    st.write(f"正在下載 {len(stock_codes)} 檔股票數據...")
    raw = yf.download(tickers=stock_codes, period="300d", group_by="ticker", auto_adjust=False, threads=True)
    
    all_found = []
    pbar = st.progress(0)

    for i, code in enumerate(stock_codes):
        pbar.progress((i+1)/len(stock_codes))
        try:
            df = raw[code].dropna() if len(stock_codes)>1 else raw.dropna()
            if len(df)<200: continue
            ind = calc_indicators(df)
            d = extract_latest(df, ind)
            if not d: continue

            price = round(d['price'],2)
            RK_p = round(d['RK_p'],1)
            stock_cap = int(d['vol'])
            prev_cap = d['pre_vol']
            mv20 = d['mv20']

            ma = {w:d[f'ma{w}'] for w in [5,10,20,60,100,200]}
            ma_d = {w:d[f'ma{w}_d'] for w in [5,10,20,60,100,200]}
            pre_ma = {w:ind[f'ma{w}'].iloc[-2] for w in [5,10,20,60]}

            if not (1<RK_p<7): continue

            cond_basic = (
                price> d['pre_high'] and mv20>100 and stock_cap>100 and
                price>ma[20] and price>ma[60] and price>ma[100] and price>ma[200] and
                all(v>0 for v in ma_d.values()) and stock_cap>prev_cap*1.5
            )
            if not cond_basic: continue

            is_breakout = any(d['pre_close']<pre_ma[w] for w in [5,10,20,60])
            if not is_breakout: continue

            # 型態判斷
            res_type=""
            if max(ma.values())/min(ma.values())<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60]>ma[100]>ma[200] and d['ma200_b']<0.08:
                res_type="六線多排"
            elif max(list(ma.values())[:-1])/min(list(ma.values())[:-1])<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60]>ma[100] and d['ma100_b']<0.08:
                res_type="五線多排"
            elif max(list(ma.values())[:4])/min(list(ma.values())[:4])<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60] and d['ma60_b']<0.08:
                res_type="四線多排"
            elif max(list(ma.values())[:3])/min(list(ma.values())[:3])<1.06 and price>ma[5]>ma[10]>ma[20] and d['ma20_b']<0.08:
                res_type="三線多排"
            elif max(list(ma.values())[:2])/min(list(ma.values())[:2])<1.06 and price>ma[5]>ma[10] and d['ma10_b']<0.15:
                res_type="二線多排"

            if res_type:
                all_found.append({
                    "股票代號": code,
                    "價格": price,
                    "漲幅%": RK_p,
                    "成交量": stock_cap,
                    "型態": res_type,
                    "更新時間": now_taipei().strftime("%H:%M:%S")
                })
        except:
            continue
    return pd.DataFrame(all_found)

# =============================
# 4. Streamlit UI
# =============================
st.title("🚀 台股多頭排列定時掃描器")

# 初始化 Session State
if "df_results" not in st.session_state: st.session_state.df_results=pd.DataFrame()
if "last_update" not in st.session_state: st.session_state.last_update="尚未執行"
if "last_run_min" not in st.session_state: st.session_state.last_run_min=""
if "seen_keys" not in st.session_state: st.session_state.seen_keys=set()

# 初始化 seen_keys 從最後存檔讀取
os.makedirs("db/results", exist_ok=True)
last_files = sorted(glob.glob("db/results/latest_new_signals*.csv"))
if last_files:
    df_last = pd.read_csv(last_files[-1], encoding="utf-8-sig")
    st.session_state.seen_keys.update(df_last["股票代號"]+"_"+df_last["型態"])

# 顯示系統時間與最後更新時間
c1,c2 = st.columns(2)
c1.metric("系統目前時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("最後資料更新時間", st.session_state.last_update)

# 讀取股票清單
json_path = os.path.join('db','taiwan_full.json')
try:
    with open(json_path,'r',encoding='utf-8') as f:
        stock_list = json.load(f)['stocks']
except:
    stock_list=["2330.TW","2303.TW","2454.TW"]

# 側邊欄控制
with st.sidebar:
    st.header("監控參數")
    st.write(f"當前監控檔數: {len(stock_list)}")
    st.write("預定排程:", SCHEDULE_TIMES)
    if st.button("🔄 重置新訊號"):
        st.session_state.seen_keys=set()

# 自動觸發檢查
curr_min = now_taipei().strftime("%H:%M")
should_trigger = (curr_min in SCHEDULE_TIMES and st.session_state.last_run_min!=curr_min)
if should_trigger:
    st.session_state.last_run_min = curr_min
    new_results = run_scan_logic(stock_list)
    if not new_results.empty:
        new_results["key"]=new_results["股票代號"]+"_"+new_results["型態"]
        filtered = new_results[~new_results["key"].isin(st.session_state.seen_keys)]
        st.session_state.seen_keys.update(new_results["key"].tolist())
        st.session_state.df_results = filtered.drop(columns=["key"])
        # 存檔
        st.session_state.df_results.to_csv("db/results/latest_new_signals.csv", index=False, encoding="utf-8-sig")
    else:
        st.session_state.df_results = new_results
    st.session_state.last_update = now_taipei().strftime("%Y-%m-%d %H:%M:%S")

# 顯示結果
st.subheader("📊 掃描結果清單")
st.caption("只顯示本輪新出現訊號")
if not st.session_state.df_results.empty:
    tab1,tab2=st.tabs(["所有結果","依型態篩選"])
    with tab1:
        st.dataframe(st.session_state.df_results,use_container_width=True)
    with tab2:
        selected_type = st.selectbox("選擇型態",st.session_state.df_results['型態'].unique())
        st.table(st.session_state.df_results[st.session_state.df_results['型態']==selected_type])
else:
    st.info("目前沒有『新出現』的多頭排列股票")

# ----------------------------
# 非阻塞自動刷新
# ----------------------------
st.experimental_rerun(interval=60_000)  # 每 60 秒刷新一次
