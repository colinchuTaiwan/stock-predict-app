import streamlit as st
import yfinance as yf
import pandas as pd
import json
import os
import base64
import requests
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# =============================
# 0. 台北時間與自動刷新
# =============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

# 每秒刷新，確保時間 metric 跳動，並檢查排程觸發
st_autorefresh(interval=1000, key="sec_refresh")

# 設定掃描排程時間 (24小時制)
SCHEDULE_TIMES = ["09:30", "11:00", "13:10", "15:00", "20:00"]

# =============================
# 1. 頁面配置與 GitHub 雲端儲存
# =============================
st.set_page_config(page_title="台股多頭排列定時掃描", layout="wide")

GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")
CACHE_PATH = "scan_cache.json"

def upload_to_github(content_dict):
    if not GITHUB_TOKEN or not GITHUB_REPO: return
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.get(url, headers=headers)
        sha = res.json()["sha"] if res.status_code == 200 else None
        content_str = json.dumps(content_dict, ensure_ascii=False, indent=2)
        content_b64 = base64.b64encode(content_str.encode()).decode()
        data = {"message": f"🤖 自動更新: {now_taipei().strftime('%Y-%m-%d %H:%M')}", "content": content_b64, "branch": "main"}
        if sha: data["sha"] = sha
        requests.put(url, headers=headers, json=data)
    except Exception as e:
        print(f"GitHub Sync Error: {e}")

def load_cache():
    # 優先從 GitHub 讀取最新狀態 (避免多台電腦不同步)
    if GITHUB_TOKEN and GITHUB_REPO:
        try:
            url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
            headers = {"Authorization": f"token {GITHUB_TOKEN}"}
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                content = base64.b64decode(res.json()["content"]).decode('utf-8')
                data = json.loads(content)
                return pd.DataFrame(data.get("data", [])), data.get("last_update", "尚未執行"), data.get("last_run_min", "")
        except: pass
    
    # 備援：從本地讀取
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return pd.DataFrame(data.get("data", [])), data.get("last_update", "尚未執行"), data.get("last_run_min", "")
        except: pass
    return pd.DataFrame(), "尚未執行", ""

def save_cache(df, update_time, run_min):
    data = {
        "data": df.to_dict(orient="records"),
        "last_update": update_time,
        "last_run_min": run_min
    }
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    upload_to_github(data)

# =============================
# 2. 掃描與運算邏輯 (保持原核心邏輯)
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
    df["pre_close"] = df['Close'].shift(1)
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    df["mv20"] = df['Volume'].rolling(20).mean() / 1000
    df["RK_p"] = (close - df['Open']) * 100 / df['Open']
    return df

def run_scan_logic(stock_codes, status_placeholder):
    all_found = []
    batch_size = 100
    chunks = [stock_codes[i:i + batch_size] for i in range(0, len(stock_codes), batch_size)]
    pbar = st.progress(0)
    for i, chunk in enumerate(chunks):
        status_placeholder.warning(f"🚀 排程觸發：正下載 {i+1}/{len(chunks)} 批次...")
        raw = yf.download(tickers=chunk, period="300d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
        for code in chunk:
            try:
                df = raw[code].dropna() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna()
                if len(df) < 200: continue
                ind = calc_indicators(df)
                d = ind.iloc[-1].to_dict()
                d.update({'price': df['Close'].iloc[-1], 'pre_close': df['Close'].iloc[-2], 'pre_high': df['High'].iloc[-2], 'vol': df['Volume'].iloc[-1]/1000, 'pre_vol': df['Volume'].iloc[-2]/1000})
                
                # 多頭排列判斷
                ma = {w: d[f'ma{w}'] for w in [5,10,20,60,100,200]}
                ma_d = {w: d[f'ma{w}_d'] for w in [5,10,20,60,100,200]}
                price, rk_p, vol = d['price'], d['RK_p'], d['vol']
                
                if (1 < rk_p < 7) and (price > d['pre_high']) and (d['mv20'] > 100) and (vol > 100) and \
                   (price > ma[20] > ma[60] > ma[100] > ma[200]) and all(v > 0 for v in ma_d.values()) and (vol > d['pre_vol'] * 1.5):
                    
                    res_type = ""
                    mv = list(ma.values())
                    if (max(mv)/min(mv) < 1.06) and (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100] > ma[200]): res_type = "六線多排"
                    elif (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100]): res_type = "五線多排"
                    elif (price > ma[5] > ma[10] > ma[20] > ma[60]): res_type = "四線多排"
                    elif (price > ma[5] > ma[10] > ma[20]): res_type = "三線多排"
                    
                    if res_type:
                        all_found.append({"股票代號": code, "價格": round(price,2), "漲幅%": round(rk_p,1), "成交量": int(vol), "型態": res_type, "掃描時間": now_taipei().strftime("%H:%M")})
            except: continue
        pbar.progress((i+1)/len(chunks))
    status_placeholder.empty()
    pbar.empty()
    return pd.DataFrame(all_found)

# =============================
# 3. UI 介面與排程執行
# =============================
st.title("📈 台股多頭排列雲端監控")

# 初始化：載入快取
if "df_results" not in st.session_state:
    df_cache, last_update, last_run_min = load_cache()
    st.session_state.df_results = df_cache
    st.session_state.last_update = last_update
    st.session_state.last_run_min = last_run_min

# 頂部儀表板
c1, c2, c3 = st.columns(3)
c1.metric("台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("最後掃描完成", st.session_state.last_update)

# 讀取股票清單
try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        data = json.load(f)
        stock_list = data["stocks"] if isinstance(data, dict) else data
except: stock_list = ["2330.TW"]
c3.metric("監控檔數", len(stock_list))

# --- 自動排程邏輯 ---
current_hm = now_taipei().strftime("%H:%M")
status_container = st.empty()

# 如果現在是排程時間且「這分鐘還沒跑過」
if current_hm in SCHEDULE_TIMES and st.session_state.last_run_min != current_hm:
    st.session_state.last_run_min = current_hm
    with status_container:
        new_df = run_scan_logic(stock_list, status_container)
        now_str = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.df_results = new_df
        st.session_state.last_update = now_str
        save_cache(new_df, now_str, current_hm)
    st.rerun()

# --- 手動按鈕 ---
if st.button("🔄 立即重新手動掃描"):
    new_df = run_scan_logic(stock_list, status_container)
    now_str = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.df_results = new_df
    st.session_state.last_update = now_str
    save_cache(new_df, now_str, current_hm)
    st.rerun()

# --- 顯示結果 ---
st.subheader("📋 最新篩選結果")
if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 尚未到排程掃描時間，或目前的市場條件無符合股票。")

with st.sidebar:
    st.header("⚙️ 系統狀態")
    st.write(f"預計掃描時間: `{', '.join(SCHEDULE_TIMES)}`")
    if GITHUB_TOKEN: st.success("☁️ 雲端同步已開啟")
    else: st.warning("💾 僅使用本地存檔")
