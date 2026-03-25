import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 時區與秒級刷新
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

# 每秒刷新一次前端，確保排程檢查精準
st_autorefresh(interval=1000, key="sec_refresh")

# ==============================
# 1. GitHub 雲端核心邏輯
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
CACHE_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")
CACHE_PATH = "scan_cache.json"

def upload_to_github(content_dict, path=CACHE_FILE):
    """通用 GitHub 上傳函數，支援路徑自動切換"""
    if not GITHUB_TOKEN or not GITHUB_REPO: return
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        
        # 取得舊檔案 SHA (如果有)
        res = requests.get(url, headers=headers, timeout=5)
        sha = res.json().get("sha") if res.status_code == 200 else None
        
        content_json = json.dumps(content_dict, ensure_ascii=False, indent=2)
        content_b64 = base64.b64encode(content_json.encode('utf-8')).decode('utf-8')
        
        data = {
            "message": f"🤖 Auto-update: {now_taipei().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch": "main"
        }
        if sha: data["sha"] = sha
        
        put_res = requests.put(url, headers=headers, json=data, timeout=10)
        return put_res.status_code in [200, 201]
    except Exception as e:
        st.sidebar.error(f"GitHub 上傳失敗 ({path}): {e}")
        return False

def load_cache_from_github():
    """從 GitHub 讀取最新掃描快取"""
    if not GITHUB_TOKEN or not GITHUB_REPO: return pd.DataFrame(), "未知"
    try:
        url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/{CACHE_FILE}"
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            data = res.json()
            return pd.DataFrame(data.get("data", [])), data.get("last_update", "尚未更新")
    except: pass
    return pd.DataFrame(), "讀取失敗"

# ==============================
# 2. 技術指標與掃描引擎
# ==============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5,10,20,60,100,200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"])/df[f"ma{w}"]
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    df["mv20"] = df['Volume'].rolling(20).mean()/1000
    df["RK_p"] = (close - df['Open'])*100/df['Open']
    return df

def run_scan_logic(stock_codes, status_placeholder):
    all_found = []
    batch_size = 50
    pbar = st.progress(0)
    
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i+batch_size]
        status_placeholder.info(f"⏳ 正在掃描批次: {i//batch_size + 1}")
        try:
            raw = yf.download(tickers=batch, period="300d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
            for code in batch:
                try:
                    df = raw[code].dropna() if len(batch) > 1 else raw.dropna()
                    if len(df) < 200: continue
                    ind = calc_indicators(df)
                    last = ind.iloc[-1]
                    pre = ind.iloc[-2]

                    price, rk_p, vol = round(last['Close'], 2), round(last['RK_p'], 1), int(last['Volume']/1000)
                    ma = {w: last[f'ma{w}'] for w in [5,10,20,60,100,200]}
                    ma_d = {w: last[f'ma{w}_d'] for w in [5,10,20,60,100,200]}

                    # 篩選邏輯
                    if 1 < rk_p < 7 and price > last['pre_high'] and last['mv20'] > 100 and vol > 100:
                        if price > ma[20] and price > ma[60] and all(v > 0 for v in ma_d.values()):
                            res_type = ""
                            mv = [ma[w] for w in [5,10,20,60,100,200] if ma[w] > 0]
                            spread = max(mv)/min(mv) if mv else 999
                            
                            if spread < 1.06 and price > ma[5] > ma[10] > ma[20] > ma[60]: res_type = "多頭排列(強)"
                            elif price > ma[5] > ma[10] > ma[20]: res_type = "三線多排"
                            
                            if res_type:
                                all_found.append({
                                    "股票代號": code, "價格": price, "漲幅%": rk_p, 
                                    "成交量": vol, "型態": res_type, "時間": now_taipei().strftime("%H:%M:%S")
                                })
                except: continue
        except: continue
        pbar.progress(min((i+batch_size)/len(stock_codes), 1.0))
    
    status_placeholder.empty()
    pbar.empty()
    return pd.DataFrame(all_found)

# ==============================
# 3. 排程設定與邏輯控制
# ==============================
SCHEDULE_TIMES = ["09:30", "10:30", "11:20", "12:20", "13:15", "14:24", "20:00", "23:00"]

# 載入代碼
try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        stock_list = json.load(f)['stocks']
except:
    stock_list = ["2330.TW", "2454.TW", "2317.TW"]

if "df_results" not in st.session_state: st.session_state.df_results = pd.DataFrame()
if "last_update" not in st.session_state: st.session_state.last_update = "尚未掃描"
if "last_run_min" not in st.session_state: st.session_state.last_run_min = ""

# 自動掃描觸發
curr_min = now_taipei().strftime("%H:%M")
if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    status_box = st.empty()
    
    # 優先嘗試從雲端同步（避免多個用戶開啟網頁導致重複掃描）
    df_cloud, time_cloud = load_cache_from_github()
    if time_cloud.startswith(now_taipei().strftime("%Y-%m-%d")):
        st.session_state.df_results = df_cloud
        st.session_state.last_update = time_cloud
        status_box.success("☁️ 已從 GitHub 同步今日最新數據")
    else:
        # 雲端沒資料才掃描
        new_res = run_scan_logic(stock_list, status_box)
        st.session_state.df_results = new_res
        st.session_state.last_update = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
        
        # 存檔至 GitHub (快取 + 歷史)
        cache_data = {"data": new_res.to_dict(orient="records"), "last_update": st.session_state.last_update}
        upload_to_github(cache_data)
        upload_to_github(cache_data, path=f"history/{now_taipei().strftime('%Y-%m-%d')}.json")
    
    time.sleep(2)
    status_box.empty()
    st.rerun()

# ==============================
# 4. 前端介面
# ==============================
st.title("📊 台股多頭排列融合掃描器 v3")

c1, c2, c3 = st.columns(3)
c1.metric("⏰ 台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("📡 最後更新", st.session_state.last_update)
c3.metric("📈 監控數量", len(stock_list))

st.divider()

if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 目前尚無訊號。系統將在排程時間自動執行掃描。")

with st.sidebar:
    st.header("⚙️ 控制面板")
    if st.button("☁️ 手動同步雲端"):
        df_cloud, time_cloud = load_cache_from_github()
        st.session_state.df_results = df_cloud
        st.session_state.last_update = time_cloud
        st.rerun()
    
    st.write("📅 排程時間:", SCHEDULE_TIMES)
    st.caption("v3 版：穩定同步與自動避錯機制")
