import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 時區與刷新設定
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

# 每秒刷新一次確保檢查時間點精準
st_autorefresh(interval=1000, key="sec_refresh")

# ==============================
# 1. GitHub 雲端存取邏輯
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
CACHE_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")

def upload_to_github(content_dict, path=CACHE_FILE):
    if not GITHUB_TOKEN or not GITHUB_REPO: return False
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers, timeout=5)
        sha = res.json().get("sha") if res.status_code == 200 else None
        content_json = json.dumps(content_dict, ensure_ascii=False, indent=2)
        content_b64 = base64.b64encode(content_json.encode('utf-8')).decode('utf-8')
        data = {"message": f"🤖 Auto-update: {now_taipei().strftime('%Y-%m-%d %H:%M')}", "content": content_b64, "branch": "main"}
        if sha: data["sha"] = sha
        requests.put(url, headers=headers, json=data, timeout=10)
        return True
    except: return False

def load_cache_from_github():
    if not GITHUB_TOKEN or not GITHUB_REPO: return pd.DataFrame(), "未知"
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{CACHE_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200: return pd.DataFrame(), "讀取失敗"
        data = json.loads(base64.b64decode(res.json().get("content", "").replace("\n", "")).decode('utf-8'))
        return pd.DataFrame(data.get("data", [])), data.get("last_update", "尚未更新")
    except: return pd.DataFrame(), "讀取失敗"

# ==============================
# 2. 技術指標與掃描引擎
# ==============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5,10,20,60,100,200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"])/df[f"ma{w}"]
    df["RK_p"] = (close - df['Open'])*100/df['Open']
    return df

def run_scan_logic(stock_codes, status_placeholder):
    st.sidebar.write("run_scan_logic")
    st.write("run_scan_logic")
    found_in_chunk = []
    try:
        # 下載這 10 檔的資料
        raw = yf.download(tickers=stock_codes, period="300d", group_by="ticker", 
                          auto_adjust=False, threads=True, progress=False)
        
        for code in stock_codes:
            st.sidebar.write({code})
            st.write({code})
            try:
                df = raw[code].copy().dropna() if len(stock_codes) > 1 else raw.copy().dropna()
                if len(df) < 200: continue
                
                ind = calc_indicators(df)
                last = ind.iloc[-1]
                price, rk_p, vol = round(last['Close'], 2), round(last['RK_p'], 1), int(last['Volume']/1000)
                ma = {w: last[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
                ma_b = {w: last.get(f'ma{w}_b', 0) for w in [20, 60, 100]}

                # 基礎條件：漲幅 1~7%、站上長線、有量
                if (1 < rk_p < 7 and price > max(ma[20], ma[60], ma[100], ma[200]) and vol > 100):
                    res_type = ""
                    ma_list = [ma[5], ma[10], ma[20], ma[60], ma[100]]
                    
                    if (max(ma_list) / min(ma_list) < 1.08) and ma_b[100] < 0.1:
                        res_type = "五線糾結"
                    elif (max(ma_list[:4]) / min(ma_list[:4]) < 1.08) and ma_b[60] < 0.1:
                        res_type = "四線糾結"
                    elif (max(ma_list[:3]) / min(ma_list[:3]) < 1.08) and ma_b[20] < 0.1:
                        res_type = "三線糾結"

                    if res_type:
                        found_in_chunk.append({
                            "股票代號": code, "價格": price, "漲幅%": rk_p, 
                            "型態": res_type, "掃描時間": now_taipei().strftime("%H:%M:%S")
                        })
            except: continue
    except Exception as e:
        st.error(f"批次下載失敗: {e}")
    return found_in_chunk

# ==============================
# 3. 排程與分批執行邏輯
# ==============================
SCHEDULE_TIMES = ["09:30", "10:30", "11:20", "12:20", "13:15", "14:30", "19:00", "23:00"]

# 載入股票名單
try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        stock_list = json.load(f)['stocks']
except:
    stock_list = ["2330.TW", "2454.TW"]

# 初始化 Session State
if "df_results" not in st.session_state: st.session_state.df_results = pd.DataFrame()
if "last_update" not in st.session_state: st.session_state.last_update = "尚未掃描"
if "last_run_min" not in st.session_state: st.session_state.last_run_min = ""

curr_min = now_taipei().strftime("%H:%M")

# --- 自動掃描觸發區 ---
if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    
    # 1. 嘗試從 GitHub 同步今日已完成的結果
    df_cloud, time_cloud = load_cache_from_github()
    if time_cloud.startswith(now_taipei().strftime("%Y-%m-%d")):
        st.session_state.df_results = df_cloud
        st.session_state.last_update = time_cloud
    else:
        # 2. 雲端沒資料，開始分批掃描
        st.session_state.df_results = pd.DataFrame() # 清空舊資料
        batch_size = 10
        total_stocks = len(stock_list)
        st.write(stock_list)
        st.sidebar.write(stock_list)
        with st.status(f"🚀 啟動全量掃描 ({total_stocks} 檔)...", expanded=True) as status:
            all_found = []
            for i in range(0, total_stocks, batch_size):
                batch = stock_list[i : i + batch_size]
                status.write(f"正在檢查第 {i+1} ~ {min(i+batch_size, total_stocks)} 檔...")
                st.write(f"正在檢查第 {i+1} ~ {min(i+batch_size, total_stocks)} 檔...")
                st.sidebar.write(f"正在檢查第 {i+1} ~ {min(i+batch_size, total_stocks)} 檔...")
                
                # 執行掃描
                chunk_results = run_scan_logic(batch, status)
                
                if chunk_results:
                    all_found.extend(chunk_results)
                    # 即時更新顯示表格
                    st.session_state.df_results = pd.DataFrame(all_found)
                
            st.session_state.last_update = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
            status.update(label="✅ 掃描完成！", state="complete")
        
        # 3. 掃描結束後上傳 GitHub
        cache_data = {"data": st.session_state.df_results.to_dict(orient="records"), "last_update": st.session_state.last_update}
        upload_to_github(cache_data)
        upload_to_github(cache_data, path=f"history/{now_taipei().strftime('%Y-%m-%d')}.json")
        st.rerun()

# ==============================
# 4. 前端介面
# ==============================
st.title("📊 台股多頭排列融合掃描器 v3.1")
c1, c2, c3 = st.columns(3)
c1.metric("⏰ 台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("📡 最後更新", st.session_state.last_update)
c3.metric("📈 訊號數量", len(st.session_state.df_results))

st.divider()

if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 目前尚無訊號。系統將在排程時間自動執行分批掃描。")

with st.sidebar:
    st.header("⚙️ 控制面板")
    if st.button("☁️ 手動同步雲端", use_container_width=True):
        df_cloud, time_cloud = load_cache_from_github()
        st.session_state.df_results = df_cloud
        st.session_state.last_update = time_cloud
        st.rerun()
    
    st.subheader("📅 排程時間")
    st.code(", ".join(SCHEDULE_TIMES))
    st.caption("分批模式：每 10 檔一組，降低系統負擔")
