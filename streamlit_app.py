import streamlit as st
import yfinance as yf
import pandas as pd
import json, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 系統環境與狀態初始化
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)

# 每 2 秒強制 UI 刷新一次，驅動排程檢查與掃描步進
st_autorefresh(interval=2000, key="sys_heartbeat")

# 初始化持久化狀態 (Session State)
if "df_results" not in st.session_state: st.session_state.df_results = pd.DataFrame()
if "last_run_key" not in st.session_state: st.session_state.last_run_key = ""
if "is_scanning" not in st.session_state: st.session_state.is_scanning = False
if "scan_idx" not in st.session_state: st.session_state.scan_idx = 0
if "found_list" not in st.session_state: st.session_state.found_list = []

# ==============================
# 1. 核心指標計算 (向量化優化)
# ==============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"]) / df[f"ma{w}"]
    df["RK_p"] = (close - df['Open']) * 100 / df['Open']
    return df

# ==============================
# 2. 健壯型掃描引擎 (防封鎖與 Retry)
# ==============================
def run_batch_logic(codes):
    found = []
    try:
        # 使用 threads=False 降低 Yahoo Finance 封鎖風險
        raw = yf.download(tickers=codes, period="300d", group_by="ticker", 
                          auto_adjust=False, threads=False, progress=False, timeout=15)
        
        for code in codes:
            try:
                df = raw[code].copy().dropna() if len(codes) > 1 else raw.copy().dropna()
                if len(df) < 200: continue
                
                ind = calc_indicators(df)
                last = ind.iloc[-1]
                
                # 提取數值
                p, rk, vol = round(last['Close'], 2), round(last['RK_p'], 1), int(last['Volume']/1000)
                ma = {w: last[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
                ma_b = {w: last.get(f'ma{w}_b', 0) for w in [20, 60, 100]}

                # 多頭排列 + 漲幅過濾
                basic_ok = (1 < rk < 7 and p > max(ma[20], ma[60], ma[100], ma[200]) and vol > 100)
                
                if basic_ok:
                    ma_list = [ma[5], ma[10], ma[20], ma[60], ma[100]]
                    res_type = ""
                    if (max(ma_list) / min(ma_list) < 1.08) and ma_b[100] < 0.1: res_type = "五線糾結"
                    elif (max(ma_list[:4]) / min(ma_list[:4]) < 1.08) and ma_b[60] < 0.1: res_type = "四線糾結"
                    elif (max(ma_list[:3]) / min(ma_list[:3]) < 1.08) and ma_b[20] < 0.1: res_type = "三線糾結"
                    
                    if res_type:
                        found.append({"股票代號": code, "價格": p, "漲幅%": rk, "型態": res_type, "時間": now_taipei().strftime("%H:%M:%S")})
            except: continue
    except Exception as e:
        st.error(f"⚠️ API 請求異常: {e}")
    return found

# ==============================
# 3. GitHub 寫入與讀取 (帶 Error Handling)
# ==============================
def sync_github(data_dict, mode="upload"):
    try:
        repo = st.secrets["GITHUB_REPO"]
        token = st.secrets["GITHUB_TOKEN"]
        path = st.secrets.get("GITHUB_FILE", "scan_cache.json")
        url = f"https://api.github.com/repos/{repo}/contents/{path}"
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

        if mode == "upload":
            res = requests.get(url, headers=headers, timeout=5)
            sha = res.json().get("sha") if res.status_code == 200 else None
            content_b64 = base64.b64encode(json.dumps(data_dict, ensure_ascii=False, indent=2).encode('utf-8')).decode('utf-8')
            payload = {"message": "🤖 Auto-scan", "content": content_b64, "branch": "main"}
            if sha: payload["sha"] = sha
            requests.put(url, headers=headers, json=payload, timeout=10)
        else: # download
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                raw_json = json.loads(base64.b64decode(res.json()['content']).decode('utf-8'))
                return pd.DataFrame(raw_json.get("data", [])), raw_json.get("last_update", "N/A")
    except Exception as e:
        st.sidebar.error(f"GitHub 同步失敗: {e}")
    return pd.DataFrame(), "錯誤"

# ==============================
# 4. 排程控制邏輯 (非阻塞式分步執行)
# ==============================
SCHEDULE_TIMES = ["09:30", "10:30", "11:20", "12:20", "13:15", "14:30", "20:00", "21:10", "22:40"]
curr_min_str = now_taipei().strftime("%Y-%m-%d %H:%M")

# 載入名單
try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        full_list = json.load(f)['stocks']
except:
    full_list = ["2330.TW", "2454.TW", "2317.TW"]

# --- A. 觸發檢查 ---
is_target_time = any(curr_min_str.endswith(t) for t in SCHEDULE_TIMES)
if is_target_time and st.session_state.last_run_key != curr_min_str and not st.session_state.is_scanning:
    # 啟動鎖定
    st.session_state.is_scanning = True
    st.session_state.last_run_key = curr_min_str
    st.session_state.scan_idx = 0
    st.session_state.found_list = []
    st.session_state.df_results = pd.DataFrame() # 重置顯示

# --- B. 分步掃描循環 (利用 Rerun 代替 Sleep) ---
if st.session_state.is_scanning:
    batch_size = 15
    start = st.session_state.scan_idx
    end = min(start + batch_size, len(full_list))
    
    # 顯示進度 UI
    st.info(f"🚀 全量掃描中: {start} ~ {end} / {len(full_list)}")
    
    # 執行一小批
    chunk = run_batch_logic(full_list[start:end])
    if chunk:
        st.session_state.found_list.extend(chunk)
        st.session_state.df_results = pd.DataFrame(st.session_state.found_list)
    
    # 更新進度步進
    st.session_state.scan_idx += batch_size
    
    # 檢查結束條件
    if st.session_state.scan_idx >= len(full_list):
        st.session_state.is_scanning = False
        # 存檔至 GitHub
        final_meta = {"data": st.session_state.found_list, "last_update": now_taipei().strftime("%Y-%m-%d %H:%M")}
        sync_github(final_meta, mode="upload")
        st.success("✅ 掃描任務完成並已備份至雲端")
        st.rerun()
    else:
        # 重要：這裡不需要 sleep，因為 API 下載本身就有延遲，且 rerun 會重新跑過 check 邏輯
        st.rerun()

# ==============================
# 5. UI 介面
# ==============================
st.title("📊 台股多頭融合掃描器 v3.5")
c1, c2, c3 = st.columns(3)
c1.metric("⏰ 台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("📡 掃描狀態", "運行中..." if st.session_state.is_scanning else "待機")
c3.metric("📈 本次訊號", len(st.session_state.df_results))

st.divider()

if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 等待下一個排程觸發點...")

with st.sidebar:
    st.header("⚙️ 管理面板")
    if st.button("☁️ 強制從雲端同步", use_container_width=True):
        df, update_time = sync_github(None, mode="download")
        if not df.empty:
            st.session_state.df_results = df
            st.toast(f"同步成功! 最後更新: {update_time}")
            st.rerun()
    
    st.subheader("📅 自動掃描點")
    st.code("\n".join(SCHEDULE_TIMES))
