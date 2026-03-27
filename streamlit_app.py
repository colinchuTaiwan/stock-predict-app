
import streamlit as st
import yfinance as yf
import pandas as pd
import json, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 全域設定與狀態初始化
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)

# 每 3 秒 UI 刷新 (稍微放慢降低 CPU 負擔)
st_autorefresh(interval=3000, key="sys_heartbeat")

# 初始化 Session State (生產級防呆)
state_keys = {
    "df_results": pd.DataFrame(),
    "last_run_key": "",
    "is_scanning": False,
    "scan_idx": 0,
    "found_list": [],
    "last_api_time": 0.0  # 用於節流 (Throttling)
}
for key, default in state_keys.items():
    if key not in st.session_state: st.session_state[key] = default

# ==============================
# 1. 指標計算 (精簡向量化)
# ==============================
def calc_indicators(df):
    df = df.copy()
    c = df['Close']
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = c.rolling(w).mean()
        df[f"ma{w}_b"] = (c - df[f"ma{w}"]) / df[f"ma{w}"]
    df["RK_p"] = (c - df['Open']) * 100 / df['Open']
    return df

# ==============================
# 2. 強化版掃描引擎 (帶 Retry 與 Throttling)
# ==============================
def run_batch_logic(codes):
    found = []
    # 節流檢查：確保兩次 API 請求之間至少間隔 1.5 秒
    elapsed = time.time() - st.session_state.last_api_time
    if elapsed < 1.5:
        time.sleep(1.5 - elapsed)
    
    st.session_state.last_api_time = time.time()

    # Retry 機制 (最多試 2 次)
    raw = pd.DataFrame()
    for _ in range(2):
        try:
            raw = yf.download(tickers=codes, period="300d", group_by="ticker", 
                              auto_adjust=False, threads=False, progress=False, timeout=10)
            if not raw.empty: break
        except:
            time.sleep(2)

    if raw.empty: return found

    for code in codes:
        try:
            df = raw[code].copy().dropna() if len(codes) > 1 else raw.copy().dropna()
            if len(df) < 200: continue
            
            ind = calc_indicators(df)
            last = ind.iloc[-1]
            p, rk, vol = round(last['Close'], 2), round(last['RK_p'], 1), int(last['Volume']/1000)
            ma = {w: last[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
            ma_b = {w: last.get(f'ma{w}_b', 0) for w in [20, 60, 100]}

            # 策略過濾：多頭排列 + 漲幅 + 量能
            if (1 < rk < 7 and p > max(ma[20], ma[60], ma[100], ma[200]) and vol > 100):
                ma_l = [ma[5], ma[10], ma[20], ma[60], ma[100], ma[200]]
                res_type = ""
                if (max(ma_l)/min(ma_l) < 1.06) and ma_b[200] < 0.12: res_type = "六線糾結"
                elif (max(ma_l[:5])/min(ma_l[:5]) < 1.06) and ma_b[100] < 0.12: res_type = "五線糾結"
                elif (max(ma_l[:4])/min(ma_l[:4]) < 1.06) and ma_b[60] < 0.12: res_type = "四線糾結"
                elif (max(ma_l[:3])/min(ma_l[:3]) < 1.06) and ma_b[20] < 0.12: res_type = "三線糾結"                
                if res_type:
                    found.append({"股票代號": code, "價格": p, "漲幅%": rk, "型態": res_type, "時間": now_taipei().strftime("%H:%M")})
        except: continue
    return found

# ==============================
# 3. GitHub 存取 (帶狀態監控)
# ==============================
def sync_github(data_dict, mode="upload"):
    try:
        repo, token = st.secrets["GITHUB_REPO"], st.secrets["GITHUB_TOKEN"]
        path = st.secrets.get("GITHUB_FILE", "scan_cache.json")
        url = f"https://api.github.com/repos/{repo}/contents/{path}"
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

        if mode == "upload":
            res = requests.get(url, headers=headers, timeout=5)
            sha = res.json().get("sha") if res.status_code == 200 else None
            content_b64 = base64.b64encode(json.dumps(data_dict, ensure_ascii=False).encode('utf-8')).decode('utf-8')
            payload = {"message": f"🤖 Scan {now_taipei()}", "content": content_b64, "branch": "main"}
            if sha: payload["sha"] = sha
            
            put_res = requests.put(url, headers=headers, json=payload, timeout=10)
            if put_res.status_code not in [200, 201]:
                st.sidebar.error(f"GitHub 上傳失敗: {put_res.status_code}")
        else:
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                raw = json.loads(base64.b64decode(res.json()['content']).decode('utf-8'))
                return pd.DataFrame(raw.get("data", [])), raw.get("last_update", "N/A")
    except Exception as e:
        st.sidebar.warning(f"GitHub 同步異常: {e}")
    return pd.DataFrame(), "N/A"

# ==============================
# 4. 非阻塞步進排程 (移除 Rerun 風暴)
# ==============================
SCHEDULE_TIMES = ["09:30", "10:30", "11:20", "12:20", "13:15", "14:30", "20:00", "21:30", "23:00"]
curr_m = now_taipei().strftime("%Y-%m-%d %H:%M")

# 載入名單
try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        full_list = json.load(f)['stocks']
except:
    full_list = ["2330.TW"]

# A. 觸發掃描
if any(curr_m.endswith(t) for t in SCHEDULE_TIMES) and st.session_state.last_run_key != curr_m and not st.session_state.is_scanning:
    st.session_state.is_scanning = True
    st.session_state.last_run_key = curr_m
    st.session_state.scan_idx = 0
    st.session_state.found_list = []

# B. 步進邏輯 (由 autorefresh 自動驅動，不再手動 st.rerun)
if st.session_state.is_scanning:
    batch_sz = 20
    start = st.session_state.scan_idx
    
    # 防越界檢查
    if start >= len(full_list):
        st.session_state.is_scanning = False
        # 存檔與清理
        final_data = {"data": st.session_state.found_list[-500:], "last_update": now_taipei().strftime("%Y-%m-%d %H:%M")}
        sync_github(final_data, "upload")
        st.toast("✅ 掃描任務圓滿完成")
    else:
        end = min(start + batch_sz, len(full_list))
        st.info(f"🔍 掃描中: {start} ~ {end} / {len(full_list)}")
        
        chunk = run_batch_logic(full_list[start:end])
        if chunk:
            st.session_state.found_list.extend(chunk)
            st.session_state.df_results = pd.DataFrame(st.session_state.found_list)
        
        # 更新 index，等待下一次 autorefresh 觸發
        st.session_state.scan_idx += batch_sz

# ==============================
# 5. UI 呈現
# ==============================
st.title("📊 台股策略監控 v3.8")
c1, c2, c3 = st.columns(3)
c1.metric("⏰ 台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("📡 狀態", "掃描中" if st.session_state.is_scanning else "待機")
c3.metric("📈 訊號數", len(st.session_state.df_results))

st.divider()

if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 系統監控中...")

with st.sidebar:
    if st.button("☁️ 手動同步雲端", use_container_width=True):
        df, ut = sync_github(None, "download")
        st.session_state.df_results = df
        st.success(f"同步成功: {ut}")
    st.subheader("📅 自動掃描點")
    st.code("\n".join(SCHEDULE_TIMES))
