import streamlit as st
import yfinance as yf
import pandas as pd
import json, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 系統環境與狀態初始化 (SSOT)
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)

# 驅動輪：每 4 秒刷新一次
st_autorefresh(interval=4000, key="battle_hardened_heartbeat")

state_schema = {
    "df_results": pd.DataFrame(),
    "last_run_key": "",
    "is_scanning": False,
    "scan_idx": 0,
    "found_list": [],
    "last_api_time": 0.0,
    "last_checkpoint_time": 0.0,
    "bootstrapped": False,
    "remote_version": 0.0 # 🔥 解決 Race Condition 的版本鎖
}
for key, default in state_schema.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ==============================
# 1. GitHub 持久化 (帶 Version Lock 與校驗)
# ==============================
def sync_github(data_dict=None, mode="upload"):
    try:
        repo, token = st.secrets.get("GITHUB_REPO"), st.secrets.get("GITHUB_TOKEN")
        path = st.secrets.get("GITHUB_FILE", "engine_state_v49.json")
        if not repo or not token: return None
        
        url = f"https://api.github.com/repos/{repo}/contents/{path}"
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

        # 🔥 A. 讀取遠端狀態 (用於恢復或版本檢查)
        res = requests.get(url, headers=headers, timeout=5)
        remote_data = None
        sha = None
        if res.status_code == 200:
            remote_json = res.json()
            sha = remote_json.get("sha")
            remote_data = json.loads(base64.b64decode(remote_json['content']).decode('utf-8'))

        if mode == "download":
            return remote_data

        if mode == "upload" and data_dict:
            # 🔥 解決 Race Condition：如果遠端版本更新，放棄本次寫入避免覆蓋新進度
            current_remote_ver = remote_data.get("version", 0) if remote_data else 0
            if current_remote_ver > data_dict.get("version", 0):
                return None 
            
            content_json = json.dumps(data_dict, ensure_ascii=False, indent=2)
            content_b64 = base64.b64encode(content_json.encode('utf-8')).decode('utf-8')
            
            payload = {
                "message": f"🤖 Sync v{data_dict['version']}", 
                "content": content_b64, "branch": "main"
            }
            if sha: payload["sha"] = sha
            requests.put(url, headers=headers, json=payload, timeout=10)
            return True
    except: pass
    return None

# 🔥 啟動引導 (Bootstrapping)
if not st.session_state.bootstrapped:
    remote = sync_github(mode="download")
    if remote:
        st.session_state.found_list = remote.get("found_list", [])
        st.session_state.df_results = pd.DataFrame(st.session_state.found_list)
        st.session_state.last_run_key = remote.get("last_run_key", "")
        st.session_state.scan_idx = remote.get("scan_idx", 0)
        st.session_state.is_scanning = remote.get("is_scanning", False)
        st.session_state.remote_version = remote.get("version", 0)
    st.session_state.bootstrapped = True
    st.toast("🔄 雲端狀態同步完成 (Version Lock Active)")

# ==============================
# 2. 確定性數據引擎 (Deterministic Engine)
# ==============================
def run_batch_logic(codes):
    if time.time() - st.session_state.last_api_time < 2.5: return None
    st.session_state.last_api_time = time.time()

    raw = pd.DataFrame()
    for _ in range(2):
        try:
            raw = yf.download(tickers=codes, period="300d", group_by="ticker", 
                              auto_adjust=False, threads=False, progress=False, timeout=8)
            # 🔥 強化：檢查資料列數，防止假成功 (Empty DataFrame)
            if not raw.empty and raw.shape[0] > 100: break 
        except: time.sleep(1.5)

    if raw is None or raw.empty or raw.shape[0] < 100: return []

    found = []
    for code in codes:
        try:
            df = raw.get(code) if len(codes) > 1 else raw
            if df is None or df.empty or len(df) < 200: continue
            
            df = df.copy().dropna()
            c, v = df['Close'], df['Volume']
            ma200 = c.rolling(200).mean().iloc[-1]
            v_ma20 = v.rolling(20).mean().iloc[-1]
            rk = ((c.iloc[-1] - df['Open'].iloc[-1]) * 100 / df['Open'].iloc[-1])
            
            # 策略核心：多頭排列 + 1.3倍量突破
            if not pd.isna(ma200) and c.iloc[-1] > ma200 and v.iloc[-1] > (1.3 * v_ma20) and 1 < rk < 7:
                found.append({
                    "股票代號": code, "價格": round(c.iloc[-1], 2), 
                    "漲幅%": round(rk, 1), "時間": now_taipei().strftime("%H:%M")
                })
        except: continue
    return found

# ==============================
# 3. 狀態機與定時 Checkpoint
# ==============================
SCHEDULE_TIMES = ["09:00", "09:30", "10:30", "11:20", "12:20", "13:15", "14:30", "20:00", "21:30", "22:56"]
now_dt = now_taipei()

# 🔥 修正：使用 datetime.combine 解決跨日與 1900-01-01 問題
is_trigger_time = False
target_t = ""
for t_str in SCHEDULE_TIMES:
    sched_t = datetime.strptime(t_str, "%H:%M").time()
    sched_dt = datetime.combine(now_dt.date(), sched_t).replace(tzinfo=tz)
    if abs((now_dt - sched_dt).total_seconds()) <= 60:
        is_trigger_time = True
        target_t = t_str
        break

try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        full_list = json.load(f)['stocks']
except: full_list = ["2330.TW"]

# A. 觸發掃描
if is_trigger_time and st.session_state.last_run_key != target_t and not st.session_state.is_scanning:
    st.session_state.is_scanning = True
    st.session_state.last_run_key = target_t
    st.session_state.scan_idx = 0
    st.session_state.found_list = []
    st.session_state.remote_version = time.time()

# B. 步進與定時 Checkpoint
if st.session_state.is_scanning:
    start = st.session_state.scan_idx
    if start >= len(full_list):
        st.session_state.is_scanning = False
        # 完成時最後存檔
        state = {
            "found_list": st.session_state.found_list, "last_run_key": st.session_state.last_run_key,
            "scan_idx": 0, "is_scanning": False, "version": time.time()
        }
        sync_github(state, mode="upload")
        st.toast("✅ 全量掃描完成")
    else:
        batch_sz = 15
        st.info(f"🔍 掃描中: {start} / {len(full_list)}")
        chunk = run_batch_logic(full_list[start:start+batch_sz])
        
        if chunk is not None:
            if chunk: st.session_state.found_list.extend(chunk)
            st.session_state.scan_idx += batch_sz
            st.session_state.df_results = pd.DataFrame(st.session_state.found_list)
            
            # 🔥 強化：Time-based Checkpoint (每 120 秒存一次，而非按數量)
            if time.time() - st.session_state.last_checkpoint_time > 120:
                st.session_state.remote_version = time.time()
                checkpoint = {
                    "found_list": st.session_state.found_list,
                    "last_run_key": st.session_state.last_run_key,
                    "scan_idx": st.session_state.scan_idx,
                    "is_scanning": True,
                    "version": st.session_state.remote_version
                }
                if sync_github(checkpoint, mode="upload"):
                    st.session_state.last_checkpoint_time = time.time()
                    st.caption("💾 背景 Checkpoint 已存入雲端...")

# ==============================
# 4. UI 介面
# ==============================
st.title("🛡️ 戰鬥硬化交易引擎 v4.9")
c1, c2, c3 = st.columns(3)
c1.metric("⏰ 台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("📡 引擎狀態", "🔥 掃描中" if st.session_state.is_scanning else "🟢 監聽中")
c3.metric("📊 累計標的", len(st.session_state.df_results))
st.dataframe(st.session_state.df_results, use_container_width=True, hide_index=True) 檢視程式碼並提出意見
