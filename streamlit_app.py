import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, time, threading, requests, base64, socket
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 環境設定
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN")
REPO_NAME = st.secrets.get("GITHUB_REPO")
DB_PATH = "db/scan_results.json"
LOCK_PATH = "db/scan.lock.json"
LOG_PATH = "app.log"
UNIVERSE_FILE = "db/taiwan_Full.json"

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. GitHub 引擎 (穩定輸出版)
# ==============================
class GitHubEngine:
    @staticmethod
    def fetch_remote(path):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                d = r.json()
                content = base64.b64decode(d["content"]).decode("utf-8")
                return (json.loads(content) if path.endswith(".json") else content), d["sha"]
        except: pass
        return None, None

    @staticmethod
    def commit_file(path, content, msg, sha=None):
        if not sha: _, sha = GitHubEngine.fetch_remote(path)
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        c_str = json.dumps(content, ensure_ascii=False) if isinstance(content, (dict, list)) else str(content)
        payload = {"message": msg, "content": base64.b64encode(c_str.encode()).decode()}
        if sha: payload["sha"] = sha
        try:
            r = requests.put(url, headers=headers, json=payload, timeout=15)
            return r.status_code in [200, 201]
        except: return False

    @staticmethod
    def delete_lock(sha):
        if not sha: return False
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{LOCK_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        try:
            r = requests.delete(url, headers=headers, json={"message": "Release Lock", "sha": sha}, timeout=10)
            return r.status_code == 200
        except: return False

# ==============================
# 2. 選股策略 (策略保持不變)
# ==============================
def analyze_stock_logic(code, df):
    try:
        if df is None or df.empty or len(df) < 200: return None
        c = df['Close']
        ma = {w: c.rolling(w).mean().iloc[-1] for w in [5, 10, 20, 60]}
        last, prev = df.iloc[-1], df.iloc[-2]
        price, vol = last['Close'], last['Volume'] / 1000
        mv20 = df['Volume'].rolling(20).mean().iloc[-1] / 1000
        rk = (price - last['Open']) * 100 / last['Open']
        
        if not (1.0 <= rk <= 7.0) or price > 250 or vol < 100 or vol < mv20 * 1.2: return None
        
        ma_list = [ma[5], ma[10], ma[20], ma[60]]
        signal = ""
        if ma[5] > ma[10] > ma[20] > ma[60]: signal = "均線多排"
        elif (max(ma_list) / min(ma_list)) < 1.06: signal = "均線糾結"
        
        if signal and price > ma[5] and price > prev['High']:
            return {"股票代號": code, "價格": round(price, 2), "漲幅": f"{round(rk, 2)}%", "成交量": int(vol), "型態": signal, "時間": now_taipei().strftime("%H:%M")}
    except: pass
    return None

# ==============================
# 3. 狀態大腦 (強化狀態清理)
# ==============================
@st.cache_resource
class DistributedBrain:
    def __init__(self):
        self.is_scanning = False
        self.last_try_time = 0

    def try_lock(self, slot):
        if time.time() - self.last_try_time < 30: return False
        self.last_try_time = time.time()
        
        rem_lock, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if rem_lock and isinstance(rem_lock, dict):
            if time.time() - rem_lock.get("ts", 0) < 600: return False # 10分鐘過期
        
        new_lock = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
        return GitHubEngine.commit_file(LOCK_PATH, new_lock, f"Lock {slot}", sha)

brain = DistributedBrain()

# ==============================
# 4. 主流程
# ==============================
st.set_page_config(page_title="趨勢選股 v15.0", layout="wide")

# 只有在「非掃描狀態」才自動刷新，間隔拉長到 20 秒，避免 GitHub API 塞車
if not brain.is_scanning:
    st_autorefresh(interval=20000, key="refresh_safe")

# 抓取遠端資料
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
db = remote_db if (remote_db and "last_slot" in remote_db) else {"ts": 0, "list": [], "last_slot": "none"}

# 時段判定
now = now_taipei()
SCHEDULE = ["09:05", "10:30", "13:20", "17:42", "17:52", "18:00"] 
current_slot = ""
for t in SCHEDULE:
    dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - dt).total_seconds() <= 1200:
        current_slot = f"{now.strftime('%m%d')}_{t}"
        break

# --- 觸發區 ---
if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot):
        brain.is_scanning = True
        st.rerun()

# --- 掃描執行區 ---
if brain.is_scanning:
    with st.status(f"🚀 正在掃描 {current_slot}...", expanded=True) as status:
        uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
        stocks = uni_data.get("stocks", ["2330.TW"]) if uni_data else ["2330.TW"]
        
        st.write(f"正在抓取 {len(stocks)} 檔股票數據...")
        # 🔥 修正：threads=False 降低並行壓力，防止卡圈圈
        data = yf.download(stocks, period="260d", group_by='ticker', threads=False, progress=False)
        
        results = []
        p_bar = st.progress(0)
        for i, code in enumerate(stocks):
            df = data[code] if len(stocks) > 1 else data
            res = analyze_stock_logic(code, df)
            if res: results.append(res)
            p_bar.progress((i + 1) / len(stocks))
            
        st.write("同步結果至 GitHub...")
        new_db = {"list": results, "last_slot": current_slot, "ts": time.time()}
        if GitHubEngine.commit_file(DB_PATH, new_db, f"Final {current_slot}"):
            _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
            GitHubEngine.delete_lock(l_sha)
            
            # 🔥 關鍵：清理快取並重置狀態，不要立即 rerun
            brain.is_scanning = False
            status.update(label="✅ 掃描完成！資料已同步。", state="complete", expanded=False)
            st.success(f"時段 {current_slot} 掃描結束。")
            st.balloons()
            time.sleep(5)
            st.rerun()

# --- UI 渲染 ---
st.title("📊 趨勢選股系統 v15.0")
col1, col2 = st.columns([3, 1])

with col1:
    if db.get("list"):
        st.subheader(f"📅 時段 {db.get('last_slot')} 名單")
        st.dataframe(pd.DataFrame(db["list"]), use_container_width=True)
    else:
        st.info("目前無符合條件標的，等待排程啟動。")

with col2:
    st.header("⚙️ 狀態")
    st.write(f"時間: `{now.strftime('%H:%M:%S')}`")
    st.write(f"槽位: `{current_slot or '等待排程'}`")
    if st.button("🚨 強制釋放鎖"):
        _, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        GitHubEngine.delete_lock(sha)
        st.rerun()
