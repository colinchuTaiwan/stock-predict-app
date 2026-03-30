import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, time, threading, requests, base64, socket
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 環境與路徑設定
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN")
REPO_NAME = st.secrets.get("GITHUB_REPO")
DB_PATH = "db/scan_results.json"
LOCK_PATH = "db/scan.lock.json"
LOG_PATH = "app.log"
UNIVERSE_FILE = "db/taiwan_Full.json"

STORAGE_DIR = "data_cache"
os.makedirs(STORAGE_DIR, exist_ok=True)
LOCAL_STATE = os.path.join(STORAGE_DIR, "scan_results.json")

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. GitHub API 引擎 (SHA 同步強化)
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
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{LOCK_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        try:
            r = requests.delete(url, headers=headers, json={"message": "Release Lock", "sha": sha}, timeout=10)
            return r.status_code == 200
        except: return False

class LogEngine:
    @staticmethod
    def add_log(msg):
        ts = now_taipei().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        old, sha = GitHubEngine.fetch_remote(LOG_PATH)
        lines = str(old).splitlines()[-20:] if old else []
        GitHubEngine.commit_file(LOG_PATH, "\n".join(lines + [line]), "Update Log", sha)

# ==============================
# 2. 策略與掃描邏輯
# ==============================
def analyze_stock(code, df):
    try:
        if df is None or len(df) < 60: return None
        last = df.iloc[-1]
        prev = df.iloc[-2]
        # 簡單範例：突破 20MA 且漲幅 > 2%
        ma20 = df['Close'].rolling(20).mean().iloc[-1]
        change = (last['Close'] - prev['Close']) / prev['Close']
        if last['Close'] > ma20 and change > 0.02:
            return {"股票代號": code, "價格": round(last['Close'], 2), "漲幅": f"{round(change*100, 2)}%", "時間": now_taipei().strftime("%H:%M")}
    except: return None
    return None

# ==============================
# 3. 分散式大腦 (增加嚴格節流)
# ==============================
@st.cache_resource
class DistributedBrain:
    def __init__(self):
        self.mu = threading.Lock()
        self.is_scanning = False
        self.last_try_time = 0

    def try_lock(self, slot):
        if time.time() - self.last_try_time < 60: return False # 嚴格 60 秒冷卻
        self.last_try_time = time.time()
        
        with self.mu:
            rem_lock, sha = GitHubEngine.fetch_remote(LOCK_PATH)
            # 檢查鎖是否過期 (15分鐘)
            if rem_lock and isinstance(rem_lock, dict):
                if time.time() - rem_lock.get("ts", 0) < 900:
                    return False 

            new_lock = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
            if GitHubEngine.commit_file(LOCK_PATH, new_lock, f"Lock {slot}", sha):
                self.is_scanning = True
                LogEngine.add_log(f"🚀 奪鎖成功: {slot}")
                return True
        return False

brain = DistributedBrain()

# ==============================
# 4. 主流程 UI
# ==============================
st.set_page_config(page_title="趨勢選股 v14.7", layout="wide")
st_autorefresh(interval=10000, key="refresh") # 增加到 10 秒刷新一次，節省 API

# 資料讀取
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
db = remote_db if remote_db else {"ts": 0, "list": [], "last_slot": ""}

# 時段判定
now = now_taipei()
SCHEDULE = ["09:00", "10:00", "11:00", "13:00", "14:30", "17:15"] # 修改為測試時段
current_slot = ""
for t in SCHEDULE:
    dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - dt).total_seconds() <= 900: # 15分鐘窗口
        current_slot = f"{now.strftime('%m%d')}_{t}"

# --- 自動觸發掃描 ---
if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot):
        st.rerun()

# --- 執行掃描區塊 ---
if brain.is_scanning:
    st.info(f"📡 正在掃描時段: {current_slot} ...")
    uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
    stocks = uni_data.get("stocks", ["2330.TW", "2317.TW", "2454.TW"]) if uni_data else ["2330.TW"]
    
    results = []
    progress_bar = st.progress(0)
    
    # 執行掃描
    data = yf.download(stocks, period="60d", group_by='ticker', progress=False)
    for i, code in enumerate(stocks):
        df = data[code] if len(stocks) > 1 else data
        res = analyze_stock(code, df)
        if res: results.append(res)
        progress_bar.progress((i + 1) / len(stocks))

    # 寫回 DB 並釋放鎖
    db.update({"list": results, "last_slot": current_slot, "ts": time.time()})
    if GitHubEngine.commit_file(DB_PATH, db, f"Complete {current_slot}"):
        _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if l_sha: GitHubEngine.delete_lock(l_sha)
        brain.is_scanning = False
        LogEngine.add_log(f"✅ 完成任務: {current_slot}")
        st.rerun()

# --- UI 渲染 ---
st.title("🛡️ 趨勢選股系統 v14.7")

col1, col2 = st.columns([3, 1])
with col1:
    st.subheader(f"📊 掃描清單 (時段: {db.get('last_slot', 'N/A')})")
    if db.get("list"):
        st.dataframe(pd.DataFrame(db["list"]), use_container_width=True)
    else:
        st.write("目前尚無符合條件之股票。")

with col2:
    st.header("⚙️ 系統狀態")
    st.write(f"時間: `{now.strftime('%H:%M:%S')}`")
    st.write(f"當前時段: `{current_slot or '非排程時間'}`")
    
    if st.button("🚨 強制釋放鎖"):
        _, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if sha: GitHubEngine.delete_lock(sha)
        st.rerun()
    
    st.subheader("最新日誌")
    logs, _ = GitHubEngine.fetch_remote(LOG_PATH)
    if logs: st.text("\n".join(str(logs).splitlines()[-8:]))
