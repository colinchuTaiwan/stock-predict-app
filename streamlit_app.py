import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, time, threading, requests, base64, socket
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 基礎設定
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

def now_taipei():
    return datetime.now(tz)

def get_worker_id():
    return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. Streamlit State（🔥核心修正）
# ==============================
if "last_try_time" not in st.session_state:
    st.session_state.last_try_time = 0

# ==============================
# 2. GitHub Engine
# ==============================
class GitHubEngine:

    @staticmethod
    def fetch_remote(path):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}

        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                content = base64.b64decode(data["content"]).decode()
                return (json.loads(content) if path.endswith(".json") else content), data["sha"]
        except:
            pass

        return None, None

    @staticmethod
    def commit_file(path, content, msg, sha=None):
        if not sha:
            _, sha = GitHubEngine.fetch_remote(path)

        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}

        c_str = json.dumps(content, ensure_ascii=False) if isinstance(content, (dict, list)) else str(content)

        payload = {
            "message": msg,
            "content": base64.b64encode(c_str.encode()).decode()
        }

        if sha:
            payload["sha"] = sha

        try:
            r = requests.put(url, headers=headers, json=payload, timeout=15)
            return r.status_code in [200, 201]
        except:
            return False

    @staticmethod
    def delete_lock(sha):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{LOCK_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}

        try:
            r = requests.delete(url, headers=headers, json={"message": "release", "sha": sha})
            return r.status_code == 200
        except:
            return False

# ==============================
# 3. Log Engine（節流）
# ==============================
class LogEngine:
    last_log = 0

    @staticmethod
    def add_log(msg):

        if time.time() - LogEngine.last_log < 5:
            return
        LogEngine.last_log = time.time()

        ts = now_taipei().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"

        old, sha = GitHubEngine.fetch_remote(LOG_PATH)

        if old:
            lines = str(old).splitlines()
            new = "\n".join(lines[-30:] + [line])
        else:
            new = line

        GitHubEngine.commit_file(LOG_PATH, new, "log", sha)

# ==============================
# 4. 分散式 Brain（穩定版）
# ==============================
@st.cache_resource
class DistributedBrain:

    def __init__(self):
        self.mu = threading.Lock()
        self.is_scanning = False
        self.current_idx = 0
        self.temp_results = []

    def try_lock(self, slot):

        now_ts = time.time()

        # 🔥 Streamlit rerun 防爆
        if now_ts - st.session_state.last_try_time < 15:
            return False

        st.session_state.last_try_time = now_ts

        LogEngine.add_log("try_lock")

        with self.mu:

            rem_lock, sha = GitHubEngine.fetch_remote(LOCK_PATH)

            # 🔥 stale lock（15分鐘）
            if rem_lock and isinstance(rem_lock, dict):
                if time.time() - rem_lock.get("ts", 0) < 900:
                    return False

            new_lock = {
                "slot": slot,
                "ts": time.time(),
                "worker": get_worker_id(),
                "status": "active"
            }

            if GitHubEngine.commit_file(LOCK_PATH, new_lock, f"Lock {slot}", sha):
                self.is_scanning = True
                self.current_idx = 0
                self.temp_results = []
                LogEngine.add_log(f"🚀 LOCK OK {slot}")
                return True

            return False

brain = DistributedBrain()

# ==============================
# 5. UI Setup
# ==============================
st.set_page_config(page_title="選股系統 v14.7", layout="wide")
st_autorefresh(interval=3000, key="refresh")

# ==============================
# 6. 讀取資料
# ==============================
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)

local_db = json.load(open(LOCAL_STATE)) if os.path.exists(LOCAL_STATE) else {"ts": 0}

db = remote_db if remote_db and remote_db.get("ts", 0) > local_db.get("ts", 0) else local_db

# ==============================
# 7. 排程
# ==============================
now = now_taipei()

SCHEDULE = ["08:30","09:25","10:40","11:30","12:30","13:15","17:22"]

current_slot = ""

for t in SCHEDULE:
    dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - dt).total_seconds() <= 600:
        current_slot = f"{now.strftime('%m%d')}_{t}"

# ==============================
# 8. 防重觸發（關鍵）
# ==============================
if (
    current_slot
    and db.get("last_slot") != current_slot
    and not brain.is_scanning
    and time.time() - db.get("ts", 0) > 20
):

    if brain.try_lock(current_slot):
        st.rerun()

# ==============================
# 9. Lock 狀態顯示
# ==============================
lock, _ = GitHubEngine.fetch_remote(LOCK_PATH)

if lock:
    st.warning(f"⚠️ Worker running: {lock.get('worker')}")

# ==============================
# 10. UI
# ==============================
st.title("📊 選股系統 v14.7")

if db.get("list"):
    df = pd.DataFrame(db["list"]).drop_duplicates(subset=["股票代號"])
    st.dataframe(df, use_container_width=True)

# ==============================
# 11. Sidebar
# ==============================
with st.sidebar:

    st.header("系統資訊")

    st.write(f"時間: `{now.strftime('%H:%M:%S')}`")
    st.write(f"Slot: `{current_slot}`")
    st.write(f"Scanning: `{brain.is_scanning}`")

    if st.button("🧪 log test"):
        LogEngine.add_log("manual test")

    if st.button("🚨 force unlock"):
        _, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if sha:
            GitHubEngine.delete_lock(sha)
        st.rerun()

    st.subheader("Logs")

    logs, _ = GitHubEngine.fetch_remote(LOG_PATH)
    if logs:
        st.text("\n".join(str(logs).splitlines()[-10:]))
