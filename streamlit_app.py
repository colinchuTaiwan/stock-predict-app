import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, time, threading, requests, base64, socket
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 雲端環境變數與路徑
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN")
REPO_NAME = st.secrets.get("REPO_NAME")
DB_PATH = "db/scan_results.json"
LOCK_PATH = "db/scan.lock.json"
STORAGE_DIR = "/tmp/stock_app"
LOCAL_STATE = os.path.join(STORAGE_DIR, "scan_results.json")

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. 帶快取的 GitHub 同步工具 (解決 Rate Limit)
# ==============================
class GitHubEngine:
    @staticmethod
    @st.cache_data(ttl=15) # 🔥 15秒內不重複請求 GitHub，保護 API Quota
    def fetch_remote(path):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            data = res.json()
            content = json.loads(base64.b64decode(data["content"]).decode("utf-8"))
            return content, data["sha"]
        return None, None

    @staticmethod
    def commit_file(path, content_dict, message, sha=None):
        """寫入 GitHub (CAS 機制)"""
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        content_bytes = json.dumps(content_dict, ensure_ascii=False).encode("utf-8")
        payload = {"message": message, "content": base64.b64encode(content_bytes).decode("utf-8")}
        if sha: payload["sha"] = sha
        res = requests.put(url, headers=headers, json=payload)
        return res.status_code in [200, 201]

    @staticmethod
    def delete_lock(sha):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{LOCK_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.delete(url, headers=headers, json={"message": "Release Lock", "sha": sha})
        return res.status_code == 200

# ==============================
# 2. 分散式任務管理器 (Double-Check Locking)
# ==============================
@st.cache_resource
class DistributedBrain:
    def __init__(self):
        self.mu = threading.Lock()
        self.is_scanning = False
        self.current_idx = 0
        self.temp_results = []

    def try_lock(self, slot):
        with self.mu:
            if self.is_scanning: return False
            
            # 🔥 Step 1: 第一次 GET 檢查
            remote_lock, sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if remote_lock and time.time() - remote_lock.get("ts", 0) < 600:
                return False 
            
            # 🔥 Step 2: 嘗試奪鎖 (利用 GitHub 的 SHA 進行原子操作)
            # 如果在 GET 與 PUT 之間有人先改了鎖，GitHub 會回傳 409 Conflict
            new_lock = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
            success = GitHubEngine.commit_file(LOCK_PATH, new_lock, f"Lock: {slot}", sha)
            
            if success:
                self.is_scanning, self.current_idx, self.temp_results = True, 0, []
                return True
            return False

brain = DistributedBrain()

# ==============================
# 3. 主程序邏輯
# ==============================
st.set_page_config(page_title="v13.5 分散式穩定版", layout="wide")
st_autorefresh(interval=5000, key="global_refresh")

# A. 全域同步：檢查遠端 DB 是否比本地新
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
if remote_db:
    # 讀取本地快取進行比較
    local_db = json.load(open(LOCAL_STATE)) if os.path.exists(LOCAL_STATE) else {"ts": 0}
    if remote_db.get("ts", 0) > local_db.get("ts", 0):
        os.makedirs(STORAGE_DIR, exist_ok=True)
        with open(LOCAL_STATE, "w") as f: json.dump(remote_db, f)
        st.toast("🔄 已同步最新雲端結果")

db = json.load(open(LOCAL_STATE)) if os.path.exists(LOCAL_STATE) else {"last_slot":"", "list":[], "status":"idle"}
now = now_taipei()
SCHEDULE = ["09:00", "09:30", "10:30", "11:30", "12:30", "13:15"]

# B. 時段觸發
current_slot = ""
for t in SCHEDULE:
    slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - slot_dt).total_seconds() <= 300:
        current_slot = f"{now.strftime('%m%d')}_{t}"
        break

if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot):
        st.rerun()

# C. 任務執行 (Error Handling 強化)
if brain.is_scanning:
    universe = ["2330.TW", "2317.TW", "2454.TW", "2303.TW", "2881.TW"] # 假設
    try:
        if brain.current_idx < len(universe):
            batch = universe[brain.current_idx : brain.current_idx + 5]
            with st.status(f"🚀 {get_worker_id()} 執行中..."):
                time.sleep(2) # 模擬 yf 下載
                for code in batch:
                    brain.temp_results.append({"股票": code, "價格": 110.0, "時間": now.strftime("%H:%M")})
                brain.current_idx += len(batch)
                # 更新本地進度
                db.update({"list": brain.temp_results, "status": "running", "ts": time.time()})
                with open(LOCAL_STATE, "w") as f: json.dump(db, f)
        else:
            # 完成並存檔至 GitHub
            db.update({"status": "complete", "last_slot": current_slot})
            _, db_sha = GitHubEngine.fetch_remote(DB_PATH)
            GitHubEngine.commit_file(DB_PATH, db, f"Final {current_slot}", db_sha)
            # 釋放鎖
            _, lock_sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if lock_sha: GitHubEngine.delete_lock(lock_sha)
            brain.is_scanning = False
            st.success("✅ 全域同步完成")
            st.rerun()
    except Exception as e:
        # 🛡️ 錯誤恢復機制
        st.error(f"任務中斷: {e}")
        _, lock_sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if lock_sha: GitHubEngine.delete_lock(lock_sha)
        brain.is_scanning = False

# ==============================
# 4. 前端展示
# ==============================
st.title("🌐 選股實驗室 v13.5 (Distributed Consensus)")

# 顯示誰正在掃描
remote_lock, _ = GitHubEngine.fetch_remote(LOCK_PATH)
if remote_lock:
    st.info(f"⚡ 任務執行中: {remote_lock['slot']} | 節點: {remote_lock['worker']}")

if db.get("list"):
    st.subheader(f"📊 {db['last_slot']} 結果")
    st.table(pd.DataFrame(db["list"]))

st.caption(f"Worker: {get_worker_id()} | Last Sync: {datetime.fromtimestamp(db.get('ts', 0)).strftime('%H:%M:%S')}")
