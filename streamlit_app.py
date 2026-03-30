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

# 修正：確保快取目錄存在
STORAGE_DIR = "data_cache" 
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR, exist_ok=True)
LOCAL_STATE = os.path.join(STORAGE_DIR, "scan_results.json")

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. GitHub API 引擎
# ==============================
class GitHubEngine:
    @staticmethod
    def fetch_remote(path):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                data = res.json()
                content = base64.b64decode(data["content"]).decode("utf-8")
                return (json.loads(content) if path.endswith(".json") else content), data["sha"]
        except: pass
        return None, None

    @staticmethod
    def commit_file(path, content, message, sha=None):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        c_str = json.dumps(content, ensure_ascii=False) if isinstance(content, (dict, list)) else str(content)
        payload = {"message": message, "content": base64.b64encode(c_str.encode("utf-8")).decode("utf-8")}
        if sha: payload["sha"] = sha
        res = requests.put(url, headers=headers, json=payload, timeout=15)
        return res.status_code in [200, 201]

    @staticmethod
    def delete_lock(sha):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{LOCK_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.delete(url, headers=headers, json={"message": "Release Lock", "sha": sha}, timeout=10)
        return res.status_code == 200

class LogEngine:
    @staticmethod
    def add_log(message):
        try:
            ts = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
            new_line = f"[{ts}] {message}"
            old_content, sha = GitHubEngine.fetch_remote(LOG_PATH)
            if old_content is None:
                updated_content = f"=== Log Start ===\n{new_line}"
            else:
                lines = str(old_content).splitlines()
                updated_content = "\n".join(lines[-100:] + [new_line])
            return GitHubEngine.commit_file(LOG_PATH, updated_content, "Update Log", sha)
        except: return False

# ==============================
# 2. 策略邏輯
# ==============================
def calc_indicators(df):
    df = df.copy()
    c = df['Close']
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = c.rolling(w).mean()
        df[f"ma{w}_b"] = (c - df[f"ma{w}"]) / df[f"ma{w}"]
    return df

def analyze_stock_logic(code, df):
    try:
        if df is None or df.empty or len(df) < 210: return None
        df = df.dropna()
        ind = calc_indicators(df)
        last, prev = ind.iloc[-1], ind.iloc[-2]
        price, open_, vol = last['Close'], last['Open'], last['Volume'] / 1000
        pre_high, pre_vol = prev['High'], prev['Volume'] / 1000
        rk = (price - open_) * 100 / open_
        
        if not (1 < rk < 7): return None

        ma = {w: last[f"ma{w}"] for w in [5, 10, 20, 60, 100, 200]}
        ma_list = list(ma.values())
        mv20 = df['Volume'].rolling(20).mean().iloc[-1] / 1000
        
        cond_basic = (price > pre_high and price > ma[5]) and (mv20 > 100 and vol > 100) and (price < 200) and (vol > pre_vol * 1.5)
        if not cond_basic: return None

        signal = "趨勢突破" # 簡化範例
        return {"股票代號": code, "價格": round(price, 2), "型態": signal, "時間": now_taipei().strftime("%H:%M")}
    except: return None

# ==============================
# 3. 分散式核心
# ==============================
@st.cache_resource
class DistributedBrain:
    def __init__(self):
        self.mu = threading.Lock()
        self.is_scanning, self.current_idx, self.temp_results = False, 0, []

    def try_lock(self, slot):
        with self.mu:
            if self.is_scanning: return False
            rem_lock, sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if rem_lock and time.time() - rem_lock.get("ts", 0) < 600: return False
            new_l = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
            if GitHubEngine.commit_file(LOCK_PATH, new_l, f"Lock:{slot}", sha):
                self.is_scanning, self.current_idx, self.temp_results = True, 0, []
                LogEngine.add_log(f"🚀 開始掃描時段: {slot}")
                return True
            return False

brain = DistributedBrain()

# ==============================
# 4. UI 與 主流程
# ==============================
st.set_page_config(page_title="v13.8 終極選股引擎", layout="wide")

# --- 關鍵修正：全程式只保留這一個 autorefresh，避免 Duplicate Key 錯誤 ---
st_autorefresh(interval=10000, key="global_sync_v138")

# 資料同步
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
if os.path.exists(LOCAL_STATE):
    with open(LOCAL_STATE, "r") as f: local_db = json.load(f)
else:
    local_db = {"ts": 0, "list": []}

if remote_db and remote_db.get("ts", 0) > local_db.get("ts", 0):
    with open(LOCAL_STATE, "w") as f: json.dump(remote_db, f)
    db = remote_db
else:
    db = local_db

# 排程檢查
now = now_taipei()
SCHEDULE = ["08:59", "09:22", "10:50", "11:50", "12:20", "13:15", "15:30"]
current_slot = ""
for t in SCHEDULE:
    slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - slot_dt).total_seconds() <= 300:
        current_slot = f"{now.strftime('%m%d')}_{t}"
        break

if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot): st.rerun()

# 掃描執行
if brain.is_scanning:
    try:
        uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
        universe = uni_data.get("stocks", []) if uni_data else ["2330.TW"]
        if brain.current_idx < len(universe):
            batch = universe[brain.current_idx : brain.current_idx + 10]
            with st.status(f"🚀 執行中 {brain.current_idx}/{len(universe)}..."):
                raw = yf.download(batch, period="300d", group_by='ticker', threads=False, progress=False)
                for code in batch:
                    df = raw[code] if len(batch) > 1 else raw
                    res = analyze_stock_logic(code, df)
                    if res: 
                        brain.temp_results.append(res)
                        LogEngine.add_log(f"⭐ 發現標的: {code}")
                brain.current_idx += len(batch)
                db.update({"list": brain.temp_results, "status": "running", "progress": brain.current_idx, "total": len(universe), "ts": time.time()})
                with open(LOCAL_STATE, "w") as f: json.dump(db, f)
        else:
            db.update({"status": "complete", "last_slot": current_slot, "ts": time.time()})
            _, db_sha = GitHubEngine.fetch_remote(DB_PATH)
            GitHubEngine.commit_file(DB_PATH, db, f"Final {current_slot}", db_sha)
            LogEngine.add_log(f"✅ 完成掃描 {current_slot}")
            _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if l_sha: GitHubEngine.delete_lock(l_sha)
            brain.is_scanning = False
            st.success("✅ 完成"); st.rerun()
    except Exception as e:
        LogEngine.add_log(f"❌ 錯誤: {str(e)}")
        brain.is_scanning = False

# --- UI 展示 ---
st.title("🛡️ 趨勢選股系統 v13.8")

if db.get("list"):
    st.subheader(f"📊 {db.get('last_slot')} 符合條件名單")
    st.dataframe(pd.DataFrame(db["list"]), use_container_width=True)

with st.sidebar:
    st.header("🛠️ 系統狀態")
    if st.button("📝 測試寫入 Log"):
        if LogEngine.add_log(f"手動測試: {now.strftime('%H:%M:%S')}"):
            st.success("Log 寫入成功")
        else:
            st.error("Log 寫入失敗")
    
    st.subheader("最新日誌")
    logs, _ = GitHubEngine.fetch_remote(LOG_PATH)
    if logs:
        st.text("\n".join(str(logs).splitlines()[-10:]))

with st.expander("🛠️ 管理員工具"):
    if st.button("🚨 重置全域鎖定"):
        _, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if sha: GitHubEngine.delete_lock(sha)
        st.rerun()
