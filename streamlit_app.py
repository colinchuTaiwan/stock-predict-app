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
# 2. 策略邏輯 (強化版)
# ==============================
def calc_indicators(df):
    df = df.copy()
    c = df['Close']
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = c.rolling(w).mean()
        df[f"ma{w}_b"] = (c - df[f"ma{w}"]) / df[f"ma{w}"]
    return df

def analyze_stock_logic(code, df):
    LogEngine.add_log(f"analyze_stock_logic")
    try:
        if df is None or df.empty: return None
        required_cols = ['Open', 'Close', 'High', 'Volume']
        if not all(col in df.columns for col in required_cols): return None
        
        df = df.dropna()
        if len(df) < 210: return None
        
        ind = calc_indicators(df)
        if ind.iloc[-1].isnull().any(): return None

        last, prev = ind.iloc[-1], ind.iloc[-2]
        price, open_, vol = last['Close'], last['Open'], last['Volume'] / 1000
        pre_close, pre_high, pre_vol = prev['Close'], prev['High'], prev['Volume'] / 1000
        rk = (price - open_) * 100 / open_

        ma_keys = [5, 10, 20, 60, 100, 200]
        ma = {w: last[f"ma{w}"] for w in ma_keys}
        pre_ma = {w: prev[f"ma{w}"] for w in ma_keys}
        ma_b = {w: last.get(f"ma{w}_b", 0) for w in [20, 60, 100, 200]}
        ma_d = {w: ma[w] - pre_ma[w] for w in ma}
        mv20 = df['Volume'].rolling(20).mean().iloc[-1] / 1000

        if not (1 < rk < 7): return None

        cond_basic = (price > pre_high and price > ma[5]) and \
                     (mv20 > 100 and vol > 100) and \
                     (price < 200) and (vol > pre_vol * 1.5)
        
        if not cond_basic: return None

        is_breakout = any(pre_close < pre_ma[w] for w in [5, 10, 20, 60])
        if not is_breakout: return None
        LogEngine.add_log(f"{code}")
        signal = None
        # 多排判斷
        if ma[5] > ma[20] > ma[60] > ma[100] > ma[200]:
            up_count = sum(1 for w in [5, 20, 60, 100, 200] if ma_d[w] > 0)
            levels = {5: "五線多排", 4: "四線多排", 3: "三線多排", 2: "二線多排"}
            signal = levels.get(up_count)

        # 糾結判斷
        ma_list = list(ma.values())
        if not signal and min(ma_list) > 0 and all(price > ma[w] for w in [20, 60, 100, 200]):
            if (max(ma_list)/min(ma_list) < 1.08) and ma_b[200] < 0.1: signal = "六線糾結"
            elif (max(ma_list[:5])/min(ma_list[:5]) < 1.08) and ma_b[100] < 0.15: signal = "五線糾結"
            elif (max(ma_list[:4])/min(ma_list[:4]) < 1.08) and ma_b[60] < 0.15: signal = "四線糾結"
            elif (max(ma_list[:3])/min(ma_list[:3]) < 1.08) and ma_b[20] < 0.15: signal = "三線糾結"

        if not signal: return None
        return {"股票代號": code, "價格": round(price, 2), "型態": signal, "更新時間": now_taipei().strftime("%H:%M:%S")}
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
            if rem_lock and time.time() - rem_lock.get("ts", 0) < 900: return False
            new_l = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
            if GitHubEngine.commit_file(LOCK_PATH, new_l, f"Lock:{slot}", sha):
                self.is_scanning, self.current_idx, self.temp_results = True, 0, []
                LogEngine.add_log(f"🚀 啟動掃描時段: {slot}")
                return True
            return False

brain = DistributedBrain()

# ==============================
# 4. 主流程與 UI
# ==============================
st.set_page_config(page_title="🛡️ 趨勢選股 v14.1", layout="wide")
st_autorefresh(interval=10000, key="v141_sync")

# 資料同步
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
local_db = json.load(open(LOCAL_STATE)) if os.path.exists(LOCAL_STATE) else {"ts": 0, "list": []}
if remote_db and remote_db.get("ts", 0) > local_db.get("ts", 0):
    with open(LOCAL_STATE, "w") as f: json.dump(remote_db, f)
    db = remote_db
else:
    db = local_db

# 排程檢查
now = now_taipei()
SCHEDULE = ["08:30", "10:06", "10:50", "11:50", "12:20", "13:15", "15:30"]
#LogEngine.add_log(f"{SCHEDULE }")

current_slot = ""
for t in SCHEDULE:
    slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - slot_dt).total_seconds() <= 600:
        current_slot = f"{now.strftime('%m%d')}_{t}"; break

if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot): st.rerun()
        
#LogEngine.add_log(f"{now.strftime('%H:%M:%S')}")

# 掃描核心
if brain.is_scanning:
    try:
        LogEngine.add_log(f"brain.is_scanning")
        uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
        universe = uni_data.get("stocks", []) if uni_data else ["2330.TW"]
        if brain.current_idx < len(universe):
            batch = universe[brain.current_idx : brain.current_idx + 15]
            with st.status(f"🚀 掃描中 {brain.current_idx}/{len(universe)}..."):
                LogEngine.add_log(f"yf.download")
                raw = yf.download(batch, period="300d", group_by='ticker', threads=True, progress=False)
                
                for code in batch:
                    df = raw[code] if len(batch) > 1 else raw
                    LogEngine.add_log(f"analyze_stock_logic")
                    res = analyze_stock_logic(code, df)
                    if res: 
                        brain.temp_results.append(res)
                        LogEngine.add_log(f"{res}")
                brain.current_idx += len(batch)
                db.update({"list": brain.temp_results, "status": "running", "progress": brain.current_idx, "total": len(universe), "ts": time.time()})
                with open(LOCAL_STATE, "w") as f: json.dump(db, f)
        else:
            db.update({"status": "complete", "last_slot": current_slot, "ts": time.time()})
            _, db_sha = GitHubEngine.fetch_remote(DB_PATH)
            GitHubEngine.commit_file(DB_PATH, db, f"Final {current_slot}", db_sha)
            LogEngine.add_log(f"✅ 完成掃描 {current_slot}, 發現 {len(brain.temp_results)} 檔")
            _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if l_sha: GitHubEngine.delete_lock(l_sha)
            brain.is_scanning = False
            st.rerun()
    except Exception as e:
        LogEngine.add_log(f"❌ 嚴重錯誤: {str(e)}")
        brain.is_scanning = False

# --- UI 渲染 ---
st.title("🛡️ 趨勢選股系統 v14.1")

# 進度條顯示
if db.get("status") == "running":
    prog = db.get("progress", 0) / max(db.get("total", 1), 1)
    st.progress(prog, text=f"任務執行中... {db.get('progress')}/{db.get('total')}")

# 結果顯示
if db.get("list"):
    st.subheader(f"📊 {db.get('last_slot')} 名單 (更新於: {now.strftime('%H:%M:%S')})")
    df_view = pd.DataFrame(db["list"]).drop_duplicates(subset=["股票代號"])
    st.dataframe(df_view.sort_values("型態"), use_container_width=True)
else:
    st.info("💡 掃描中或目前無符合標的...")

with st.sidebar:
    st.header("⚙️ 系統資訊")
    st.write(f"當前伺服器時間: `{now.strftime('%H:%M:%S')}`")
    st.write(f"預定排程: `{', '.join(SCHEDULE)}`")
    
    if st.button("📝 測試 Log"): LogEngine.add_log("手動測試成功")
    
    st.subheader("最新日誌")
    logs, _ = GitHubEngine.fetch_remote(LOG_PATH)
    if logs: st.text("\n".join(str(logs).splitlines()[-8:]))

with st.expander("🛠️ 診斷與管理"):
    st.write("當前 DB 狀態:", db)
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🚨 強制重置 (Reset)"):
            db.update({"status": "idle", "progress": 0, "list": [], "last_slot": "", "ts": time.time()})
            with open(LOCAL_STATE, "w") as f: json.dump(db, f)
            _, d_sha = GitHubEngine.fetch_remote(DB_PATH)
            GitHubEngine.commit_file(DB_PATH, db, "Manual Reset", d_sha)
            _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if l_sha: GitHubEngine.delete_lock(l_sha)
            st.rerun()
    with col2:
        if st.button("🔄 刷新頁面"): st.rerun()
