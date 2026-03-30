import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, time, threading, requests, base64, socket
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 環境設定 (修正路徑權限)
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN")
REPO_NAME = st.secrets.get("REPO_NAME")
DB_PATH = "db/scan_results.json"
LOCK_PATH = "db/scan.lock.json"
LOG_PATH = "app.log"
UNIVERSE_FILE = "db/taiwan_Full.json" 

# 修正：直接使用當前目錄，避免 /tmp 權限問題
STORAGE_DIR = "data_cache" 
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR, exist_ok=True)
LOCAL_STATE = os.path.join(STORAGE_DIR, "scan_results.json")

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. GitHub API 引擎 (修正寫入邏輯)
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
        # 處理內容格式
        if isinstance(content, (dict, list)):
            c_str = json.dumps(content, ensure_ascii=False)
        else:
            c_str = str(content)
        
        payload = {
            "message": message,
            "content": base64.b64encode(c_str.encode("utf-8")).decode("utf-8")
        }
        if sha: payload["sha"] = sha # 如果是更新，必須帶 sha；如果是新建，不帶 sha
        
        res = requests.put(url, headers=headers, json=payload, timeout=15)
        return res.status_code in [200, 201]

class LogEngine:
    @staticmethod
    def add_log(message):
        try:
            ts = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
            new_line = f"[{ts}] {message}"
            
            # 獲取舊 Log 與 SHA
            old_content, sha = GitHubEngine.fetch_remote(LOG_PATH)
            
            if old_content is None:
                updated_content = f"=== Log Start ===\n{new_line}"
            else:
                lines = str(old_content).splitlines()
                # 限制長度，只留最後 100 行避免 API 過載
                updated_content = "\n".join(lines[-100:] + [new_line])
            
            success = GitHubEngine.commit_file(LOG_PATH, updated_content, "Update Log", sha)
            return success
        except:
            return False

# ==============================
# 2. 策略與指標 (略，維持 v13.6)
# ==============================
# ... (此處保留 analyze_stock_logic 與 calc_indicators)

# ==============================
# 3. 分散式大腦
# ==============================
@st.cache_resource
class DistributedBrain:
    def __init__(self):
        self.mu = threading.Lock()
        self.is_scanning, self.current_idx, self.temp_results = False, 0, []

brain = DistributedBrain()

# ==============================
# 4. 主流程 (修正排程與 Log 紀錄)
# ==============================
st.set_page_config(page_title="v13.7 穩定診斷版", layout="wide")
st_autorefresh(interval=10000, key="global_sync") # 稍微拉長頻率，避免 GitHub API rate limit

# 初始化資料
if not os.path.exists(LOCAL_STATE):
    with open(LOCAL_STATE, "w") as f: json.dump({"ts": 0, "list": []}, f)

# 同步雲端
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
with open(LOCAL_STATE, "r") as f: local_db = json.load(f)
if remote_db and remote_db.get("ts", 0) > local_db.get("ts", 0):
    with open(LOCAL_STATE, "w") as f: json.dump(remote_db, f)

db = local_db
now = now_taipei()
SCHEDULE = ["08:59", "09:35", "10:50", "11:50", "12:20", "13:15", "15:30"]

# 檢查排程並觸發
current_slot = ""
for t in SCHEDULE:
    slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - slot_dt).total_seconds() <= 600: # 10分鐘內視為有效時段
        current_slot = f"{now.strftime('%m%d')}_{t}"
        break

# 執行自動鎖定與 Log
if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    # 嘗試取得 GitHub 鎖
    rem_lock, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
    if not rem_lock or (time.time() - rem_lock.get("ts", 0) > 900): # 鎖定超過15分則失效
        new_l = {"slot": current_slot, "ts": time.time(), "worker": get_worker_id()}
        if GitHubEngine.commit_file(LOCK_PATH, new_l, f"Lock:{current_slot}", l_sha):
            brain.is_scanning = True
            LogEngine.add_log(f"🚀 掃描啟動: {current_slot} | 排程: {SCHEDULE}")
            st.rerun()


# ==============================
# 2. 指標與策略邏輯
# ==============================
def calc_indicators(df):
    df = df.copy()
    c = df['Close']
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = c.rolling(w).mean()
        df[f"ma{w}_b"] = (c - df[f"ma{w}"]) / df[f"ma{w}"]
    df["RK_p"] = (c - df['Open']) * 100 / df['Open']
    return df

def analyze_stock_logic(code, df):
    try:
        if df is None or df.empty: return None
        required_cols = ['Open', 'Close', 'High', 'Volume']
        if not all(col in df.columns for col in required_cols): return None
        df = df.dropna()
        if len(df) < 210: return None
        ind = calc_indicators(df)
        last, prev = ind.iloc[-1], ind.iloc[-2]
        
        price, open_, vol = last['Close'], last['Open'], last['Volume'] / 1000
        pre_high, pre_vol = prev['High'], prev['Volume'] / 1000
        rk = (price - open_) * 100 / open_
        
        if not (1 < rk < 7): return None

        ma_keys = [5, 10, 20, 60, 100, 200]
        ma = {w: last[f"ma{w}"] for w in ma_keys}
        pre_ma = {w: prev[f"ma{w}"] for w in ma_keys}
        ma_b = {w: last.get(f"ma{w}_b", 0) for w in [20, 60, 100, 200]}
        ma_d = {w: ma[w] - pre_ma[w] for w in ma}
        mv20 = df['Volume'].rolling(20).mean().iloc[-1] / 1000

        cond_basic = (price > pre_high and price > ma[5]) and (mv20 > 100 and vol > 100) and (price < 200) and (vol > pre_vol * 1.5)
        if not cond_basic: return None

        is_breakout = any(prev['Close'] < pre_ma[w] for w in [5, 10, 20, 60])
        if not is_breakout: return None

        signal = None
        ma_list = list(ma.values())
        if all(price > ma[w] for w in [20, 60, 100, 200]):
            if (max(ma_list)/min(ma_list) < 1.08) and ma_b[200] < 0.1: signal = "六線糾結"
            elif (max(ma_list[:5])/min(ma_list[:5]) < 1.08) and ma_b[100] < 0.15: signal = "五線糾結"
            elif (max(ma_list[:4])/min(ma_list[:4]) < 1.08) and ma_b[60] < 0.15: signal = "四線糾結"
            elif (max(ma_list[:3])/min(ma_list[:3]) < 1.08) and ma_b[20] < 0.15: signal = "三線糾結"

        if not signal and ma[5] > ma[20] > ma[60] > ma[100] > ma[200]:
            up_count = sum(1 for w in [5, 20, 60, 100, 200] if ma_d[w] > 0)
            if up_count >= 5: signal = "五線多排"
            elif up_count == 4: signal = "四線多排"

        if not signal: return None
        return {"股票代號": code, "價格": round(price, 2), "型態": signal, "時間": now_taipei().strftime("%H:%M")}
    except: return None

# ==============================
# 3. 任務調度
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
                LogEngine.add_log(f"開始掃描時段: {slot}")
                return True
            return False

brain = DistributedBrain()

# ==============================
# 4. UI 與 主執行流
# ==============================
st.set_page_config(page_title="v13.5 終極選股引擎", layout="wide")
st_autorefresh(interval=5000, key="global_sync")

remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
local_db = json.load(open(LOCAL_STATE)) if os.path.exists(LOCAL_STATE) else {"ts": 0}
if remote_db and remote_db.get("ts", 0) > local_db.get("ts", 0):
    with open(LOCAL_STATE, "w") as f: json.dump(remote_db, f)

db = json.load(open(LOCAL_STATE)) if os.path.exists(LOCAL_STATE) else {"last_slot":"", "list":[], "status":"idle"}
now = now_taipei()
SCHEDULE = ["08:59", "09:22", "10:50", "11:50", "12:20", "13:15", "15:30"]
LogEngine.add_log(f"時間表: {SCHEDULE}")
current_slot = ""
for t in SCHEDULE:
    slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - slot_dt).total_seconds() <= 300:
        current_slot = f"{now.strftime('%m%d')}_{t}"; break

if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot): st.rerun()

# 掃描核心
if brain.is_scanning:
    try:
        uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
        universe = uni_data.get("stocks", []) if uni_data else ["2330.TW"]
        if brain.current_idx < len(universe):
            batch = universe[brain.current_idx : brain.current_idx + 10]
            LogEngine.add_log(f"正在下載批次: {', '.join(batch)}")
            with st.status(f"🚀 掃描進度 {brain.current_idx}/{len(universe)}..."):
                raw = yf.download(batch, period="300d", group_by='ticker', threads=False, progress=False)
                for code in batch:
                    df = raw[code] if len(batch) > 1 else raw
                    res = analyze_stock_logic(code, df)
                    if res: 
                            brain.temp_results.append(res)
                            LogEngine.add_log(f"⭐ 發現符合標的: {code}")
                brain.current_idx += len(batch)
                db.update({"list": brain.temp_results, "status": "running", "progress": brain.current_idx, "total": len(universe), "ts": time.time()})
                with open(LOCAL_STATE, "w") as f: json.dump(db, f)
        else:
            db.update({"status": "complete", "last_slot": current_slot, "ts": time.time()})
            _, db_sha = GitHubEngine.fetch_remote(DB_PATH)
            GitHubEngine.commit_file(DB_PATH, db, f"Final {current_slot}", db_sha)
            LogEngine.add_log(f"完成掃描 {current_slot}, 找到 {len(brain.temp_results)} 檔")
            _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if l_sha: GitHubEngine.delete_lock(l_sha)
            brain.is_scanning = False
            st.success("✅ 完成"); st.rerun()
    except Exception as e:
        LogEngine.add_log(f"錯誤: {str(e)}")
        _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if l_sha: GitHubEngine.delete_lock(l_sha)
        brain.is_scanning = False

# --- 前端展示 ---
st.title("🛡️ 趨勢選股系統 v13.5 (Log 版)")
remote_lock, _ = GitHubEngine.fetch_remote(LOCK_PATH)
if remote_lock:
    st.warning(f"⚡ 跨伺服器任務執行中: `{remote_lock['slot']}` | 節點: `{remote_lock['worker']}`")
    if db.get("status") == "running":
        st.progress(db.get("progress", 0)/max(db.get("total", 1), 1), text=f"進度: {db.get('progress')}/{db.get('total')}")

if db.get("list"):
    st.subheader(f"📊 {db.get('last_slot', '最新')} 符合條件名單")
    st.dataframe(pd.DataFrame(db["list"]), use_container_width=True)
else:
    st.info("💡 目前無符合趨勢之股票。")
# Debug 工具欄
with st.sidebar:
    st.header("🛠️ 診斷面板")
    if st.button("📝 測試寫入 Log"):
        if LogEngine.add_log(f"手動測試: {now.strftime('%H:%M:%S')}"):
            st.success("寫入成功！")
        else:
            st.error("寫入失敗，請檢查 Token 權限")
    
    st.subheader("最新日誌")
    logs, _ = GitHubEngine.fetch_remote(LOG_PATH)
    if logs:
        st.text("\n".join(str(logs).splitlines()[-5:]))

if db.get("list"):
    st.write(f"最後更新時段: {db.get('last_slot')}")
    st.dataframe(db["list"])
    
with st.expander("🛠️ 管理員工具 & 系統日誌"):
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🚨 重置全域鎖定"):
            _, sha = GitHubEngine.fetch_remote(LOCK_PATH)
            if sha: GitHubEngine.delete_lock(sha)
            LogEngine.add_log("管理員手動重置鎖定")
            st.rerun()
    with col2:
        if st.button("🔄 刷新日誌"): st.rerun()
    
    # 顯示最近 Log
    logs, _ = GitHubEngine.fetch_remote(LOG_PATH)
    if logs:
        st.code(logs, language="text")
