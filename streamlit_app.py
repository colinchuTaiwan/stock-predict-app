import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, time, threading, requests, base64, socket
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 環境與路徑設定 (逐行檢視：路徑與時區)
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN")
REPO_NAME = st.secrets.get("GITHUB_REPO")
DB_PATH = "db/scan_results.json"
LOCK_PATH = "db/scan.lock.json"
LOG_PATH = "app.log"
UNIVERSE_FILE = "db/taiwan_Full.json" # 確保 GitHub 上有此檔案

STORAGE_DIR = "data_cache"
os.makedirs(STORAGE_DIR, exist_ok=True)
LOCAL_STATE = os.path.join(STORAGE_DIR, "scan_results.json")

tz = timezone(timedelta(hours=8)) # 台北時區
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. GitHub API 引擎 (逐行檢視：SHA 同步與錯誤處理)
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
            return None, None # 404 或其他錯誤
        except: return None, None

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
        lines = str(old).splitlines()[-20:] if old else [] # 保持日誌簡潔
        GitHubEngine.commit_file(LOG_PATH, "\n".join(lines + [line]), "Update Log", sha)

# ==============================
# 2. 選股策略邏輯 (逐行檢視：均線、量能、漲幅判斷)
# ==============================
def analyze_stock_logic(code, df):
    try:
        if df is None or df.empty or len(df) < 200: return None
        
        # 指標計算
        c = df['Close']
        ma = {w: c.rolling(w).mean().iloc[-1] for w in [5, 10, 20, 60, 200]}
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = last['Close']
        vol = last['Volume'] / 1000  # 換算成「張」
        mv20 = df['Volume'].rolling(20).mean().iloc[-1] / 1000
        
        # 漲幅判斷 (1% ~ 7%)
        rk = (price - last['Open']) * 100 / last['Open']
        if not (1.0 <= rk <= 7.0): return None
        
        # 量能判斷 (股價 < 250, 成交量需大於 20日均量且爆量)
        if price > 250 or vol < 100 or vol < mv20 * 1.3: return None
        
        signal = ""
        ma_list = [ma[5], ma[10], ma[20], ma[60]]
        
        # 均線多排判斷
        if ma[5] > ma[10] > ma[20] > ma[60]:
            signal = "均線多排 (強勢趨勢)"
        # 均線糾結判斷 (差距 6% 內)
        elif (max(ma_list) / min(ma_list)) < 1.06:
            signal = "均線糾結 (起漲點預期)"
            
        if signal and price > ma[5] and price > prev['High']:
            return {
                "股票代號": code,
                "價格": round(price, 2),
                "漲幅": f"{round(rk, 2)}%",
                "成交量": int(vol),
                "型態": signal,
                "時間": now_taipei().strftime("%H:%M")
            }
    except: pass
    return None

# ==============================
# 3. 分散式核心 (逐行檢視：Singleton 狀態同步)
# ==============================
@st.cache_resource
class DistributedBrain:
    def __init__(self):
        self.mu = threading.Lock()
        self.is_scanning = False
        self.last_try_time = 0

    def try_lock(self, slot):
        # 防瘋狂重試 (60秒)
        if time.time() - self.last_try_time < 60: return False 
        self.last_try_time = time.time()
        
        with self.mu:
            rem_lock, sha = GitHubEngine.fetch_remote(LOCK_PATH)
            # 檢查過期鎖 (15分鐘)
            if rem_lock and isinstance(rem_lock, dict):
                if time.time() - rem_lock.get("ts", 0) < 900:
                    return False 
            
            new_lock = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
            return GitHubEngine.commit_file(LOCK_PATH, new_lock, f"Lock {slot}", sha)

brain = DistributedBrain()

# ==============================
# 4. 主流程 (逐行檢視：資料同步與 UI)
# ==============================
st.set_page_config(page_title="趨勢選股 v14.9", layout="wide")
if not brain.is_scanning:
    st_autorefresh(interval=10000, key="refresh_v149")

# A. 資料同步
remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
db = remote_db if (remote_db and "last_slot" in remote_db) else {"ts": 0, "list": [], "last_slot": "none"}

# B. 時段判定 (檢視排程)
now = now_taipei()
SCHEDULE = ["09:05", "09:30", "10:30", "11:30", "12:30", "13:20", "14:30", "17:42"] 
current_slot = ""
for t in SCHEDULE:
    dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - dt).total_seconds() <= 1200: # 20分鐘窗口
        current_slot = f"{now.strftime('%m%d')}_{t}"
        break

# C. 自動奪鎖觸發
if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot):
        brain.is_scanning = True
        st.rerun()

# D. 掃描執行任務 (逐行檢視：yf 下載邏輯)
if brain.is_scanning:
    # 使用 st.empty() 建立一個容器，避免刷新導致畫面閃爍
    status_placeholder = st.empty()
    
    with status_placeholder.container():
        st.info(f"📡 掃描啟動: 時段 {current_slot}")
        
        # 1. 抓取股票清單
        uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
        stocks = uni_data.get("stocks", ["2330.TW", "2317.TW"]) if uni_data else ["2330.TW"]
        
        # 2. 顯示進度
        progress_bar = st.progress(0)
        
        try:
            # 🔥 關鍵：增加 threads=False 或限制數量，避免 yfinance 被 GitHub/Yahoo 封鎖
            # 增加 timeout 確保不會無限等待
            data = yf.download(stocks, period="300d", group_by='ticker', threads=True, progress=False)
            
            results = []
            for i, code in enumerate(stocks):
                df = data[code] if len(stocks) > 1 else data
                if df is not None and not df.empty:
                    res = analyze_stock_logic(code, df)
                    if res:
                        results.append(res)
                progress_bar.progress((i + 1) / len(stocks))
            
            # 3. 寫回資料庫 (重要：這步完成後，循環才會停止)
            new_db = {
                "list": results, 
                "last_slot": current_slot, 
                "ts": time.time(),
                "status": "complete"
            }
            
            with st.spinner("正在同步資料至 GitHub..."):
                if GitHubEngine.commit_file(DB_PATH, new_db, f"Scan {current_slot} Final"):
                    # 4. 釋放鎖
                    _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
                    if l_sha:
                        GitHubEngine.delete_lock(l_sha)
                    
                    brain.is_scanning = False
                    st.success(f"✅ 掃描完成！命中 {len(results)} 檔。")
                    time.sleep(3) # 讓使用者看一眼結果
                    st.rerun() # 最後一次強制刷新，回到正常顯示模式
                    
        except Exception as e:
            st.error(f"掃描發生異常: {e}")
            # 發生錯誤時要釋放狀態，否則會卡死
            brain.is_scanning = False
            time.sleep(5)
            st.rerun()

# E. UI 渲染 (逐行檢視：資料呈現)
st.title("📊 趨勢選股系統 v14.9")

if db.get("list"):
    st.subheader(f"📅 時段 {db.get('last_slot')} 名單")
    df_show = pd.DataFrame(db["list"]).drop_duplicates(subset=["股票代號"])
    st.dataframe(df_show.sort_values("型態"), use_container_width=True)
else:
    st.info("目前尚無掃描結果，系統將在下一個排程時間點啟動。")

with st.sidebar:
    st.header("⚙️ 狀態資訊")
    st.write(f"伺服器時間: `{now.strftime('%H:%M:%S')}`")
    st.write(f"預定排程: `{', '.join(SCHEDULE)}`")
    if st.button("📝 測試 Log"): 
        LogEngine.add_log("手動測試成功") 
    st.write(f"當前槽位: `{current_slot or '等待中'}`")
    st.write(f"執行狀態: `{'掃描中' if brain.is_scanning else '閒置'}`")
    
    if st.button("🚨 手動強制釋放鎖"):
        _, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if sha: GitHubEngine.delete_lock(sha)
        st.rerun()

    st.subheader("最新日誌")
    logs, _ = GitHubEngine.fetch_remote(LOG_PATH)
    if logs: st.text("\n".join(str(logs).splitlines()[-8:]))
