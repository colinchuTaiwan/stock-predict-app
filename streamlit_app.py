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

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. GitHub 引擎
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
# 2. 選股邏輯 (增加安全性檢查)
# ==============================
def analyze_stock_logic(code, df):
    try:
        # 檢查 DataFrame 是否合法且包含必要欄位
        if df is None or df.empty or len(df) < 200: return None
        if 'Close' not in df.columns: return None
        
        c = df['Close']
        ma = {w: c.rolling(w).mean().iloc[-1] for w in [5, 10, 20, 60]}
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = float(last['Close'])
        open_p = float(last['Open'])
        vol = float(last['Volume']) / 1000
        mv20 = df['Volume'].rolling(20).mean().iloc[-1] / 1000
        
        rk = (price - open_p) * 100 / open_p
        
        # 策略過濾
        if not (1.0 <= rk <= 8.0): return None
        if price > 280 or vol < 150 or vol < mv20 * 1.1: return None
        
        ma_list = [ma[5], ma[10], ma[20], ma[60]]
        signal = ""
        if ma[5] > ma[10] > ma[20] > ma[60]: signal = "均線多排"
        elif (max(ma_list) / min(ma_list)) < 1.07: signal = "均線糾結"
        
        if signal and price > ma[5] and price > prev['High']:
            return {
                "股票代號": code,
                "價格": round(price, 2),
                "漲幅": f"{round(rk, 2)}%",
                "成交量": int(vol),
                "型態": signal,
                "時間": now_taipei().strftime("%H:%M")
            }
    except Exception as e:
        pass
    return None

# ==============================
# 3. 狀態大腦
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
            if time.time() - rem_lock.get("ts", 0) < 600: return False
        new_lock = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
        return GitHubEngine.commit_file(LOCK_PATH, new_lock, f"Lock {slot}", sha)

brain = DistributedBrain()

# ==============================
# 4. 主流程
# ==============================
st.set_page_config(page_title="趨勢選股 v15.1", layout="wide")

if not brain.is_scanning:
    st_autorefresh(interval=30000, key="refresh_safe") # 拉長到 30 秒更穩定

remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
db = remote_db if (remote_db and isinstance(remote_db, dict) and "last_slot" in remote_db) else {"ts": 0, "list": [], "last_slot": "none"}

now = now_taipei()
SCHEDULE = ["09:05", "09:35", "10:35", "11:35", "12:35", "13:25", "17:58", "18:10", "20:00"] 
current_slot = ""
for t in SCHEDULE:
    dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - dt).total_seconds() <= 1500:
        current_slot = f"{now.strftime('%m%d')}_{t}"
        break

if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot):
        brain.is_scanning = True
        st.rerun()

# --- 核心掃描區 (🔥修正 KeyError) ---
if brain.is_scanning:
    with st.status(f"🚀 正在掃描 {current_slot}...", expanded=True) as status:
        uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
        stocks = uni_data.get("stocks", ["2330.TW", "2317.TW"]) if uni_data else ["2330.TW"]
        
        st.write(f"準備下載 {len(stocks)} 檔股票...")
        # 這裡不使用 threads 以免連線過多被封鎖
        try:
            data = yf.download(stocks, period="260d", group_by='ticker', threads=False, progress=False)
            
            results = []
            p_bar = st.progress(0)
            
            for i, code in enumerate(stocks):
                try:
                    # 🔥 修正點：安全檢查 data 裡面是否有該 code
                    if len(stocks) > 1:
                        if code in data.columns.levels[0]:
                            df = data[code]
                        else:
                            continue # 跳過抓不到的股票
                    else:
                        df = data
                    
                    res = analyze_stock_logic(code, df)
                    if res: results.append(res)
                except:
                    continue
                p_bar.progress((i + 1) / len(stocks))
            
            st.write("同步至 GitHub...")
            new_db = {"list": results, "last_slot": current_slot, "ts": time.time()}
            if GitHubEngine.commit_file(DB_PATH, new_db, f"Final {current_slot}"):
                _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
                GitHubEngine.delete_lock(l_sha)
                brain.is_scanning = False
                status.update(label="✅ 掃描完成！", state="complete", expanded=False)
                st.balloons()
                time.sleep(3)
                st.rerun()
        except Exception as e:
            st.error(f"下載失敗: {e}")
            brain.is_scanning = False
            time.sleep(5)
            st.rerun()

# --- UI 呈現 ---
st.title("📊 多頭趨勢選股實驗室 v11.2")
if db.get("list"):
    st.subheader(f"📅 最新結果: {db.get('last_slot')}")
    st.dataframe(pd.DataFrame(db["list"]), use_container_width=True)
else:
    st.info("等待排程自動觸發...")

with st.sidebar:
    st.write(f"伺服器時間: `{now.strftime('%H:%M:%S')}`")
    st.write(f"預定排程: `{', '.join(SCHEDULE)}`")    
    st.write(f"目前槽位: `{current_slot}`")
    if st.button("🚨 強制釋放"):
        _, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        GitHubEngine.delete_lock(sha)
        st.rerun()

st.markdown("---")
with st.expander("⚠️ 投資免責聲明 (Disclaimer)"):
    st.caption("""
    1. **本工具僅供技術分析實驗與研究參考**，不構成任何投資建議、買賣邀約或承諾。
    2. 系統顯示之資料來源為第三方 API，資料可能存在延遲、錯誤或缺漏，使用者應自行核實。
    3. 過去的績效不代表未來獲利，投資一定有風險，股票投資有賺有賠，申購前應詳閱公開說明書並審慎評估。
    4. 使用者須對其投資決策負完全責任，本程式開發者不負擔任何法律責任或損失賠償。
    """)
