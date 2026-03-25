import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 環境設定
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

st_autorefresh(interval=1000, key="sec_refresh")

GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")
CACHE_PATH = "scan_cache.json"
HISTORY_PATH = "history.json"
SCHEDULE_TIMES = ["09:10", "10:20", "11:30", "12:30", "13:20", "14:30", "20:00"]

# ==============================
# 1. 核心同步功能
# ==============================
def sync_from_cloud():
    """從 GitHub 抓取最新快取，同步至本地與 Session State"""
    if not GITHUB_TOKEN or not GITHUB_REPO: return False
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            content_dict = json.loads(base64.b64decode(res.json()["content"]).decode('utf-8'))
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(content_dict, f, ensure_ascii=False, indent=2)
            st.session_state.df_results = pd.DataFrame(content_dict.get("data", []))
            st.session_state.last_update = content_dict.get("last_update", "未知時間")
            return True
    except: pass
    return False

def save_cache(df, update_time):
    """保存數據至本地並推送到 GitHub"""
    data = {"data": df.to_dict(orient="records"), "last_update": update_time}
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # 執行 GitHub 上傳
    if GITHUB_TOKEN and GITHUB_REPO:
        try:
            url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
            headers = {"Authorization": f"token {GITHUB_TOKEN}"}
            res = requests.get(url, headers=headers)
            sha = res.json().get("sha") if res.status_code == 200 else None
            content_b64 = base64.b64encode(json.dumps(data).encode()).decode()
            payload = {"message": f"Update {update_time}", "content": content_b64, "branch": "main"}
            if sha: payload["sha"] = sha
            requests.put(url, headers=headers, json=payload)
        except: pass

# ==============================
# 2. 初始化 Session State
# ==============================
if "df_results" not in st.session_state:
    if not sync_from_cloud(): # 啟動先試雲端同步
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH,"r") as f:
                d = json.load(f)
                st.session_state.df_results = pd.DataFrame(d.get("data", []))
                st.session_state.last_update = d.get("last_update", "尚未掃描")
        else:
            st.session_state.df_results = pd.DataFrame()
            st.session_state.last_update = "尚未掃描"

if "last_run_min" not in st.session_state: st.session_state.last_run_min = ""
if "seen_keys" not in st.session_state: st.session_state.seen_keys = set()

# 載入股票清單
try:
    with open("db/taiwan_Full.json","r",encoding="utf-8") as f:
        stock_list = json.load(f)['stocks']
except: stock_list = ["2330.TW", "2454.TW", "2303.TW"]

# ==============================
# 3. 排程與掃描邏輯
# ==============================
curr_min = now_taipei().strftime("%H:%M")

if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    status = st.empty()
    
    # 智慧防重掃描
    status.info("🔍 檢查雲端數據...")
    sync_success = sync_from_cloud()
    last_upd_dt = None
    try: last_upd_dt = datetime.strptime(st.session_state.last_update, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
    except: pass
    
    # 如果雲端數據是 5 分鐘內更新的，直接採用
    if sync_success and last_upd_dt and (now_taipei() - last_upd_dt).total_seconds() < 300:
        status.success(f"✅ 取得雲端現成數據 ({curr_min})")
        time.sleep(2)
        st.rerun()
    else:
        status.warning("🚀 執行本地全掃描...")
        # 呼叫您先前的 run_scan_logic (此處省略定義以節省長度)
        new_res = run_scan_logic(stock_list, status) 
        
        if not new_res.empty:
            new_res["key"] = new_res["股票代號"] + "_" + new_res["型態"]
            filtered = new_res[~new_res["key"].isin(st.session_state.seen_keys)].copy()
            st.session_state.seen_keys.update(new_res["key"].tolist())
            st.session_state.df_results = filtered.drop(columns=["key"])
            # update_history(st.session_state.df_results) # 您的歷史存檔函式
        
        st.session_state.last_update = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
        save_cache(st.session_state.df_results, st.session_state.last_update)
        status.success("✅ 掃描完成並同步至雲端")
        time.sleep(2)
        st.rerun()

# ==============================
# 4. UI 介面
# ==============================
st.title("🚀 台股多頭排列融合監控")

c1, c2, c3 = st.columns(3)
c1.metric("⏰ 系統時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("📡 數據更新時間", st.session_state.last_update)
c3.metric("📊 訊號數量", len(st.session_state.df_results))

st.divider()

with st.sidebar:
    st.header("⚙️ 數據管理")
    if st.button("🔄 同步雲端快取 (GitHub)"):
        if sync_from_cloud(): st.toast("✅ 同步成功"); time.sleep(1); st.rerun()
        else: st.error("同步失敗")
        
    if st.button("📂 讀取本地快取"):
        st.toast("📁 已載入本地檔案"); time.sleep(1); st.rerun()

    st.divider()
    st.write("📅 排程時間:", ", ".join(SCHEDULE_TIMES))
    if st.button("🗑️ 清空本日紀錄"):
        st.session_state.seen_keys = set()
        st.session_state.df_results = pd.DataFrame()
        st.rerun()

# 顯示結果表格
if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 暫無訊號，等待排程掃描或手動同步。")
