import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh
import os
# 確保必要的目錄存在
os.makedirs("db", exist_ok=True)
# ==============================
# 0. 台北時間與秒級刷新
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

st_autorefresh(interval=1000, key="sec_refresh")

# ==============================
# 1. 頁面與 GitHub 配置
# ==============================
st.set_page_config(page_title="台股多頭排列最終融合版", layout="wide")

GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")
CACHE_PATH = "scan_cache.json"
HISTORY_PATH = "history.json" # 本地持久化歷史

SCHEDULE_TIMES = ["09:10", "10:20", "11:30", "12:30", "13:20", "14:30", "20:00", "22:00"]

def upload_to_github(content_dict):
    if not GITHUB_TOKEN or not GITHUB_REPO: return
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.get(url, headers=headers)
        sha = res.json().get("sha") if res.status_code==200 else None
        content_b64 = base64.b64encode(json.dumps(content_dict, ensure_ascii=False, indent=2).encode()).decode()
        data = {
            "message": f"🤖 自動更新報告: {now_taipei().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64, "branch": "main"
        }
        if sha: data["sha"]=sha
        requests.put(url, headers=headers, json=data)
    except Exception as e:
        print(f"GitHub Sync Error: {e}")

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH,"r",encoding="utf-8") as f:
                data=json.load(f)
                return pd.DataFrame(data.get("data",[])), data.get("last_update","尚未執行")
        except: pass
    return pd.DataFrame(), "尚未執行"

def save_cache(df, update_time):
    data={"data":df.to_dict(orient="records"), "last_update":update_time}
    with open(CACHE_PATH,"w",encoding="utf-8") as f:
        json.dump(data,f,ensure_ascii=False, indent=2)
    upload_to_github(data)

def update_history(new_df):
    """將新訊號合併至歷史紀錄並去重"""
    if new_df.empty: return
    history = []
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH,"r",encoding="utf-8") as f:
                history = json.load(f)
        except: history = []
    
    combined = pd.concat([pd.DataFrame(history), new_df]).drop_duplicates(subset=['股票代號', '型態'], keep='last')
    with open(HISTORY_PATH,"w",encoding="utf-8") as f:
        json.dump(combined.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

# ==============================
# 2. 技術指標計算 (核心邏輯)
# ==============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5,10,20,60,100,200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"])/df[f"ma{w}"]
    for w in [5,10,20,60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)
    df["pre_close"]=df['Close'].shift(1)
    df["pre_high"]=df['High'].shift(1)
    df["pre_vol"]=df['Volume'].shift(1)
    df["mv20"]=df['Volume'].rolling(20).mean()/1000
    df["RK_p"]=(close - df['Open'])*100/df['Open']
    return df

# ==============================
# 3. 掃描核心函數
# ==============================
def run_scan_logic(stock_codes, status_placeholder=None):
    all_found=[]
    batch_size=50
    total=len(stock_codes)
    if status_placeholder: pbar = st.progress(0)
    
    for i in range(0, total, batch_size):
        batch = stock_codes[i:i+batch_size]
        if status_placeholder: status_placeholder.info(f"⏳ 正在掃描批次 {i//batch_size+1}/{(total+batch_size-1)//batch_size}...")
        try:
            raw = yf.download(tickers=batch, period="300d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
            if raw.empty: continue
            for code in batch:
                try:
                    df = raw[code].dropna() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna()
                    if len(df) < 200: continue
                    ind = calc_indicators(df)
                    last_row = ind.iloc[-1]
                    
                    price, RK_p, vol = round(last_row['Close'],2), round(last_row['RK_p'],1), int(last_row['Volume']/1000)
                    ma = {w: last_row[f'ma{w}'] for w in [5,10,20,60,100,200]}
                    ma_d = {w: last_row[f'ma{w}_d'] for w in [5,10,20,60,100,200]}
                    
                    # 篩選條件
                    if not (1 < RK_p < 7): continue
                    if not (price > last_row['pre_high'] and last_row['mv20'] > 100 and vol > 100 and
                            price > ma[20] > ma[60] > ma[100] > ma[200] and
                            all(v > 0 for v in ma_d.values()) and vol > last_row['pre_vol']/1000 * 1.5): continue
                    
                    # 突破判斷
                    if not any(last_row['pre_close'] < last_row[f'pre_ma{w}'] for w in [5,10,20,60]): continue
                    
                    # 型態標籤
                    res_type = ""
                    mv_vals = list(ma.values())
                    if max(mv_vals)/min(mv_vals) < 1.06 and price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100] > ma[200]: res_type="六線多排"
                    elif price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100]: res_type="五線多排"
                    elif price > ma[5] > ma[10] > ma[20] > ma[60]: res_type="四線多排"
                    elif price > ma[5] > ma[10] > ma[20]: res_type="三線多排"

                    if res_type:
                        all_found.append({
                            "股票代號": code, "價格": price, "漲幅%": RK_p,
                            "成交量": vol, "型態": res_type, "時間": now_taipei().strftime("%H:%M:%S")
                        })
                except: continue
        except: continue
        if status_placeholder: pbar.progress(min((i+batch_size)/total, 1.0))
    
    if status_placeholder:
        status_placeholder.empty()
        pbar.empty()
    return pd.DataFrame(all_found)


# ==============================
# 4. 初始化與快取同步邏輯 (新增從 GitHub 主動拉取)
# ==============================
def sync_from_cloud():
    """主動從 GitHub 抓取最新快取，更新到 Session State 與本地檔案"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            content_b64 = res.json()["content"]
            content_dict = json.loads(base64.b64decode(content_b64).decode('utf-8'))
            
            # 更新本地檔案備份
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(content_dict, f, ensure_ascii=False, indent=2)
            
            # 更新到 Session State
            st.session_state.df_results = pd.DataFrame(content_dict.get("data", []))
            st.session_state.last_update = content_dict.get("last_update", "未知時間")
            return True
    except Exception as e:
        st.sidebar.error(f"同步失敗: {e}")
    return False

# 初始啟動載入
if "df_results" not in st.session_state:
    df_local, last_upd_local = load_cache()
    st.session_state.df_results = df_local
    st.session_state.last_update = last_upd_local

if "last_run_min" not in st.session_state: st.session_state.last_run_min = ""
if "seen_keys" not in st.session_state: st.session_state.seen_keys = set()

# 讀取清單
try:
    with open("db/taiwan_full.json","r",encoding="utf-8") as f:
        stock_list = json.load(f)['stocks']
except: stock_list = ["2330.TW", "2454.TW", "2303.TW"]

# 自動掃描觸發
# ==============================
# 5. 排程觸發掃描 (加入「防重複掃描」優化)
# ==============================
curr_min = now_taipei().strftime("%H:%M")

if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    status_placeholder = st.empty()
    
    # --- 優化點：先嘗試從雲端抓取 ---
    status_placeholder.info("🔍 檢查雲端是否有現成數據...")
    sync_success = sync_from_cloud()
    
    # 檢查同步下來的時間，是否就是今天且就在最近 (例如 5 分鐘內)
    # 如果雲端已經有這一個時段的資料了，我們就直接用，不用再掃描
    last_upd_dt = None
    try:
        last_upd_dt = datetime.strptime(st.session_state.last_update, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
    except: pass

    # 如果雲端數據更新時間與現在差距小於 5 分鐘，視為已有現成結果
    is_recent = last_upd_dt and (now_taipei() - last_upd_dt).total_seconds() < 300
    
    if sync_success and is_recent:
        status_placeholder.success(f"✅ 偵測到雲端已有最新數據 ({curr_min})，已自動同步。")
        time.sleep(2)
        st.rerun()
    else:
        # --- 雲端沒資料或資料太舊，才執行真實掃描 ---
        status_placeholder.warning("☁️ 雲端無最新數據，開始執行本地掃描...")
        new_res = run_scan_logic(stock_list, status_placeholder)
        
        if not new_res.empty:
            new_res["key"] = new_res["股票代號"] + "_" + new_res["型態"]
            filtered = new_res[~new_res["key"].isin(st.session_state.seen_keys)].copy()
            st.session_state.seen_keys.update(new_res["key"].tolist())
            
            display_df = filtered.drop(columns=["key"])
            st.session_state.df_results = display_df
            update_history(display_df)
        else:
            st.session_state.df_results = pd.DataFrame()
            
        st.session_state.last_update = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
        save_cache(st.session_state.df_results, st.session_state.last_update)
        status_placeholder.success(f"✅ {curr_min} 本地掃描並上傳完成！")
        time.sleep(2)
        st.rerun()

# ==============================
# 6. UI 顯示
# ==============================
with st.sidebar:
    st.header("⚙️ 數據管理")
    st.info(f"當前監控: {len(stock_list)} 檔")
    # 增加來源顯示標籤
    source_tag = "🌐 雲端" if "202" in st.session_state.last_update else "💾 本地"
    st.write(f"數據來源: **{source_tag}**")    
    # 修改後的按鈕：改為從快取取得資訊
    if st.button("🔄 同步雲端快取 (GitHub)", help="從 GitHub 取得其他設備掃描好的最新數據"):
        with st.spinner("正在同步雲端數據..."):
            success = sync_from_cloud()
            if success:
                st.toast("✅ 已成功取得 GitHub 最新資訊")
                time.sleep(1)
                st.rerun()
            else:
                st.error("同步失敗，請檢查 GitHub Token 設定")

    if st.button("📂 讀取本地快取", help="強制重新讀取本地 scan_cache.json"):
        df_local, last_upd_local = load_cache()
        st.session_state.df_results = df_local
        st.session_state.last_update = last_upd_local
        st.toast("📁 已載入本地檔案")
        st.rerun()

    st.divider()
    if st.button("🗑️ 清空本日紀錄"):
        if os.path.exists(HISTORY_PATH): os.remove(HISTORY_PATH)
        st.session_state.seen_keys = set()
        st.session_state.df_results = pd.DataFrame()
        st.rerun()

# 顯示本輪結果
st.subheader("🎯 本輪最新訊號")
if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 目前尚無新訊號，等待排程觸發。")

# 顯示歷史回顧
with st.expander("📜 本日累計歷史訊號回顧", expanded=False):
    if os.path.exists(HISTORY_PATH):
        h_df = pd.read_json(HISTORY_PATH)
        if not h_df.empty:
            st.dataframe(h_df.sort_values("時間", ascending=False), use_container_width=True)
        else: st.write("尚無歷史紀錄")
    else: st.write("尚無歷史紀錄")
