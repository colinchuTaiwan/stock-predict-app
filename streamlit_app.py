import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, base64, requests, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 時區與秒級刷新
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

# 每秒刷新一次前端，確保排程檢查精準
st_autorefresh(interval=1000, key="sec_refresh")

# ==============================
# 1. GitHub 雲端核心邏輯
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
CACHE_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")
CACHE_PATH = "scan_cache.json"

def upload_to_github(content_dict, path=CACHE_FILE):
    """通用 GitHub 上傳函數，支援路徑自動切換"""
    if not GITHUB_TOKEN or not GITHUB_REPO: return False
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # 1. 取得舊檔案 SHA
        res = requests.get(url, headers=headers, timeout=5)
        sha = res.json().get("sha") if res.status_code == 200 else None
        
        # 2. 準備內容
        content_json = json.dumps(content_dict, ensure_ascii=False, indent=2)
        # 確保使用 utf-8 編碼
        content_b64 = base64.b64encode(content_json.encode('utf-8')).decode('utf-8')
        
        data = {
            "message": f"🤖 Auto-update: {now_taipei().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch": "main"
        }
        if sha: data["sha"] = sha
        
        # 3. 寫入
        put_res = requests.put(url, headers=headers, json=data, timeout=10)
        return put_res.status_code in [200, 201]
    except Exception as e:
        st.sidebar.error(f"GitHub 上傳失敗 ({path}): {e}")
        return False

def load_cache_from_github():
    """從 GitHub API 讀取最新掃描快取，穩定版"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return pd.DataFrame(), "未知"

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{CACHE_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}

    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            print(f"GitHub 回傳非200: {res.status_code}")
            return pd.DataFrame(), "讀取失敗"

        json_res = res.json()
        content_b64 = json_res.get("content", "")
        if not content_b64:
            return pd.DataFrame(), "空內容"

        # 移除換行符號再解碼
        content_b64 = content_b64.replace("\n", "")
        data = json.loads(base64.b64decode(content_b64).decode('utf-8'))

        return pd.DataFrame(data.get("data", [])), data.get("last_update", "尚未更新")
    except Exception as e:
        print(f"讀取 GitHub 失敗: {e}")
        return pd.DataFrame(), "讀取失敗"

# ==============================
# 2. 技術指標與掃描引擎
# ==============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5,10,20,60,100,200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"])/df[f"ma{w}"]
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    df["mv20"] = df['Volume'].rolling(20).mean()/1000
    df["RK_p"] = (close - df['Open'])*100/df['Open']
    return df

def run_scan_logic(stock_codes, status_placeholder):
    all_found = []
    batch_size = 50
    pbar = st.progress(0)
    
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i+batch_size]
        status_placeholder.info(f"⏳ 正在掃描批次: {i//batch_size + 1}")
        try:
            raw = yf.download(tickers=batch, period="300d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
            for code in batch:
                try:
                    df = raw[code].dropna() if len(batch) > 1 else raw.dropna()
                    if len(df) < 200: continue
                    ind = calc_indicators(df)
                    last = ind.iloc[-1]
                    
                    price = round(last['Close'], 2)
                    rk_p = round(last['RK_p'], 1)
                    vol = int(last['Volume']/1000)
                    
                    # 提取均線數值 (假設 calc_indicators 已算出這些欄位)
                    ma = {w: last[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
                    # 提取乖離/距離指標 (對應你提到的 ma20_b, ma60_b 等)
                    ma_b = {w: last.get(f'ma{w}_b', 0) for w in [20, 60, 100]}

                    # --- 基礎過濾條件 ---
                    # 1. 漲幅在 1%~7% 之間 2. 股價站上所有長線 (20, 60, 100, 200) 3. 有量
                    basic_check = (
                        1 < rk_p < 7 and 
                        price > max(ma[20], ma[60], ma[100], ma[200]) and 
                        vol > 100
                    )

                    if basic_check:
                        res_type = ""
                        
                        # 【五線糾結判斷】 (5, 10, 20, 60, 100)
                        ma5_list = [ma[5], ma[10], ma[20], ma[60], ma[100]]
                        if (max(ma5_list) / min(ma5_list) < 1.08) and ma_b[100] < 0.1:
                            res_type = "五線糾結"
                            
                        # 【四線糾結判斷】 (5, 10, 20, 60)
                        elif (max(ma5_list[:4]) / min(ma5_list[:4]) < 1.08) and ma_b[60] < 0.1:
                            res_type = "四線糾結"
                            
                        # 【三線糾結判斷】 (5, 10, 20)
                        elif (max(ma5_list[:3]) / min(ma5_list[:3]) < 1.08) and ma_b[20] < 0.1:
                            res_type = "三線糾結"

                        if res_type:
                            all_found.append({
                                "股票代號": code, 
                                "價格": price, 
                                "漲幅%": rk_p, 
                                "成交量": vol, 
                                "型態": res_type, 
                                "時間": now_taipei().strftime("%H:%M:%S")
                            })
                except: continue
        except: continue
        pbar.progress(min((i+batch_size)/len(stock_codes), 1.0))
    
    status_placeholder.empty()
    pbar.empty()
    return pd.DataFrame(all_found)

# ==============================
# 3. 排程設定與邏輯控制
# ==============================
SCHEDULE_TIMES = ["09:30", "10:30", "11:20", "12:20", "13:15", "17:23", "20:00", "23:00"]

# 載入代碼
try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        stock_list = json.load(f)['stocks']
except:
    stock_list = ["2330.TW", "2454.TW", "2317.TW"]
    
print(stock_list)
if "df_results" not in st.session_state: st.session_state.df_results = pd.DataFrame()
if "last_update" not in st.session_state: st.session_state.last_update = "尚未掃描"
if "last_run_min" not in st.session_state: st.session_state.last_run_min = ""

# 自動掃描觸發
curr_min = now_taipei().strftime("%H:%M")
if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    status_box = st.empty()
    
    # 優先嘗試從雲端同步（避免多個用戶開啟網頁導致重複掃描）
    df_cloud, time_cloud = load_cache_from_github()
    if time_cloud.startswith(now_taipei().strftime("%Y-%m-%d")):
        st.session_state.df_results = df_cloud
        st.session_state.last_update = time_cloud
        status_box.success("☁️ 已從 GitHub 同步今日最新數據")
    else:
        # 雲端沒資料才掃描
        new_res = run_scan_logic(stock_list, status_box)
        st.session_state.df_results = new_res
        st.session_state.last_update = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
        
        # 存檔至 GitHub (快取 + 歷史)
        cache_data = {"data": new_res.to_dict(orient="records"), "last_update": st.session_state.last_update}
        upload_to_github(cache_data)
        upload_to_github(cache_data, path=f"history/{now_taipei().strftime('%Y-%m-%d')}.json")
    
    time.sleep(2)
    status_box.empty()
    st.rerun()

# ==============================
# 4. 前端介面
# ==============================
st.title("📊 台股多頭排列融合掃描器 v3")

c1, c2, c3 = st.columns(3)
c1.metric("⏰ 台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("📡 最後更新", st.session_state.last_update)
c3.metric("📈 監控數量", len(stock_list))

st.divider()

if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results.sort_values("漲幅%", ascending=False), use_container_width=True)
else:
    st.info("⌛ 目前尚無訊號。系統將在排程時間自動執行掃描。")

with st.sidebar:
    st.header("⚙️ 控制面板")
    
    # --- 手動同步按鈕 ---
    if st.button("☁️ 手動同步雲端", use_container_width=True):
        with st.spinner("正在連線 GitHub..."):
            df_cloud, time_cloud = load_cache_from_github()
            
            # 檢查是否真的讀取成功 (df_cloud 不應為空，且 time_cloud 不應是 "讀取失敗")
            if time_cloud != "讀取失敗" and not df_cloud.empty:
                st.session_state.df_results = df_cloud
                st.session_state.last_update = time_cloud
                st.toast("✅ 雲端同步成功！", icon="🎉")
                time.sleep(1) # 給使用者看一眼彈窗的時間
                st.rerun()
            elif time_cloud != "讀取失敗" and df_cloud.empty:
                st.warning("☁️ 雲端檔案存在，但目前沒有符合條件的股票訊號。")
                st.session_state.last_update = time_cloud
            else:
                st.error("❌ 同步失敗：請檢查 GitHub Token 或 Repository 設定。")
                st.toast("同步失敗，請檢查側邊欄錯誤訊息", icon="⚠️")

    st.divider()
    
    # --- 顯示排程資訊 ---
    st.subheader("📅 自動掃描排程")
    st.caption("系統將在以下台北時間自動執行：")
    # 將排程時間漂亮的顯示出來
    st.code(", ".join(SCHEDULE_TIMES))
    
    st.divider()
    
    # --- 重置按鈕 ---
    if st.button("🗑️ 清空本地快取紀錄", help="清空目前網頁看到的結果，但不影響雲端檔案"):
        st.session_state.seen_keys = set()
        st.session_state.df_results = pd.DataFrame()
        st.session_state.last_update = "已清空"
        st.toast("本地紀錄已清除")
        st.rerun()

    
    
    st.write("📅 排程時間:", SCHEDULE_TIMES)
    st.caption("v3 版：穩定同步與自動避錯機制")
