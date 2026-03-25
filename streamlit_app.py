import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import time
import base64
import requests

from datetime import datetime, timedelta, timezone

# =============================
# 台北時間
# =============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

# =============================
# GitHub 設定（從 secrets 讀）
# =============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")

# =============================
# GitHub 上傳
# =============================
def upload_to_github(content_dict):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        print("GitHub 未設定")
        return

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
    
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}"
    }

    # 先取得 SHA（如果檔案存在）
    res = requests.get(url, headers=headers)
    sha = None
    if res.status_code == 200:
        sha = res.json()["sha"]

    content_str = json.dumps(content_dict, ensure_ascii=False, indent=2)
    content_b64 = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")

    data = {
        "message": f"update scan result {now_taipei().strftime('%Y-%m-%d %H:%M:%S')}",
        "content": content_b64,
        "branch": "main"
    }

    if sha:
        data["sha"] = sha

    requests.put(url, headers=headers, json=data)

# =============================
# 本地 cache
# =============================
CACHE_PATH = "scan_cache.json"

def load_cache():
    try:
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return pd.DataFrame(data.get("data", [])), data.get("last_update", "尚未執行")
    except:
        pass
    return pd.DataFrame(), "尚未執行"

def save_cache(df, update_time):
    data = {
        "data": df.to_dict(orient="records"),
        "last_update": update_time
    }

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # 🔥 同步到 GitHub
    upload_to_github(data)

# =============================
# Streamlit UI
# =============================
st.set_page_config(page_title="台股多頭排列掃描", layout="wide")

SCHEDULE_TIMES = [
    "07:00","09:20","10:20","11:20","12:20",
    "13:20","15:00","18:00","22:30","23:30"
]

st.title("🚀 台股多頭排列掃描器 + GitHub雲端版")

# 初始化
if "df_results" not in st.session_state:
    df_cache, last_update_cache = load_cache()
    st.session_state.df_results = df_cache
    st.session_state.last_update = last_update_cache
    st.session_state.last_run_min = ""

# 顯示時間
c1, c2 = st.columns(2)
c1.metric("台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("最後更新時間", st.session_state.last_update)

# 股票清單
json_path = os.path.join('db', 'taiwan_full.json')

st.write("📂 嘗試讀取:", json_path)
st.write("📂 當前目錄:", os.getcwd())
st.write("📂 目錄內容:", os.listdir("."))

try:
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
        # 判斷格式
        if isinstance(data, dict) and "stocks" in data:
            stock_list = data["stocks"]
        elif isinstance(data, list):
            stock_list = data
        else:
            raise ValueError("JSON 格式錯誤")

    st.success(f"✅ 成功載入股票數: {len(stock_list)}")

except Exception as e:
    st.error(f"❌ 讀取失敗: {e}")
    stock_list = ["2330.TW", "2303.TW", "2454.TW"]

# 側邊
with st.sidebar:
    st.header("監控")
    st.write(f"股票數: {len(stock_list)}")
    st.write("排程:", SCHEDULE_TIMES)
    if st.button("🚀 手動執行"):
        st.session_state.last_run_min = "manual"

# =============================
# 掃描邏輯（你原本）
# =============================
def run_scan_logic(stock_codes):
    st.write(f"正在下載 {len(stock_codes)} 檔股票數據...")
    raw = yf.download(tickers=stock_codes, period="300d", group_by="ticker", auto_adjust=False, threads=True)
    
    all_found = []
    pbar = st.progress(0)

    for i, code in enumerate(stock_codes):
        pbar.progress((i + 1) / len(stock_codes))
        try:
            df = raw[code].dropna() if len(stock_codes) > 1 else raw.dropna()
            if len(df) < 200: continue

            ind = calc_indicators(df)
            d = extract_latest(df, ind)
            if not d: continue

            # 提取變數
            price = round(d['price'], 2)
            pre_high = round(d['pre_high'], 2)
            pre_close = d['pre_close']
            RK_p = round(d['RK_p'], 1)
            stock_cap = int(d['vol'])
            prev_cap = d['pre_vol']
            mv20 = d['mv20']
            
            # 均線數值
            ma = {w: d[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
            ma_d = {w: d[f'ma{w}_d'] for w in [5, 10, 20, 60, 100, 200]}
            pre_ma = {w: ind[f'ma{w}'].iloc[-2] for w in [5, 10, 20, 60]}

            # --- 基本過濾條件 ---
            if not (1 < RK_p < 7): continue
            
            cond_basic = (
                price > pre_high and mv20 > 100 and stock_cap > 100 and
                price > ma[20] and price > ma[60] and price > ma[100] and price > ma[200] and
                all(v > 0 for v in ma_d.values()) and
                stock_cap > prev_cap * 1.5
            )
            if not cond_basic: continue

            is_breakout = any(pre_close < pre_ma[w] for w in [5, 10, 20, 60])
            if not is_breakout: continue

            # --- 訊號分類判斷 ---
            res_type = ""
            # Signal 1: 六線
            if (max(ma.values())/min(ma.values()) < 1.06) and \
               (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100] > ma[200]) and (d['ma200_b'] < 0.08):
                res_type = "六線多排"
            # Signal 2: 五線
            elif (max(list(ma.values())[:-1])/min(list(ma.values())[:-1]) < 1.06) and \
                 (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100]) and (d['ma100_b'] < 0.08):
                res_type = "五線多排"
            # Signal 3: 四線
            elif (max(list(ma.values())[:4])/min(list(ma.values())[:4]) < 1.06) and \
                 (price > ma[5] > ma[10] > ma[20] > ma[60]) and (d['ma60_b'] < 0.08):
                res_type = "四線多排"
            # Signal 4: 三線
            elif (max(list(ma.values())[:3])/min(list(ma.values())[:3]) < 1.06) and \
                 (price > ma[5] > ma[10] > ma[20]) and (d['ma20_b'] < 0.08):
                res_type = "三線多排"
            
            if res_type:
                all_found.append({
                    "股票代號": code,
                    "價格": price,
                    "漲幅%": RK_p,
                    "成交量": stock_cap,
                    "型態": res_type,
                    "更新時間": now_taipei().strftime("%H:%M:%S")
                })
        except:
            continue
    return pd.DataFrame(all_found)


# =============================
# 觸發
# =============================
curr_min = now_taipei().strftime("%H:%M")
should_trigger = (curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min)
is_manual = (st.session_state.last_run_min == "manual")

if should_trigger or is_manual:
    st.session_state.last_run_min = curr_min

    new_df = run_scan_logic(stock_list)

    update_time = now_taipei().strftime("%Y-%m-%d %H:%M:%S")

    st.session_state.df_results = new_df
    st.session_state.last_update = update_time

    save_cache(new_df, update_time)

    st.rerun()

# 顯示
st.subheader("📊 掃描結果")
if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results, use_container_width=True)
else:
    st.info("尚無結果")

# 自動刷新
time.sleep(60)
st.rerun()
