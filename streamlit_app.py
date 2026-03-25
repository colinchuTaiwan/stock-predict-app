import streamlit as st
import yfinance as yf
import pandas as pd
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
# GitHub 設定
# =============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")

# =============================
# GitHub 上傳
# =============================
def upload_to_github(content_dict):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}

    res = requests.get(url, headers=headers)
    sha = res.json()["sha"] if res.status_code == 200 else None

    content_str = json.dumps(content_dict, ensure_ascii=False, indent=2)
    content_b64 = base64.b64encode(content_str.encode()).decode()

    data = {
        "message": f"update {now_taipei()}",
        "content": content_b64,
        "branch": "main"
    }
    if sha:
        data["sha"] = sha

    res = requests.put(url, headers=headers, json=data)
    if res.status_code not in [200, 201]:
        st.error(f"GitHub 上傳失敗: {res.text}")

# =============================
# cache
# =============================
CACHE_PATH = "scan_cache.json"

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
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

    upload_to_github(data)

# =============================
# 技術指標
# =============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']

    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()

    for w in [5, 10, 20, 60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)

    df["pre_close"] = df['Close'].shift(1)
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)

    df["mv20"] = df['Volume'].rolling(20).mean() / 1000

    for w in [10, 20, 60, 100, 200]:
        df[f"ma{w}_b"] = (close - df[f"ma{w}"]) / df[f"ma{w}"]

    df["RK_p"] = (close - df['Open']) * 100 / df['Open']

    return df

def extract_latest(df, ind):
    if len(df) < 3:
        return None

    d = ind.iloc[-1].to_dict()
    d.update({
        'price': df['Close'].iloc[-1],
        'pre_close': df['Close'].iloc[-2],
        'pre_high': df['High'].iloc[-2],
        'vol': df['Volume'].iloc[-1] / 1000,
        'pre_vol': df['Volume'].iloc[-2] / 1000,
    })
    return d

# =============================
# 分批
# =============================
def chunk_list(lst, size=100):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

# =============================
# 掃描
# =============================
def run_scan_logic(stock_codes):
    st.write(f"下載 {len(stock_codes)} 檔資料中...")

    all_found = []
    pbar = st.progress(0)

    chunks = list(chunk_list(stock_codes, 100))

    for i, chunk in enumerate(chunks):
        raw = yf.download(
            tickers=chunk,
            period="300d",
            group_by="ticker",
            auto_adjust=False,
            threads=True
        )

        for code in chunk:
            try:
                # 修正 single ticker bug
                if isinstance(raw.columns, pd.MultiIndex):
                    df = raw[code].dropna()
                else:
                    df = raw.dropna()

                if len(df) < 200:
                    continue

                ind = calc_indicators(df)
                d = extract_latest(df, ind)
                if not d:
                    continue

                price = round(d['price'], 2)
                pre_high = round(d['pre_high'], 2)
                pre_close = d['pre_close']
                RK_p = round(d['RK_p'], 1)
                stock_cap = int(d['vol'])
                prev_cap = d['pre_vol']
                mv20 = d['mv20']

                ma = {w: d[f'ma{w}'] for w in [5,10,20,60,100,200]}
                ma_d = {w: d[f'ma{w}_d'] for w in [5,10,20,60,100,200]}
                pre_ma = {w: ind[f'ma{w}'].iloc[-2] for w in [5,10,20,60]}

                if not (1 < RK_p < 7):
                    continue

                if not (
                    price > pre_high and mv20 > 100 and stock_cap > 100 and
                    price > ma[20] > ma[60] > ma[100] > ma[200] and
                    all(v > 0 for v in ma_d.values()) and
                    stock_cap > prev_cap * 1.5
                ):
                    continue

                if not any(pre_close < pre_ma[w] for w in [5,10,20,60]):
                    continue

                res_type = ""

                if (max(ma.values())/min(ma.values()) < 1.06) and \
                   (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100] > ma[200]) and d['ma200_b'] < 0.08:
                    res_type = "六線多排"
                elif (price > ma[5] > ma[10] > ma[20] > ma[60] > ma[100]):
                    res_type = "五線多排"
                elif (price > ma[5] > ma[10] > ma[20] > ma[60]):
                    res_type = "四線多排"
                elif (price > ma[5] > ma[10] > ma[20]):
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

            except Exception as e:
                print(code, e)

        pbar.progress((i+1)/len(chunks))

    return pd.DataFrame(all_found)

# =============================
# UI
# =============================
st.set_page_config(page_title="台股掃描", layout="wide")
st.title("🚀 台股多頭排列掃描")

if "df_results" not in st.session_state:
    df_cache, last_update = load_cache()
    st.session_state.df_results = df_cache
    st.session_state.last_update = last_update
    st.session_state.last_run_min = ""

# 時間
c1, c2 = st.columns(2)
c1.metric("台北時間", now_taipei().strftime("%H:%M:%S"))
c2.metric("最後更新", st.session_state.last_update)

# 股票清單
try:
    with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
        stock_list = json.load(f)
        if isinstance(stock_list, dict):
            stock_list = stock_list["stocks"]
except:
    stock_list = ["2330.TW"]

# 手動執行
if st.button("🚀 手動掃描"):
    new_df = run_scan_logic(stock_list)
    now = now_taipei().strftime("%Y-%m-%d %H:%M:%S")

    st.session_state.df_results = new_df
    st.session_state.last_update = now

    save_cache(new_df, now)
    st.rerun()

# 顯示
st.subheader("📊 掃描結果")
if not st.session_state.df_results.empty:
    st.dataframe(st.session_state.df_results, use_container_width=True)
else:
    st.info("尚無結果")
