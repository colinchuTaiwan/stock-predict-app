import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 全域配置與穩定性層
# ==============================
STATE_FILE = "db/scan_results.json"
tz = timezone(timedelta(hours=8))

def now_taipei():
    return datetime.now(tz)

# 🔒 鎖定機制與狀態保護
if "lock" not in st.session_state:
    st.session_state.lock = False
if "active_slot" not in st.session_state:
    st.session_state.active_slot = None
if "yf_lock_time" not in st.session_state:
    st.session_state.yf_lock_time = 0

@st.cache_data(ttl=3600)
def get_universe():
    try:
        if not os.path.exists("db/taiwan_Full.json"): return []
        with open("db/taiwan_Full.json", "r", encoding="utf-8-sig") as f:
            return json.load(f).get("stocks", [])
    except: return []

def load_persistence():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f: return json.load(f)
        except: pass
    return {"last_slot": "", "list": []}

def save_persistence(last_slot, results):
    try:
        os.makedirs("db", exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"last_slot": last_slot, "list": results}, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except: pass

# 5秒心跳自動重整
st_autorefresh(interval=5000, key="v10_4_heartbeat")

# ==============================
# 1. 核心策略引擎 (v10.4 整合版)
# ==============================
def analyze_stock_logic(code, df):
    try:
        # A. 數據清洗與時間效驗
        df = df.dropna()
        if len(df) < 210: return None

        last_row = df.iloc[-1]
        last_date = df.index[-1].date()
        today_date = now_taipei().date()

        # 🚩 修正時差問題：如果最後一筆不是今天，代表 Yahoo 數據還沒更新，不進行判定
        if last_date < today_date:
            return None 

        curr = last_row
        prev = df.iloc[-2] # 真正的昨收

        # B. 數據提取
        price = round(curr["Close"], 2)
        open_ = round(curr["Open"], 2)
        vol = int(curr["Volume"])
        pre_close = round(prev["Close"], 2)
        pre_high = round(prev["High"], 2)

        # C. 指標計算 (MA Cache 優化)
        ma_periods = [5, 10, 20, 60, 100, 200]
        close_series = df["Close"]
        ma_cache = {m: close_series.rolling(m).mean() for m in ma_periods}
        mas = {f"ma{m}": ma_cache[m].iloc[-1] for m in ma_periods}
        pre_mas = {f"ma{m}": ma_cache[m].iloc[-2] for m in ma_periods}

        # D. 量能判定 (今日量 vs 20MA量)
        mv20 = df["Volume"].rolling(20).mean().iloc[-1]
        if mv20 <= 0: return None
        vol_ratio = round(vol / mv20, 2)

        # E. 漲幅與紅 K 判定 (1313 條款)
        if open_ <= 0: return None
        change_p = round((price - pre_close) * 100 / pre_close, 1)

        # 必須是實體紅 K (收盤 > 開盤) 且 漲幅介於 1.5% ~ 8.0%
        if price <= open_ or not (1.5 < change_p < 8.0):
            return None

        # F. 突破與基礎過濾
        cond_basic = (
            price > pre_high and       # 突破昨高
            price > mas["ma5"] and     # 站上 5MA
            vol_ratio > 1.2 and        # 量能增溫
            price < 200                # 價格門檻
        )
        if not cond_basic: return None

        # G. 首日突破確認
        is_breakout = any(pre_close < pre_mas[f"ma{m}"] for m in [5, 10, 20, 60])
        if not is_breakout: return None

        # H. 訊號分類判定
        signal = "None"
        ma_vals = [v for v in mas.values() if v > 0]
        ma_max, ma_min = max(ma_vals), min(ma_vals)
        tangle_ratio = round(ma_max / ma_min, 3)
        above_all = all(price > mas[f"ma{m}"] for m in [20, 60, 100, 200])

        if tangle_ratio < 1.06 and above_all:
            # 糾結突破
            b200 = (price - mas["ma200"]) / mas["ma200"]
            b100 = (price - mas["ma100"]) / mas["ma100"]
            if b200 < 0.08: signal = "Signal 5: 六線糾結突破"
            elif b100 < 0.08: signal = "Signal 6: 五線糾結突破"
            else: signal = "Signal 7: 多線糾結突破"
        elif mas["ma5"] > mas["ma20"] > mas["ma60"] > mas["ma100"] > mas["ma200"]:
            # 多頭排列
            slopes = sum(1 for m in ma_periods if mas[f"ma{m}"] > pre_mas[f"ma{m}"])
            if slopes >= 5: signal = "Signal 1: 五線多排強攻"
            else: signal = "Signal 2: 趨勢多排轉強"

        if signal == "None": return None

        return {
            "股票代號": code,
            "現價": price,
            "漲幅%": change_p,
            "量能倍數": vol_ratio,
            "訊號": signal,
            "糾結度": tangle_ratio,
            "最後更新": last_date.strftime("%Y-%m-%d")
        }
    except: return None

# ==============================
# 2. 狀態管理與排程邏輯
# ==============================
if "v10" not in st.session_state:
    db = load_persistence()
    st.session_state.v10 = {
        "running": False, "idx": 0, "results": db["list"],
        "last_slot": db["last_slot"], "last_api": 0.0
    }

v = st.session_state.v10
now = now_taipei()
SCHEDULE = ["09:05", "10:00", "11:30", "13:00", "01:15"] # 修正掃描時間點

# 自動觸發檢查 (略過重複觸發邏輯...)
# ... (與之前版本相同)

# ==============================
# 3. 掃描引擎 (yf.download)
# ==============================
if v["running"] and not st.session_state.lock:
    st.session_state.lock = True
    try:
        universe = get_universe()
        if v["idx"] < len(universe):
            batch = universe[v["idx"]: v["idx"] + 20]
            # 強制抓取最近 250 天資料確保 MA200 準確
            raw = yf.download(batch, period="250d", group_by="ticker", threads=True, progress=False)
            for code in batch:
                try:
                    df_sub = raw.xs(code, level=0, axis=1) if len(batch) > 1 else raw
                    hit = analyze_stock_logic(code, df_sub)
                    if hit: v["results"].append(hit)
                except: pass
            v["idx"] += len(batch)
            if v["idx"] % 40 == 0: save_persistence(v["last_slot"], v["results"])
        else:
            v["running"] = False
            save_persistence(v["last_slot"], v["results"])
    finally:
        st.session_state.lock = False

# ==============================
# 4. UI 視覺化呈現
# ==============================
st.title("🛡️ 量子哨兵 Quantum Guard v10.4")
st.caption(f"🚀 當前台北時間: {now.strftime('%H:%M:%S')} | 資料狀態: 今日實時校驗已開啟")

if v["results"]:
    df_view = pd.DataFrame(v["results"]).drop_duplicates(subset=["股票代號"], keep="last")
    # 依照訊號與量能倍數排序
    df_view = df_view.sort_values(["訊號", "量能倍數"], ascending=[True, False])
    
    st.dataframe(
        df_view, 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "量能倍數": st.column_config.NumberColumn("量能倍數 🔥", format="%.2f"),
            "漲幅%": st.column_config.NumberColumn("漲幅%", format="%.1f%%"),
            "現價": st.column_config.NumberColumn("現價", format="%.2f")
        }
    )
else:
    st.info("⌛ 掃描中或目前無符合訊號之標的。系統已自動排除黑 K 與非今日數據。")

if st.button("🔴 緊急重置系統"):
    st.session_state.v10.update({"running": False, "idx": 0, "results": []})
    st.rerun()
