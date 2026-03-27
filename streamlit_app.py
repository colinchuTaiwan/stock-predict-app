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

st_autorefresh(interval=5000, key="v10_3_heartbeat")

# ==============================
# 1. 核心策略引擎 (v10.3 紅 K 強化邏輯)
# ==============================


def analyze_stock_logic(code, df):
    try:
        # =========================
        # 0. 資料時間校驗 (核心修正)
        # =========================
        df = df.dropna()
        if len(df) < 210: return None

        last_row = df.iloc[-1]
        last_date = df.index[-1].date()
        today_date = now_taipei().date()

        # 如果最後一筆資料日期小於今天，代表當前 K 線尚未更新
        # 此時我們將最後一筆視為「昨日」，並跳過此股（或在此處實作即時爬蟲）
        if last_date < today_date:
            # 💡 註解：若要在非交易日測試，可暫時註解掉下面這行
            return None 

        curr = last_row
        prev = df.iloc[-2] # 這才是真正的昨收

        # =========================
        # 1. 基礎數據提取
        # =========================
        price = round(curr["Close"], 2)   # 今日現價
        open_ = round(curr["Open"], 2)    # 今日開盤
        vol = int(curr["Volume"])

        pre_close = round(prev["Close"], 2) # 昨收
        pre_high = round(prev["High"], 2)   # 昨高

        # A. MA 計算 (性能優化版)
        ma_periods = [5, 10, 20, 60, 100, 200]
        close_series = df["Close"]
        ma_cache = {m: close_series.rolling(m).mean() for m in ma_periods}
        mas = {f"ma{m}": ma_cache[m].iloc[-1] for m in ma_periods}
        pre_mas = {f"ma{m}": ma_cache[m].iloc[-2] for m in ma_periods}

        # B. 量能判定 (今日量 vs 20日均量)
        mv20 = df["Volume"].rolling(20).mean().iloc[-1]
        if mv20 <= 0: return None
        vol_ratio = round(vol / mv20, 2)

        # C. 漲幅與紅 K 判定 (確保是今天的表現)
        if open_ <= 0: return None
        
        # 實質漲跌幅 (今日收盤 vs 昨收)
        change_p = round((price - pre_close) * 100 / pre_close, 1)

        # 🚩 修正 1313 問題：必須是紅 K 且漲幅達標
        if price <= open_ or not (1.5 < change_p < 8.0):
            return None

        # D. 突破壓力判定
        cond_basic = (
            price > pre_high and       # 突破昨高
            price > mas["ma5"] and     # 站上 5MA
            vol_ratio > 1.2 and        # 量能增溫
            price < 300                # 價格門檻
        )
        if not cond_basic: return None

        # E. 首日突破確認 (昨收在任一短中均線之下)
        is_breakout = any(pre_close < pre_mas[f"ma{m}"] for m in [5, 10, 20, 60])
        if not is_breakout: return None

        # =========================
        # F. 訊號分類 (Signal 1-8)
        # =========================
        signal = "None"
        ma_vals = [v for v in mas.values() if v > 0]
        ma_max, ma_min = max(ma_vals), min(ma_vals)
        tangle_ratio = round(ma_max / ma_min, 3)
        above_all = all(price > mas[f"ma{m}"] for m in [20, 60, 100, 200])

        if tangle_ratio < 1.06 and above_all:
            # 糾結突破模式
            b200 = (price - mas["ma200"]) / mas["ma200"]
            b100 = (price - mas["ma100"]) / mas["ma100"]
            if b200 < 0.08: signal = "Signal 5: 六線糾結突破"
            elif b100 < 0.08: signal = "Signal 6: 五線糾結突破"
            else: signal = "Signal 7: 多線糾結突破"
        elif mas["ma5"] > mas["ma20"] > mas["ma60"] > mas["ma100"] > mas["ma200"]:
            # 多頭排列模式
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
            "最後更新": last_date.strftime("%Y-%m-%d") # 顯示日期供覆盤檢查
        }
    except Exception as e:
        return None


# ==============================
# 2. 狀態管理與排程監控
# ==============================
if "v10" not in st.session_state:
    db = load_persistence()
    st.session_state.v10 = {
        "running": False, "idx": 0, "results": db["list"],
        "last_slot": db["last_slot"], "last_api": 0.0
    }

v = st.session_state.v10
now = now_taipei()
SCHEDULE = ["08:40", "9:30", "10:50", "12:20", "13:15", "15:00", "00:50"]

# 🔒 穩定排程點判定
current_slot_key = ""
for t in SCHEDULE:
    try:
        slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        if abs((now - slot_dt).total_seconds()) <= 60:
            current_slot_key = f"{now.strftime('%m%d')}_{t}"
            break
    except: pass

# 觸發掃描條件
if current_slot_key and current_slot_key != v["last_slot"] and st.session_state.active_slot != current_slot_key and not v["running"]:
    v["running"], v["idx"], v["results"], v["last_slot"] = True, 0, [], current_slot_key
    st.session_state.active_slot = current_slot_key
    save_persistence(v["last_slot"], v["results"])

# ==============================
# 3. 掃描引擎 (Mutex 防護)
# ==============================
if v["running"] and not st.session_state.lock:
    st.session_state.lock = True
    try:
        universe = get_universe()
        u_len = len(universe)
        if u_len > 0 and v["idx"] < u_len:
            if time.time() - st.session_state.yf_lock_time > 3.5:
                batch = universe[v["idx"]: v["idx"] + 20]
                raw = yf.download(batch, period="250d", group_by="ticker", threads=True, progress=False)
                
                for code in batch:
                    try:
                        try: df_sub = raw.xs(code, level=0, axis=1)
                        except: df_sub = raw[code] if code in raw else raw
                        
                        hit = analyze_stock_logic(code, df_sub)
                        if hit: v["results"].append(hit)
                    except: pass
                
                v["idx"] += len(batch)
                st.session_state.yf_lock_time = time.time()
                if v["idx"] % 40 == 0: save_persistence(v["last_slot"], v["results"])
        else:
            v["running"] = False
            save_persistence(v["last_slot"], v["results"])
    finally:
        st.session_state.lock = False

# ==============================
# 4. UI 視覺展示
# ==============================
st.title("🛡️ 多頭趨勢選股策略實驗室 v10.1")

# 顯示排程資訊與最後更新
st.code("排程點: " + ", ".join(SCHEDULE))
last_update_time = now.strftime("%Y-%m-%d %H:%M:%S")
st.caption(f"📊 系統最後檢查時間: {last_update_time}")

c1, c2 = st.columns(2)
slot_label = v["last_slot"].split("_")[-1] if "_" in v["last_slot"] else "等待觸發"
c1.metric("當前執行時段", slot_label)
c2.metric("符合標的數", len(v["results"]))

universe_total = len(get_universe())
if v["running"] and universe_total > 0:
    st.progress(min(v["idx"] / universe_total, 1.0))
    st.caption(f"🚀 深度掃描中: {v['idx']} / {universe_total}")

if v["results"]:
    df_view = pd.DataFrame(v["results"]).drop_duplicates(subset=["股票代號"], keep="last")
    st.dataframe(df_view.sort_values(["訊號", "股票代號"]), use_container_width=True, hide_index=True)
else:
    st.info("⌛ 盤中監控中，符合 Signal 1-8 之標的將即時推播於此。")

with st.expander("🛠️ 引擎診斷"):
    st.write(f"執行狀態: {v['running']}")
    if st.button("🔴 強制重置 (Reset All)"):
        v.update({"running": False, "idx": 0, "results": [], "last_slot": ""})
        st.session_state.active_slot = None
        st.session_state.lock = False
        save_persistence("", [])
        st.rerun()
