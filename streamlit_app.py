
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
    """🔒 原子化寫入，防止 JSON 損壞"""
    try:
        os.makedirs("db", exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"last_slot": last_slot, "list": results}, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except: pass

# 5秒心跳自動重整
st_autorefresh(interval=5000, key="v10_heartbeat")

# ==============================
# 1. 核心策略引擎 (策略邏輯更新)
# ==============================
def analyze_stock_logic(code, df):
    """
    核心策略引擎：執行基礎過濾與 Signal 1-8 判定
    """
    try:
        # A. 數據清洗與基本檢查
        df = df.dropna()
        if len(df) < 210: return None
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        price = round(curr['Close'], 2)
        open_ = round(curr['Open'], 2)
        vol = int(curr['Volume'])
        
        pre_close = round(prev['Close'], 2)
        pre_high = round(prev['High'], 2)
        pre_vol = int(prev['Volume']) / 1000 
        
        # B. 均線與指標計算
        ma_periods = [5, 10, 20, 60, 100, 200]
        mas = {f"ma{m}": df['Close'].rolling(m).mean().iloc[-1] for m in ma_periods}
        pre_mas = {f"ma{m}": df['Close'].rolling(m).mean().iloc[-2] for m in ma_periods}
        mv20 = df['Volume'].rolling(20).mean().iloc[-1]
        
        # C. 漲幅與乖離
        rk_p = round((price - open_) * 100 / open_, 1)
        bias = {f"ma{m}_b": (price - mas[f"ma{m}"]) / mas[f"ma{m}"] for m in ma_periods}
        
        # D. 基礎門檻過濾 (1.5% < RK_p < 7.0%)
        if not (1.5 < rk_p < 7.0): return None
        
        cond_basic = (
            price > pre_high and price > mas['ma5'] and 
            mv20 > 100 and vol > 100 and price < 200 and
            vol > (pre_vol * 1.5)
        )
        if not cond_basic: return None
        
        # E. 突破確認 (昨日收盤需在任一短中均線之下)
        is_breakout_trigger = any(pre_close < pre_mas[f"ma{m}"] for m in [5, 10, 20, 60])
        if not is_breakout_trigger: return None

        # F. 進階訊號判定 (Signal 1-8)
        signal = "None"
        ma_vals = list(mas.values())
        
        # 1. 糾結模式判定
        tangle_ratio = max(ma_vals) / min(ma_vals)
        above_all = all(price > mas[f"ma{m}"] for m in [20, 60, 100, 200])
        
        if tangle_ratio < 1.06 and above_all:
            if bias['ma200_b'] < 0.1: signal = "Signal 5: 六線糾結突破"
            elif bias['ma100_b'] < 0.1: signal = "Signal 6: 五線糾結突破"
            elif bias['ma60_b'] < 0.1: signal = "Signal 7: 四線糾結突破"
            else: signal = "Signal 8: 三線糾結突破"
            
        # 2. 多頭排列模式
        elif mas['ma5'] > mas['ma20'] > mas['ma60'] > mas['ma100'] > mas['ma200']:
            slopes = sum(1 for m in ma_periods if mas[f"ma{m}"] > pre_mas[f"ma{m}"])
            if slopes >= 5: signal = "Signal 1: 五線多排"
            elif slopes == 4: signal = "Signal 2: 四線多排"
            elif slopes == 3: signal = "Signal 3: 三線多排"
            else: signal = "Signal 4: 二線多排"

        if signal == "None": return None

        return {
            "股票代號": code, "價格": price, "漲幅%": rk_p, 
            "訊號": signal, "糾結度": round(tangle_ratio, 3), 
            "時間": now_taipei().strftime("%H:%M")
        }
    except: return None

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
SCHEDULE = ["08:40", "9:30", "10:50", "12:20", "13:15", "15:00", "00:04"]

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
st.title("🛡️ 多頭趨勢選股策略實驗室 v10.0")

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
