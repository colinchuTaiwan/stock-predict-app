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
if "last_api_time" not in st.session_state:
    st.session_state.last_api_time = 0

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

# 1秒心跳自動重整 (Streamlit 渲染驅動器)
st_autorefresh(interval=3000, key="v10_heartbeat")

# ==============================
# 1. 核心策略引擎 (向量化判定)
# ==============================
def calc_indicators(df):
    """向量化計算指標，提升執行效率"""
    df = df.copy()
    c = df['Close']
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = c.rolling(w).mean()
        df[f"ma{w}_b"] = (c - df[f"ma{w}"]) / df[f"ma{w}"]
    # RK_p: 實體紅 K 漲幅 (收盤 vs 開盤)
    df["RK_p"] = (c - df['Open']) * 100 / df['Open']
    return df

def analyze_stock_logic(code, df):
    """
    單一標的邏輯判定：執行基礎過濾與 Signal 5-8 判定
    """
    try:
        df = df.dropna()
        if len(df) < 200: return None
        
        # 🕒 日期效驗：確保抓到的是今天的資料 (盤中實測關鍵)
        if df.index[-1].date() < now_taipei().date():
            return None

        ind = calc_indicators(df)
        last = ind.iloc[-1]
        
        p = round(last['Close'], 2)
        rk = round(last['RK_p'], 1)
        vol = int(last['Volume'] / 1000) # 轉換為「張」
        
        ma = {w: last[f'ma{w}'] for w in [5, 10, 20, 60, 100, 200]}
        ma_b = {w: last.get(f'ma{w}_b', 0) for w in [20, 60, 100, 200]}

        # 核心策略過濾 (採用你驗證正確的條件)
        if (1 < rk < 7 and p > max(ma[20], ma[60], ma[100], ma[200]) and vol > 100):
            ma_l = [ma[5], ma[10], ma[20], ma[60], ma[100], ma[200]]
            res_type = ""
            
            # 糾結突破判定
            if (max(ma_l)/min(ma_l) < 1.06) and ma_b[200] < 0.12: res_type = "六線糾結突破"
            elif (max(ma_l[:5])/min(ma_l[:5]) < 1.06) and ma_b[100] < 0.12: res_type = "五線糾結突破"
            elif (max(ma_l[:4])/min(ma_l[:4]) < 1.06) and ma_b[60] < 0.12: res_type = "四線糾結突破"
            elif (max(ma_l[:3])/min(ma_l[:3]) < 1.06) and ma_b[20] < 0.12: res_type = "三線糾結突破"                
            
            if res_type:
                return {
                    "股票代號": code, 
                    "價格": p, 
                    "漲幅%": rk, 
                    "訊號": res_type, 
                    "成交量(張)": vol,
                    "時間": now_taipei().strftime("%H:%M")
                }
    except: pass
    return None

# ==============================
# 2. 狀態管理與排程監控
# ==============================
if "v10" not in st.session_state:
    db = load_persistence()
    st.session_state.v10 = {
        "running": False, "idx": 0, "results": db["list"],
        "last_slot": db["last_slot"]
    }

v = st.session_state.v10
now = now_taipei()
SCHEDULE = ["09:05", "09:35", "10:10", "11:00", "12:00", "13:00", "02:22"]

# 🔒 穩定排程點判定
current_slot_key = ""
for t in SCHEDULE:
    try:
        slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        if abs((now - slot_dt).total_seconds()) <= 60:
            current_slot_key = f"{now.strftime('%m%d')}_{t}"
            break
    except: pass

# 觸發掃描
if current_slot_key and current_slot_key != v["last_slot"] and not v["running"]:
    v["running"], v["idx"], v["results"], v["last_slot"] = True, 0, [], current_slot_key
    st.session_state.active_slot = current_slot_key
    save_persistence(v["last_slot"], v["results"])

# ==============================
# 3. 掃描引擎 (帶 Mutex 防護與 Throttling)
# ==============================
if v["running"] and not st.session_state.lock:
    st.session_state.lock = True
    try:
        universe = get_universe()
        if v["idx"] < len(universe):
            # 節流：確保兩次請求間隔
            if time.time() - st.session_state.yf_lock_time > 2.0:
                batch = universe[v["idx"]: v["idx"] + 10] # 批次增加到 15 支提高效率
                raw = yf.download(batch, period="300d", group_by="ticker", threads=True, progress=False)
                
                for code in batch:
                    try:
                        # 處理單支與多支下載的 Dataframe 結構差異 (Multi-pass Extraction)
                        if len(batch) > 1:
                            df_sub = raw[code].copy()
                        else:
                            df_sub = raw.copy()
                        
                        hit = analyze_stock_logic(code, df_sub)
                        if hit: v["results"].append(hit)
                    except: pass
                
                v["idx"] += len(batch)
                st.session_state.yf_lock_time = time.time()
                # 每處理 60 支存一次檔
                if v["idx"] % 60 == 0: save_persistence(v["last_slot"], v["results"])
        else:
            v["running"] = False
            save_persistence(v["last_slot"], v["results"])
    finally:
        st.session_state.lock = False

# ==============================
# 4. UI 視覺展示
# ==============================
st.title("🛡️ 多頭趨勢選股策略實驗室 v10.1")
st.caption(f"🚀 核心引擎: v9.8 Stable | 當前時區: Taipei (UTC+8)")

c1, c2, c3 = st.columns(3)
slot_label = v["last_slot"].split("_")[-1] if "_" in v["last_slot"] else "等待觸發"
c1.metric("執行時段", slot_label)
c2.metric("符合標的", len(v["results"]))
c3.metric("資料日期", now.strftime("%m/%d"))

if v["running"]:
    u_total = len(get_universe())
    progress = min(v["idx"] / u_total, 1.0) if u_total > 0 else 0
    st.progress(progress)
    st.caption(f"🛰️ 批次掃描中: {v['idx']} / {u_total} (自動避開黑 K 與舊數據)")

if v["results"]:
    df_view = pd.DataFrame(v["results"]).drop_duplicates(subset=["股票代號"], keep="last")
    st.dataframe(
        df_view.sort_values(["訊號", "價格"]), 
        use_container_width=True, 
        hide_index=True,
        column_config={
            "價格": st.column_config.NumberColumn(format="%.2f"),
            "漲幅%": st.column_config.NumberColumn(format="%.1f%%"),
            "成交量(張)": st.column_config.NumberColumn(format="%d")
        }
    )
else:
    st.info("⌛ 監控中。系統將在排程點自動掃描全台股標的。")

with st.sidebar:
    st.header("⚙️ 引擎控制台")
    st.write(f"Mutex Lock: `{st.session_state.lock}`")
    st.write(f"API Throttling: `Active`")
    if st.button("🔴 緊急重置 (Reset System)"):
        v.update({"running": False, "idx": 0, "results": [], "last_slot": ""})
        st.session_state.active_slot = None
        st.session_state.lock = False
        save_persistence("", [])
        st.rerun()
