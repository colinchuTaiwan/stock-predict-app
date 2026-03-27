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
st_autorefresh(interval=2000, key="v10_1_heartbeat") #原先5000 改2000

# ==============================
# 1. 核心策略引擎 (策略邏輯更新)
# ==============================
def analyze_stock_logic(code, df):
    try:
        # =========================
        # A. 數據清洗與預檢
        # =========================
        df = df.dropna()
        if len(df) < 210:
            return None

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        price = round(curr["Close"], 2)
        open_ = round(curr["Open"], 2)
        vol = int(curr["Volume"])

        pre_close = round(prev["Close"], 2)
        pre_high = round(prev["High"], 2)
        pre_vol = int(prev["Volume"]) / 1000

        # =========================
        # B. MA（性能優化：避免重複 rolling）
        # =========================
        ma_periods = [5, 10, 20, 60, 100, 200]
        close = df["Close"]

        ma_cache = {m: close.rolling(m).mean() for m in ma_periods}

        mas = {f"ma{m}": ma_cache[m].iloc[-1] for m in ma_periods}
        pre_mas = {f"ma{m}": ma_cache[m].iloc[-2] for m in ma_periods}

        # =========================
        # C. 成交量（改為相對量能）
        # =========================
        mv20 = df["Volume"].rolling(20).mean().iloc[-1]

        if mv20 == 0 or pd.isna(mv20):
            return None

        vol_spike = vol > mv20 * 1.2  # ✔ 替代 static 100

        # =========================
        # D. 漲幅與乖離
        # =========================
        if open_ == 0 or pd.isna(open_):
            return None

        rk_p = round((price - open_) * 100 / open_, 1)

        # =========================
        # E. 基礎過濾
        # =========================
        if not (1.5 < rk_p < 7.0):
            return None

        cond_basic = (
            price > pre_high and
            price > mas["ma5"] and
            vol_spike and
            price < 200
        )

        if not cond_basic:
            return None

        # =========================
        # F. 突破確認（首日突破）
        # =========================
        is_breakout = any(
            pre_close < pre_mas[f"ma{m}"] for m in [5, 10, 20, 60]
        )

        if not is_breakout:
            return None

        # =========================
        # G. 訊號分類
        # =========================
        signal = "None"

        ma_vals = [v for v in mas.values() if not pd.isna(v)]
        if not ma_vals:
            return None

        ma_max = max(ma_vals)
        ma_min = min(ma_vals)

        if ma_min <= 0 or pd.isna(ma_min):
            return None

        tangle_ratio = ma_max / ma_min
        above_all = all(
            price > mas[f"ma{m}"] for m in [20, 60, 100, 200]
        )

        # =========================
        # 1. 糾結突破
        # =========================
        if tangle_ratio < 1.06 and above_all:

            bias_200 = (price - mas["ma200"]) / mas["ma200"]
            bias_100 = (price - mas["ma100"]) / mas["ma100"]
            bias_60 = (price - mas["ma60"]) / mas["ma60"]

            # ✔ 穩定化 bias threshold（避免過度敏感）
            if bias_200 < 0.08:
                signal = "Signal 5: 六線糾結突破"
            elif bias_100 < 0.08:
                signal = "Signal 6: 五線糾結突破"
            elif bias_60 < 0.08:
                signal = "Signal 7: 四線糾結突破"
            else:
                signal = "Signal 8: 三線糾結突破"

        # =========================
        # 2. 多頭排列
        # =========================
        elif (
            mas["ma5"] > mas["ma20"] >
            mas["ma60"] > mas["ma100"] >
            mas["ma200"]
        ):

            slopes = sum(
                1 for m in ma_periods
                if mas[f"ma{m}"] > pre_mas[f"ma{m}"]
            )

            if slopes >= 5:
                signal = "Signal 1: 五線多排"
            elif slopes == 4:
                signal = "Signal 2: 四線多排"
            elif slopes == 3:
                signal = "Signal 3: 三線多排"
            else:
                signal = "Signal 4: 二線多排"

        if signal == "None":
            return None

        # =========================
        # H. return
        # =========================
        return {
            "股票代號": code,
            "價格": price,
            "漲幅%": rk_p,
            "訊號": signal,
            "糾結度": round(tangle_ratio, 3),
            "時間": now_taipei().strftime("%H:%M")
        }

    except Exception:
        return None

# ==============================
# 2. 狀態管理與排程
# ==============================
if "v10" not in st.session_state:
    db = load_persistence()
    st.session_state.v10 = {
        "running": False, "idx": 0, "results": db["list"],
        "last_slot": db["last_slot"], "last_api": 0.0
    }

v = st.session_state.v10
now = now_taipei()
# 修正排程格式為 HH:MM
SCHEDULE = ["08:40", "09:30", "10:50", "12:20", "13:15", "15:00", "00:24"]

current_slot_key = ""
for t in SCHEDULE:
    try:
        slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        if abs((now - slot_dt).total_seconds()) <= 60:
            current_slot_key = f"{now.strftime('%m%d')}_{t}"
            break
    except: pass

if current_slot_key and current_slot_key != v["last_slot"] and st.session_state.active_slot != current_slot_key and not v["running"]:
    v["running"], v["idx"], v["results"], v["last_slot"] = True, 0, [], current_slot_key
    st.session_state.active_slot = current_slot_key
    save_persistence(v["last_slot"], v["results"])

# ==============================
# 3. 掃描引擎
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
                        # 穩定的數據提取路由
                        if isinstance(raw.columns, pd.MultiIndex):
                            df_sub = raw.xs(code, level=0, axis=1)
                        else:
                            df_sub = raw
                        
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
# 4. UI 介面 (量子哨兵風格)
# ==============================
st.title("🛡️ 量子哨兵：多頭趨勢選股實驗室 v10.1")

# 排程與檢查點
st.code("巡邏排程: " + " | ".join(SCHEDULE))
last_heartbeat = now.strftime("%Y-%m-%d %H:%M:%S")
st.caption(f"💓 系統心跳正常: {last_heartbeat}")

c1, c2 = st.columns(2)
slot_label = v["last_slot"].split("_")[-1] if "_" in v["last_slot"] else "待命守護中"
c1.metric("當前巡邏時段", slot_label)
c2.metric("已獲取訊號", len(v["results"]))

u_total = len(get_universe())
if v["running"] and u_total > 0:
    progress_val = min(v["idx"] / u_total, 1.0)
    st.progress(progress_val)
    st.caption(f"🔎 深度掃描進度: {int(progress_val*100)}% ({v['idx']}/{u_total})")

if v["results"]:
    # 確保 UI 顯示最新且不重複
    df_view = pd.DataFrame(v["results"]).drop_duplicates(subset=["股票代號"], keep="last")
    st.dataframe(
        df_view.sort_values(["訊號", "股票代號"]), 
        use_container_width=True, 
        hide_index=True,
        height=500
    )
else:
    st.info("⌛ 目前市場波動平穩，尚未偵測到符合 Signal 1-8 突破模式的標的。")

with st.expander("🛠️ 哨兵核心診斷"):
    st.write(f"引擎狀態: {'🔥 掃描中' if v['running'] else '💤 待機'}")
    st.write(f"Slot Key: `{v['last_slot']}`")
    if st.button("🔴 重置系統並清空紀錄"):
        v.update({"running": False, "idx": 0, "results": [], "last_slot": ""})
        st.session_state.active_slot = None
        st.session_state.lock = False
        save_persistence("", [])
        st.rerun()
