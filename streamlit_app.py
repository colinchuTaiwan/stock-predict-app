import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 全域設定
# ==============================
STATE_FILE = "db/scan_results.json"
tz = timezone(timedelta(hours=8))

def now_taipei():
    return datetime.now(tz)

# Session 狀態初始化
st.session_state.setdefault("lock", False)
st.session_state.setdefault("active_slot", None)
st.session_state.setdefault("yf_lock_time", 0)

# ==============================
# 1. 股票池
# ==============================
@st.cache_data(ttl=3600)
def get_universe():
    try:
        if not os.path.exists("db/taiwan_Full.json"):
            return []
        with open("db/taiwan_Full.json", "r", encoding="utf-8-sig") as f:
            return json.load(f).get("stocks", [])
    except:
        return []

# ==============================
# 2. JSON 永續（Atomic）
# ==============================
def load_persistence():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {"last_slot": "", "list": []}

def save_persistence(last_slot, results):
    try:
        os.makedirs("db", exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"last_slot": last_slot, "list": results}, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except:
        pass

# ==============================
# 3. 指標計算
# ==============================
def calc_indicators(df):
    df = df.copy()
    c = df['Close']

    for w in [5, 10, 20, 60, 100, 200]:
        df[f"ma{w}"] = c.rolling(w).mean()
        df[f"ma{w}_b"] = (c - df[f"ma{w}"]) / df[f"ma{w}"]

    df["RK_p"] = (c - df['Open']) * 100 / df['Open']
    return df

# ==============================
# 4. 單股策略引擎（核心修正）
# ==============================
def analyze_stock_logic(code, df):
    try:
        df = df.dropna()
        if len(df) < 210:
            return None

        ind = calc_indicators(df)

        last = ind.iloc[-1]
        prev = ind.iloc[-2]

        # ==============================
        # 1️⃣ 基本數據
        # ==============================
        price = last['Close']
        open_ = last['Open']
        vol = last['Volume'] / 1000   # 張

        pre_close = prev['Close']
        pre_high = prev['High']
        pre_vol = prev['Volume'] / 1000

        rk = (price - open_) * 100 / open_

        # ==============================
        # 2️⃣ 均線
        # ==============================
        ma = {w: last[f"ma{w}"] for w in [5, 10, 20, 60, 100, 200]}
        pre_ma = {w: prev[f"ma{w}"] for w in [5, 10, 20, 60, 100, 200]}

        ma_b = {w: last.get(f"ma{w}_b", 0) for w in [20, 60, 100, 200]}

        # 均線斜率（方向）
        ma_d = {w: ma[w] - pre_ma[w] for w in ma}

        mv20 = df['Volume'].rolling(20).mean().iloc[-1] / 1000

        # ==============================
        # 3️⃣ 第一層：紅K過濾
        # ==============================
        if not (1 < rk < 7):
            return None

        # ==============================
        # 4️⃣ 第二層：基礎條件（🔥關鍵）
        # ==============================
        cond_basic = (
            (price > pre_high and price > ma[5]) and
            (mv20 > 100 and vol > 100) and
            (price < 200) and
            (vol > pre_vol * 1.5)
        )

        if not cond_basic:
            return None

        # ==============================
        # 5️⃣ 第三層：突破觸發（🔥關鍵）
        # ==============================
        is_breakout = (
            pre_close < pre_ma[5] or
            pre_close < pre_ma[10] or
            pre_close < pre_ma[20] or
            pre_close < pre_ma[60]
        )

        if not is_breakout:
            return None

        # ==============================
        # 6️⃣ Signal 判定
        # ==============================
        signal = None

        # ===== 多頭排列 =====
        trend_base = (ma[5] > ma[20] > ma[60] > ma[100] > ma[200])

        if trend_base:
            up_count = sum(1 for w in [5,20,60,100,200] if ma_d[w] > 0)

            if up_count >= 5:
                signal = "Signal 1: 五線多排"
            elif up_count == 4:
                signal = "Signal 2: 四線多排"
            elif up_count == 3:
                signal = "Signal 3: 三線多排"
            elif up_count == 2:
                signal = "Signal 4: 二線多排"

        # ===== 糾結模式 =====
        ma_list = [ma[5], ma[10], ma[20], ma[60], ma[100], ma[200]]

        if min(ma_list) == 0:
            return None

        above_all = all(price > ma[w] for w in [20,60,100,200])

        if above_all:
            if (max(ma_list)/min(ma_list) < 1.08) and ma_b[200] < 0.1:
                signal = "Signal 5: 六線糾結"
            elif (max(ma_list[:5])/min(ma_list[:5]) < 1.08) and ma_b[100] < 0.1:
                signal = "Signal 6: 五線糾結"
            elif (max(ma_list[:4])/min(ma_list[:4]) < 1.08) and ma_b[60] < 0.1:
                signal = "Signal 7: 四線糾結"
            elif (max(ma_list[:3])/min(ma_list[:3]) < 1.08) and ma_b[20] < 0.1:
                signal = "Signal 8: 三線糾結"

        if not signal:
            return None

        # ==============================
        # 7️⃣ 回傳
        # ==============================
        return {
            "股票代號": code,
            "價格": round(price, 2),
            "漲幅%": round(rk, 1),
            "成交量": int(vol),
            "訊號": signal,
            "時間": now_taipei().strftime("%H:%M")
        }

    except:
        return None

# ==============================
# 5. 初始化狀態
# ==============================
if "v10" not in st.session_state:
    db = load_persistence()
    st.session_state.v10 = {
        "running": False,
        "idx": 0,
        "results": db["list"],
        "last_slot": db["last_slot"]
    }

v = st.session_state.v10
now = now_taipei()

# ==============================
# 6. 排程觸發
# ==============================
SCHEDULE = ["08:40", "09:30", "10:50", "12:20", "13:15", "02:39"]

current_slot_key = ""
for t in SCHEDULE:
    slot_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if abs((now - slot_dt).total_seconds()) <= 60:
        current_slot_key = f"{now.strftime('%m%d')}_{t}"
        break

if current_slot_key and current_slot_key != v["last_slot"] and not v["running"]:
    v["running"] = True
    v["idx"] = 0
    v["results"] = []
    v["last_slot"] = current_slot_key
    st.session_state.active_slot = current_slot_key
    save_persistence(v["last_slot"], v["results"])

# ==============================
# 7. 掃描引擎（Mutex + 分批）
# ==============================
if v["running"] and not st.session_state.lock:
    st.session_state.lock = True
    try:
        universe = get_universe()
        total = len(universe)

        if total > 0 and v["idx"] < total:
            if time.time() - st.session_state.yf_lock_time > 3:

                batch = universe[v["idx"]: v["idx"] + 10]

                raw = yf.download(
                    batch,
                    period="250d",
                    group_by="ticker",
                    threads=True,
                    progress=False
                )

                for code in batch:
                    try:
                        try:
                            df_sub = raw.xs(code, level=0, axis=1)
                        except:
                            df_sub = raw[code] if code in raw else raw

                        result = analyze_stock_logic(code, df_sub)
                        if result:
                            v["results"].append(result)
                    except:
                        continue

                v["idx"] += len(batch)
                st.session_state.yf_lock_time = time.time()

                if v["idx"] % 40 == 0:
                    save_persistence(v["last_slot"], v["results"])
        else:
            v["running"] = False
            save_persistence(v["last_slot"], v["results"])

    finally:
        st.session_state.lock = False

# ==============================
# 8. UI
# ==============================
st_autorefresh(interval=1000, key="heartbeat")

st.title("🛡️ 多頭趨勢選股實驗室 v10.2")

st.code("排程: " + ", ".join(SCHEDULE))
st.caption(f"最後更新: {now.strftime('%Y-%m-%d %H:%M:%S')}")

c1, c2 = st.columns(2)
slot_label = v["last_slot"].split("_")[-1] if "_" in v["last_slot"] else "等待中"
c1.metric("當前時段", slot_label)
c2.metric("符合數量", len(v["results"]))

if v["running"]:
    st.progress(v["idx"] / max(len(get_universe()), 1))

if v["results"]:
    df_view = pd.DataFrame(v["results"]).drop_duplicates(subset=["股票代號"])
    st.dataframe(df_view.sort_values("型態"), use_container_width=True)
else:
    st.info("掃描中...")

with st.expander("診斷"):
    st.write(v)

    if st.button("Reset"):
        v.update({"running": False, "idx": 0, "results": [], "last_slot": ""})
        st.session_state.lock = False
        save_persistence("", [])
        st.rerun()
