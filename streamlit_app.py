import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, time
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 全域配置 + Stability Layer
# ==============================
STATE_FILE = "db/scan_results.json"
tz = timezone(timedelta(hours=8))

def now_taipei():
    return datetime.now(tz)

# 🔒 Scan Mutex + Guard State
if "lock" not in st.session_state:
    st.session_state.lock = False

if "active_slot" not in st.session_state:
    st.session_state.active_slot = None

if "yf_lock_time" not in st.session_state:
    st.session_state.yf_lock_time = 0


@st.cache_data(ttl=3600)
def get_universe():
    try:
        if not os.path.exists("db/taiwan_Full.json"):
            return []
        with open("db/taiwan_Full.json", "r", encoding="utf-8-sig") as f:
            return json.load(f).get("stocks", [])
    except:
        return []


def load_persistence():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {"last_slot": "", "list": []}


def save_persistence(last_slot, results):
    # 🔒 atomic write (防 JSON 壞掉)
    try:
        os.makedirs("db", exist_ok=True)
        tmp = STATE_FILE + ".tmp"

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {"last_slot": last_slot, "list": results},
                f,
                ensure_ascii=False
            )

        os.replace(tmp, STATE_FILE)

    except:
        pass


st_autorefresh(interval=5000, key="v9_8_heartbeat")

# ==============================
# 1. Strategy Filter (不改邏輯)
# ==============================
def run_strategy_filter(code, df):
    try:
        if df is None or df.empty or len(df) < 200:
            return None

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        price = round(curr["Close"], 2)
        open_ = round(curr["Open"], 2)
        vol = int(curr["Volume"])
        pre_high = round(prev["High"], 2)
        pre_vol = int(prev["Volume"]) / 1000

        RK_p = round((price - open_) * 100 / open_, 1)

        ma = {m: round(df["Close"].rolling(m).mean().iloc[-1], 2)
              for m in [5, 10, 20, 60, 100, 200]}

        pre_ma = {m: df["Close"].rolling(m).mean().iloc[-2]
                  for m in [5, 10, 20, 60]}

        mv20 = df["Volume"].rolling(20).mean().iloc[-1]

        # 基礎條件
        if not (1.0 < RK_p < 7.5):
            return None

        if not (
            price > pre_high and price > ma[5]
            and mv20 > 100 and vol > 100
            and price < 200 and vol > pre_vol * 1.5
        ):
            return None

        if not any(prev["Close"] < pre_ma[m] for m in [5, 10, 20, 60]):
            return None

        # 訊號
        all_mas = [ma[m] for m in [5, 10, 20, 60, 100, 200]]
        tangle_ratio = max(all_mas) / min(all_mas)

        sig = None

        if all(price > ma[m] for m in [20, 60, 100, 200]):
            if tangle_ratio < 1.08:
                sig = "Signal 5: 六線糾結"
            elif max(all_mas[:5]) / min(all_mas[:5]) < 1.08:
                sig = "Signal 6: 五線糾結"

        if not sig and ma[5] > ma[20] > ma[60] > ma[100] > ma[200]:
            sig = "Signal 1: 多頭排列"

        if not sig:
            return None

        return {
            "代號": code,
            "價格": price,
            "漲幅%": RK_p,
            "訊號": sig,
            "糾結度": round(tangle_ratio, 3),
            "時間": now_taipei().strftime("%H:%M")
        }

    except:
        return None


# ==============================
# 2. State Init + Slot Guard
# ==============================
if "v98" not in st.session_state:
    db = load_persistence()
    st.session_state.v98 = {
        "running": False,
        "idx": 0,
        "results": db["list"],
        "last_slot": db["last_slot"],
        "last_api": 0.0
    }

v = st.session_state.v98
now = now_taipei()

SCHEDULE = ["09:05", "10:30", "11:30", "13:05", "14:45", "23:37"]

# 🔒 Stable slot detection (fix drift)
current_slot_key = ""

for t in SCHEDULE:
    try:
        slot_dt = datetime.strptime(
            f"{now.strftime('%Y-%m-%d')} {t}",
            "%Y-%m-%d %H:%M"
        ).replace(tzinfo=tz)

        if abs((now - slot_dt).total_seconds()) <= 60:
            current_slot_key = f"{now.strftime('%m%d')}_{t}"
            break
    except:
        pass

# 🔒 Prevent duplicate trigger
if (
    current_slot_key
    and current_slot_key != v["last_slot"]
    and st.session_state.active_slot != current_slot_key
    and not v["running"]
):
    v["running"] = True
    v["idx"] = 0
    v["results"] = []
    v["last_slot"] = current_slot_key

    st.session_state.active_slot = current_slot_key

    save_persistence(v["last_slot"], v["results"])


# ==============================
# 3. Scanner Engine (Mutex + Safe YF)
# ==============================
if v["running"]:

    if not st.session_state.lock:
        st.session_state.lock = True

        try:
            universe = get_universe()
            u_len = len(universe)

            if u_len > 0 and v["idx"] < u_len:

                if time.time() - st.session_state.yf_lock_time > 3.8:

                    batch = universe[v["idx"]: v["idx"] + 20]

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
                                try:
                                    df_sub = raw[code]
                                except:
                                    df_sub = raw

                            hit = run_strategy_filter(code, df_sub)

                            if hit:
                                v["results"].append(hit)

                                # 🔒 memory cap
                                if len(v["results"]) > 2000:
                                    v["results"] = v["results"][-2000:]

                        except:
                            pass

                    v["idx"] += len(batch)
                    st.session_state.yf_lock_time = time.time()

                    # periodic persistence
                    if v["idx"] % 40 == 0:
                        save_persistence(v["last_slot"], v["results"])

            else:
                v["running"] = False
                save_persistence(v["last_slot"], v["results"])

        finally:
            st.session_state.lock = False


# ==============================
# 4. UI (Stable Render)
# ==============================
st.title("🛡️ Quantum Guard v9.8 Stable Engine")

slot_display = v["last_slot"].split("_")[-1] if "_" in v["last_slot"] else "監聽中"

c1, c2 = st.columns(2)
c1.metric("掃描時段", slot_display)
c2.metric("標的數", len(v["results"]))

universe = get_universe()
u_len = len(universe)

if v["running"] and u_len > 0:
    st.progress(min(v["idx"] / max(u_len, 1), 1.0))
    st.caption(f"{v['idx']} / {u_len}")

if v["results"]:
    df_view = pd.DataFrame(v["results"]).drop_duplicates(
        subset=["代號"],
        keep="last"
    )

    st.dataframe(
        df_view.sort_values(["訊號", "代號"]),
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("⌛ 等待訊號觸發中...")

with st.expander("🛠️ 系統控制"):
    st.write(f"Running: {v['running']}")
    st.write(f"Slot: {v['last_slot']}")

    if st.button("🔴 Reset All"):
        v.update({
            "running": False,
            "idx": 0,
            "results": [],
            "last_slot": "",
            "last_api": 0.0
        })

        st.session_state.active_slot = None
        st.session_state.lock = False
        st.session_state.yf_lock_time = 0

        save_persistence("", [])
        st.rerun()
