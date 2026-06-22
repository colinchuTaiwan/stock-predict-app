import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, os, time, requests, base64, socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from streamlit_autorefresh import st_autorefresh

# ==============================
# Firebase
# ==============================
import firebase_admin
from firebase_admin import credentials, db as firebase_db

@st.cache_resource
def init_firebase():
    if firebase_admin._apps:
        return firebase_admin.get_app()
    s = st.secrets["firebase"]
    cert_dict = {
        "type":                        s["type"],
        "project_id":                  s["project_id"],
        "private_key_id":              s["private_key_id"],
        "private_key":                 s["private_key"].replace("\\n", "\n"),
        "client_email":                s["client_email"],
        "client_id":                   s["client_id"],
        "auth_uri":                    s["auth_uri"],
        "token_uri":                   s["token_uri"],
        "client_x509_cert_url":        s.get("client_x509_cert_url", ""),
        "auth_provider_x509_cert_url": s.get("auth_provider_x509_cert_url", ""),
    }
    cred = credentials.Certificate(cert_dict)
    return firebase_admin.initialize_app(cred, {"databaseURL": s["database_url"]})

# ==============================
# 0. 基礎設定
# ==============================
GITHUB_TOKEN  = st.secrets.get("GITHUB_TOKEN")
REPO_NAME     = st.secrets.get("GITHUB_REPO")
DB_PATH       = "db/scan_results.json"
LOCK_PATH     = "db/scan.lock.json"
LOG_PATH      = "app.log"
UNIVERSE_FILE = "db/taiwan_Full.json"
SITE_ID       = st.secrets.get("SITE_ID", "stock_scanner")

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)
def get_worker_id(): return f"{socket.gethostname()}-{os.getpid()}"

# ==============================
# 1. GitHub 引擎
# ==============================
class GitHubEngine:
    @staticmethod
    def fetch_remote(path):
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                d = r.json()
                content = base64.b64decode(d["content"]).decode("utf-8")
                return (json.loads(content) if path.endswith(".json") else content), d["sha"]
        except: pass
        return None, None

    @staticmethod
    def commit_file(path, content, msg, sha=None):
        if not sha: _, sha = GitHubEngine.fetch_remote(path)
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        c_str = json.dumps(content, ensure_ascii=False) if isinstance(content, (dict, list)) else str(content)
        payload = {"message": msg, "content": base64.b64encode(c_str.encode()).decode()}
        if sha: payload["sha"] = sha
        try:
            r = requests.put(url, headers=headers, json=payload, timeout=15)
            return r.status_code in [200, 201]
        except: return False

    @staticmethod
    def delete_lock(sha):
        if not sha: return False
        url = f"https://api.github.com/repos/{REPO_NAME}/contents/{LOCK_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        try:
            r = requests.delete(url, headers=headers, json={"message": "Release Lock", "sha": sha}, timeout=10)
            return r.status_code == 200
        except: return False

# ==============================
# 2. 訪客計數器（Firebase 版）
# ==============================
def track_visitor(site_id: str) -> int:
    init_firebase()
    ref = firebase_db.reference(f"visitor_counts/{site_id}")
    def increment(current): return (current or 0) + 1
    try:
        if "counted" not in st.session_state:
            count = ref.transaction(increment)
            st.session_state["counted"] = True
            return count
        return ref.get() or 0
    except Exception:
        return 0

# ==============================
# 3. 選股邏輯（遮罩三點分型版）
# ==============================

MASK_SIZE = 13
CENTER    = MASK_SIZE // 2   # = 6

@dataclass
class FractalTop:
    index_i: int
    high:    float
    low:     float
    close:   float   # 用於頭頭高（頂2收盤 > 頂1收盤）
    k0_rel:  int
    k2_rel:  int

@dataclass
class FractalBottom:
    index_i: int
    low:     float
    high:    float
    close:   float   # 用於底底高（底2收盤 > 底1收盤）
    k0_rel:  int
    k2_rel:  int

def _flat(df, col):
    return df[col].values.flatten().astype(float)

def calc_ma(df):
    df = df.copy()
    c = df["Close"].squeeze()
    for w in [5, 10, 20, 60, 100, 200]:
        df[f"MA{w}"] = c.rolling(w).mean()
    return df

# ── 遮罩頂分型 ────────────────────────────────────────────────────────────────
def find_confirmed_tops(df) -> List[FractalTop]:
    """
    遮罩大小 13，k1 位於索引 6（CENTER）。
    候選條件：
      ① High[6] 是遮罩13根中的唯一最高價
      ② MA5[6] > MA10[6] > MA20[6] > MA60[6]
    三點確認：
      k0：從索引 5 往左到 0，第一根 Low[j] < Low[6]
      k2：從索引 7 往右到 12，第一根 Low[j] < Low[6]
    """
    high  = _flat(df, "High")
    low   = _flat(df, "Low")
    close = _flat(df, "Close")
    ma5   = _flat(df, "MA5")
    ma10  = _flat(df, "MA10")
    ma20  = _flat(df, "MA20")
    ma60  = _flat(df, "MA60")
    n     = len(high)

    tops: List[FractalTop] = []

    for abs_k1 in range(CENTER, n - CENTER):
        s, e = abs_k1 - CENTER, abs_k1 + CENTER + 1
        m_high  = high [s:e]
        m_low   = low  [s:e]
        m_close = close[s:e]
        m_ma5   = ma5  [s:e]
        m_ma10  = ma10 [s:e]
        m_ma20  = ma20 [s:e]
        m_ma60  = ma60 [s:e]

        k1_high  = m_high [CENTER]
        k1_low   = m_low  [CENTER]
        k1_close = m_close[CENTER]

        if np.isnan(k1_high):
            continue
        if k1_high != np.max(m_high):
            continue
        if np.sum(m_high == k1_high) > 1:
            continue

        v5, v10, v20, v60 = m_ma5[CENTER], m_ma10[CENTER], m_ma20[CENTER], m_ma60[CENTER]
        if any(np.isnan(v) for v in [v5, v10, v20, v60]):
            continue
        if not (v5 > v10 > v20 > v60):
            continue

        k0_rel: Optional[int] = None
        for j in range(CENTER - 1, -1, -1):
            if m_low[j] < k1_low:
                k0_rel = j
                break
        if k0_rel is None:
            continue

        k2_rel: Optional[int] = None
        for j in range(CENTER + 1, MASK_SIZE):
            if m_low[j] < k1_low:
                k2_rel = j
                break
        if k2_rel is None:
            continue

        tops.append(FractalTop(
            index_i=abs_k1, high=k1_high, low=k1_low,
            close=k1_close, k0_rel=k0_rel, k2_rel=k2_rel,
        ))

    return tops

# ── 遮罩底分型 ────────────────────────────────────────────────────────────────
def find_confirmed_bottoms(df, tops: List[FractalTop]) -> List[FractalBottom]:
    """
    前提：必須先有頂分型。
    候選條件：
      ① 遮罩左側（0~5）所有 High 不超過最近頂分型的 Low
      ② Low[6] 是遮罩13根中的唯一最低價
      ③ Close[6] < MA5[6]
    三點確認：
      k0：從索引 5 往左到 0，第一根 High[j] > High[6]
      k2：從索引 7 往右到 12，第一根 High[j] > High[6]
    """
    if not tops:
        return []

    high  = _flat(df, "High")
    low   = _flat(df, "Low")
    close = _flat(df, "Close")
    ma5   = _flat(df, "MA5")
    n     = len(low)

    bottoms: List[FractalBottom] = []

    for abs_k1 in range(CENTER, n - CENTER):
        s, e = abs_k1 - CENTER, abs_k1 + CENTER + 1
        m_high  = high [s:e]
        m_low   = low  [s:e]
        m_close = close[s:e]
        m_ma5   = ma5  [s:e]

        k1_low  = m_low [CENTER]
        k1_high = m_high[CENTER]

        prev_tops = [t for t in tops if t.index_i < abs_k1]
        if not prev_tops:
            continue
        nearest_top = prev_tops[-1]

        left_highs = m_high[0:CENTER]
        if np.any(left_highs > nearest_top.low):
            continue

        if np.isnan(k1_low):
            continue
        if k1_low != np.min(m_low):
            continue
        if np.sum(m_low == k1_low) > 1:
            continue

        if np.isnan(m_ma5[CENTER]) or m_close[CENTER] >= m_ma5[CENTER]:
            continue

        k0_rel: Optional[int] = None
        for j in range(CENTER - 1, -1, -1):
            if m_high[j] > k1_high:
                k0_rel = j
                break
        if k0_rel is None:
            continue

        k2_rel: Optional[int] = None
        for j in range(CENTER + 1, MASK_SIZE):
            if m_high[j] > k1_high:
                k2_rel = j
                break
        if k2_rel is None:
            continue

        bottoms.append(FractalBottom(
            index_i=abs_k1, low=k1_low, high=k1_high,
            close=m_close[CENTER], k0_rel=k0_rel, k2_rel=k2_rel,
        ))

    return bottoms

# ── 頭頭高、底底高（收盤價版） ────────────────────────────────────────────────
def check_higher_highs_higher_lows(df):
    """
    回傳 (passed, 頂1收盤, 頂2收盤, 底1收盤, 底2收盤)
    頭頭高：top[-1].close > top[-2].close
    底底高：bot[-1].close > bot[-2].close
    結構驗證：top[-2].index < bot[-1].index < top[-1].index
    """
    NONE5 = (False, None, None, None, None)

    tops = find_confirmed_tops(df)
    if len(tops) < 2:
        return NONE5

    bottoms = find_confirmed_bottoms(df, tops)
    if len(bottoms) < 2:
        return NONE5

    last_top = tops[-1]
    prev_top = tops[-2]
    last_bot = bottoms[-1]
    prev_bot = bottoms[-2]

    if not (last_top.close > prev_top.close):
        return NONE5
    if not (last_bot.close > prev_bot.close):
        return NONE5
    if not (prev_top.index_i < last_bot.index_i < last_top.index_i):
        return NONE5

    return (
        True,
        round(prev_top.close, 2),
        round(last_top.close, 2),
        round(prev_bot.close, 2),
        round(last_bot.close, 2),
    )

def is_higher_highs_higher_lows(df) -> bool:
    return check_higher_highs_higher_lows(df)[0]

# ── 頭頭低、底底高（收斂三角） ───────────────────────────────────────────────
def check_lower_highs_higher_lows(df):
    """
    收斂三角（對稱三角整理）：
      頭頭低：top[-1].close < top[-2].close  ← 壓力線下傾
      底底高：bot[-1].close > bot[-2].close  ← 支撐線上傾
      結構驗證：top[-2].index < bot[-1].index < top[-1].index
    回傳 (passed, 頂1收盤, 頂2收盤, 底1收盤, 底2收盤)
    """
    NONE5 = (False, None, None, None, None)

    tops = find_confirmed_tops(df)
    if len(tops) < 2:
        return NONE5

    bottoms = find_confirmed_bottoms(df, tops)
    if len(bottoms) < 2:
        return NONE5

    prev_top = tops[-2]
    last_top = tops[-1]
    prev_bot = bottoms[-2]
    last_bot = bottoms[-1]

    if not (last_top.close < prev_top.close):   # 頭頭低
        return NONE5
    if not (last_bot.close > prev_bot.close):   # 底底高
        return NONE5
    if not (prev_top.index_i < last_bot.index_i < last_top.index_i):
        return NONE5

    return (True,
            round(prev_top.close, 2), round(last_top.close, 2),
            round(prev_bot.close, 2), round(last_bot.close, 2))

# ── 其餘技術條件（維持原版）──────────────────────────────────────────────────
def is_above_ma20(df):
    last = df.iloc[-1]
    return float(last["Close"]) > float(last["MA20"])

def is_ma_aligned(df):
    """四線多排：MA20 > MA60 > MA100 > MA200"""
    last = df.iloc[-1]
    try:
        mas = [float(last[f"MA{n}"]) for n in [20, 60, 100, 200]]
    except (TypeError, ValueError):
        return False
    if any(np.isnan(v) for v in mas):
        return False
    return all(mas[i] > mas[i+1] for i in range(len(mas)-1))

def is_ma_breakout(df, price_cap=200.0):
    """
    均線突破：
    - 前一日收盤 < MA5 OR MA10 OR MA20
    - 當日收盤站上 MA5 AND MA10 AND MA20
    - 收盤 < price_cap
    """
    if len(df) < 2:
        return False
    try:
        prev = df.iloc[-2]
        last = df.iloc[-1]
        pc, lc = float(prev["Close"]), float(last["Close"])
        pm5,  pm10,  pm20  = float(prev["MA5"]),  float(prev["MA10"]),  float(prev["MA20"])
        lm5,  lm10,  lm20  = float(last["MA5"]),  float(last["MA10"]),  float(last["MA20"])
    except (TypeError, ValueError):
        return False
    if any(np.isnan(v) for v in [pm5, pm10, pm20, lm5, lm10, lm20]):
        return False
    prev_below_any = (pc < pm5) or (pc < pm10) or (pc < pm20)
    last_above_all = (lc > lm5) and (lc > lm10) and (lc > lm20)
    return prev_below_any and last_above_all and (lc < price_cap)

def is_red_candle_limited(df, max_gain=0.07):
    """紅K且漲幅 < max_gain（排除漲停追高）"""
    try:
        last_open  = float(df["Open"].values.flatten()[-1])
        last_close = float(df["Close"].values.flatten()[-1])
    except (TypeError, ValueError):
        return False
    if last_open <= 0:
        return False
    gain = (last_close - last_open) / last_open
    return 0 < gain < max_gain

def is_close_above_prev_high(df):
    """當日收盤 > 前一日最高價"""
    if len(df) < 2:
        return False
    try:
        return _flat(df, "Close")[-1] > _flat(df, "High")[-2]
    except (TypeError, ValueError):
        return False

def is_ma20_bias_ok(df, threshold_20=0.07):
    """MA20 乖離率 < 7%"""
    last = df.iloc[-1]
    try:
        close = float(last["Close"])
        ma20  = float(last["MA20"])
    except (TypeError, ValueError):
        return False
    if np.isnan(ma20) or ma20 == 0:
        return False
    return (close - ma20) / ma20 < threshold_20

# ── 完整選股邏輯 ──────────────────────────────────────────────────────────────
def analyze_stock_logic(code, df):
    """
    七個條件（遮罩分型版）：
    1. 頭頭高、底底高（遮罩三點確認，比較收盤價）
    2. 站上 MA20
    3. 四線多排：MA20 > MA60 > MA100 > MA200
    4. 均線突破：前一日 < MA5/10/20 其一，當日站上三條，收盤<200
    5. 紅K：漲幅 0~7%
    6. 收盤過前高：當日收盤 > 前一日最高價
    7. MA20 乖離 < 7%
    額外過濾：均量 > 100 張，當日量 > 前日量 1.5 倍
    """
    try:
        if df is None or df.empty:
            return None
        required = ["Open", "High", "Low", "Close", "Volume"]
        if not all(c in df.columns for c in required):
            return None
        df = df.dropna()
        if len(df) < 210:
            return None

        df = calc_ma(df)
        if df.iloc[-1][["MA5","MA10","MA20","MA60","MA100","MA200"]].isnull().any():
            return None

        last   = df.iloc[-1]
        price  = float(last["Close"])
        open_p = float(last["Open"])
        vol    = float(last["Volume"]) / 1000
        mv20   = df["Volume"].rolling(20).mean().iloc[-1] / 1000
        prev_vol = float(df.iloc[-2]["Volume"]) / 1000

        # ── 條件①：形態判斷（多頭趨勢 / 收斂三角，任一通過）────────────────
        hh_hl, 頂1, 頂2, 底1, 底2 = check_higher_highs_higher_lows(df)
        lh_hl, t1_lh, t2_lh, b1_lh, b2_lh = check_lower_highs_higher_lows(df)

        # 任一通過即採用；優先顯示多頭趨勢的分型數值
        pattern_pass = hh_hl or lh_hl
        if not hh_hl and lh_hl:
            頂1, 頂2, 底1, 底2 = t1_lh, t2_lh, b1_lh, b2_lh

        # 形態標籤
        if hh_hl and lh_hl:
            pattern_label = "多頭+三角"
        elif hh_hl:
            pattern_label = "多頭趨勢"
        elif lh_hl:
            pattern_label = "收斂三角"
        else:
            pattern_label = ""
        above20         = is_above_ma20(df)
        aligned         = is_ma_aligned(df)
        ma_break        = is_ma_breakout(df)
        red_candle      = is_red_candle_limited(df)
        close_over_high = is_close_above_prev_high(df)
        bias_ok         = is_ma20_bias_ok(df)

        if not (pattern_pass and above20 and aligned and ma_break and
                red_candle and close_over_high and bias_ok):
            return None

        # 成交量過濾：均量 > 100 張，當日量 > 前日量 1.5 倍
        if mv20 < 100 or vol < prev_vol * 1.5:
            return None

        rk = (price - open_p) * 100 / open_p

        mas      = {w: float(last[f"MA{w}"]) for w in [5, 10, 20, 60, 100, 200]}
        prev_mas = {w: float(df.iloc[-2][f"MA{w}"]) for w in [5, 10, 20, 60, 100, 200]}
        up_count = sum(1 for w in [5, 10, 20, 60, 100, 200] if mas[w] > prev_mas[w])

        if mas[5] > mas[10] > mas[20] > mas[60] > mas[100] > mas[200]:
            ma_signal = {6:"六線多排", 5:"五線多排", 4:"四線多排",
                      3:"三線多排", 2:"二線多排"}.get(up_count, "多排")
        else:
            ma_signal = f"四線多排+突破（{up_count}線向上）"

        signal = f"{pattern_label}｜{ma_signal}" if pattern_label else ma_signal

        return {
            "股票代號": code,
            "價格":     round(price, 2),
            "漲幅":     f"{round(rk, 2)}%",
            "成交量":   int(vol),
            "頂1收盤":  頂1,
            "頂2收盤":  頂2,
            "底1收盤":  底1,
            "底2收盤":  底2,
            "MA20乖離": f"{round((price - mas[20]) / mas[20] * 100, 2)}%",
            "MA60乖離": f"{round((price - mas[60]) / mas[60] * 100, 2)}%",
            "型態":     signal,
            "時間":     now_taipei().strftime("%H:%M"),
        }

    except Exception:
        return None

# ==============================
# 4. 狀態大腦
# ==============================
@st.cache_resource
class DistributedBrain:
    def __init__(self):
        self.is_scanning   = False
        self.last_try_time = 0

    def try_lock(self, slot):
        if time.time() - self.last_try_time < 30: return False
        self.last_try_time = time.time()
        rem_lock, sha = GitHubEngine.fetch_remote(LOCK_PATH)
        if rem_lock and isinstance(rem_lock, dict):
            if time.time() - rem_lock.get("ts", 0) < 600: return False
        new_lock = {"slot": slot, "ts": time.time(), "worker": get_worker_id()}
        return GitHubEngine.commit_file(LOCK_PATH, new_lock, f"Lock {slot}", sha)

brain = DistributedBrain()

# ==============================
# 5. 主流程
# ==============================
st.set_page_config(page_title="趨勢選股 v11.2", layout="wide")

visitor_count = track_visitor(SITE_ID)

if not brain.is_scanning:
    st_autorefresh(interval=30000, key="refresh_safe")

remote_db, _ = GitHubEngine.fetch_remote(DB_PATH)
db = remote_db if (remote_db and isinstance(remote_db, dict) and "last_slot" in remote_db) \
    else {"ts": 0, "list": [], "last_slot": "none"}

now = now_taipei()
SCHEDULE = ["00:05", "02:00", "06:00", "08:30", "09:30", "10:30",
            "11:30", "12:15", "13:15", "15:00", "20:00", "23:00"]

current_slot = ""
for t in SCHEDULE:
    dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    if 0 <= (now - dt).total_seconds() <= 1500:
        current_slot = f"{now.strftime('%m%d')}_{t}"
        break

if current_slot and db.get("last_slot") != current_slot and not brain.is_scanning:
    if brain.try_lock(current_slot):
        brain.is_scanning = True
        st.rerun()

# --- 核心掃描區 ---
if brain.is_scanning:
    with st.status(f"🚀 正在掃描 {current_slot}...", expanded=True) as status:
        uni_data, _ = GitHubEngine.fetch_remote(UNIVERSE_FILE)
        stocks = uni_data.get("stocks", ["2330.TW", "2317.TW"]) if uni_data else ["2330.TW"]

        st.write(f"準備下載 {len(stocks)} 檔股票...")
        try:
            data = yf.download(stocks, period="260d", group_by="ticker",
                               threads=False, progress=False, auto_adjust=True)

            results = []
            p_bar = st.progress(0)

            for i, code in enumerate(stocks):
                try:
                    if len(stocks) > 1:
                        if isinstance(data.columns, pd.MultiIndex):
                            col_l0 = data.columns.get_level_values(0).unique().tolist()
                            if any(".TW" in str(v) for v in col_l0):
                                df = data[code].copy() if code in data.columns.get_level_values(0) else None
                            else:
                                df = data.xs(code, axis=1, level=1).copy() if code in data.columns.get_level_values(1) else None
                        else:
                            df = data
                    else:
                        df = data

                    if df is not None:
                        res = analyze_stock_logic(code, df)
                        if res:
                            results.append(res)
                except Exception:
                    pass
                p_bar.progress((i + 1) / len(stocks))

            st.write("同步至 GitHub...")
            new_db = {"list": results, "last_slot": current_slot, "ts": time.time()}
            if GitHubEngine.commit_file(DB_PATH, new_db, f"Final {current_slot}"):
                _, l_sha = GitHubEngine.fetch_remote(LOCK_PATH)
                GitHubEngine.delete_lock(l_sha)
                brain.is_scanning = False
                status.update(label="✅ 掃描完成！", state="complete", expanded=False)
                st.balloons()
                time.sleep(3)
                st.rerun()
        except Exception as e:
            st.error(f"下載失敗: {e}")
            brain.is_scanning = False
            time.sleep(5)
            st.rerun()

# --- UI 呈現 ---
st.title("📊 多頭趨勢選股實驗室 v11.2")
if db.get("list"):
    st.subheader(f"📅 最新結果: {db.get('last_slot')}")
    st.dataframe(pd.DataFrame(db["list"]), use_container_width=True)
else:
    st.info("等待排程自動觸發...")

with st.sidebar:
    st.subheader("⏰ 系統即時時間")
    st.title(f"{now.strftime('%H:%M:%S')}")
    st.write(f"預定排程: `{', '.join(SCHEDULE)}`")
    st.metric("👥 累計訪客", visitor_count)
    st.caption(f"網站識別碼: `{SITE_ID}`")
    if brain.is_scanning:
        st.warning("🔄 掃描引擎運行中...")

st.markdown("---")
with st.expander("⚠️ 投資免責聲明 (Disclaimer)"):
    st.caption("""
    1. **本工具僅供技術分析實驗與研究參考**，不構成任何投資建議、買賣邀約或承諾。
    2. 系統顯示之資料來源為第三方 API，資料可能存在延遲、錯誤或缺漏，使用者應自行核實。
    3. 過去的績效不代表未來獲利，投資一定有風險，股票投資有賺有賠，申購前應詳閱公開說明書並審慎評估。
    4. 使用者須對其投資決策負完全責任，本程式開發者不負擔任何法律責任或損失賠償。
    """)
