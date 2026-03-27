import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import json, base64, requests, time, concurrent.futures
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 全域配置與環境 (Global Scope)
# ==============================
tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)

# 驅動頻率 (4秒一次，兼顧 UI 流暢與 API 冷卻)
st_autorefresh(interval=4000, key="v8_5_engine_tick")

@st.cache_data(ttl=3600)
def get_universe():
    """全域緩存股票清單，避免重複 IO 損耗"""
    try:
        # 預設路徑，請確保資料夾存在
        with open("db/taiwan_Full.json", "r", encoding="utf-8") as f:
            return json.load(f)['stocks']
    except:
        return ["2330.TW", "2454.TW", "2317.TW", "2303.TW", "2603.TW"]

# ==============================
# 1. 核心選股邏輯 (整合 Signal 1-8)
# ==============================
def analyze_stock_logic(code, df):
    """
    核心策略引擎：執行基礎過濾與 Signal 1-8 判定
    """
    try:
        # A. 數據清洗與基本檢查
        df = df.dropna()
        if len(df) < 210: return None
        
        # 取得最新一根 (d) 與前一根 (pre)
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        price = round(curr['Close'], 2)
        open_ = round(curr['Open'], 2)
        high_ = round(curr['High'], 2)
        low_ = round(curr['Low'], 2)
        vol = int(curr['Volume'])
        
        pre_close = round(prev['Close'], 2)
        pre_high = round(prev['High'], 2)
        pre_vol = int(prev['Volume']) / 1000 # 換算單位
        
        # B. 均線與指標計算
        ma_periods = [5, 10, 20, 60, 100, 200]
        mas = {f"ma{m}": df['Close'].rolling(m).mean().iloc[-1] for m in ma_periods}
        pre_mas = {f"ma{m}": df['Close'].rolling(m).mean().iloc[-2] for m in ma_periods}
        
        mv20 = df['Volume'].rolling(20).mean().iloc[-1]
        
        # C. 漲幅與乖離
        rk_p = round((price - open_) * 100 / open_, 1)
        bias = {f"ma{m}_b": (price - mas[f"ma{m}"]) / mas[f"ma{m}"] for m in ma_periods}
        
        # D. 基礎門檻過濾 (Basic Filter)
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
        
        # 1. 糾結模式判定 (優先級高：爆發力最強)
        # 判定糾結度 (Max/Min) 與 位階 (需在五線之上)
        tangle_ratio = max(ma_vals) / min(ma_vals)
        above_all = all(price > mas[f"ma{m}"] for m in [20, 60, 100, 200])
        
        if tangle_ratio < 1.08 and above_all:
            if bias['ma200_b'] < 0.1: signal = "Signal 5: 六線糾結突破"
            elif bias['ma100_b'] < 0.1: signal = "Signal 6: 五線糾結突破"
            elif bias['ma60_b'] < 0.1: signal = "Signal 7: 四線糾結突破"
            else: signal = "Signal 8: 三線糾結突破"
            
        # 2. 多頭排列模式 (若非糾結，則判斷排列強度)
        elif mas['ma5'] > mas['ma20'] > mas['ma60'] > mas['ma100'] > mas['ma200']:
            # 計算均線斜率向上數量
            slopes = sum(1 for m in ma_periods if mas[f"ma{m}"] > pre_mas[f"ma{m}"])
            if slopes >= 5: signal = "Signal 1: 五線全揚"
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
# 2. 狀態管理與調度器 (Event-Driven)
# ==============================
if "engine" not in st.session_state:
    st.session_state.engine = {
        "is_scanning": False, "scan_idx": 0, "found_list": [],
        "triggered_slots": set(), "last_api_time": 0.0
    }

ev = st.session_state.engine

# 排程設定
SCHEDULE = ["09:05", "10:10", "11:30", "13:05", "14:45", "23:20"]
now_dt = now_taipei()
today_key = now_dt.strftime("%Y-%m-%d")

current_slot = ""
for t in SCHEDULE:
    slot_dt = datetime.strptime(f"{today_key} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    # 45秒時間窗，避免漂移
    if abs((now_dt - slot_dt).total_seconds()) <= 45:
        current_slot = f"{today_key} {t}"
        break

# 觸發與重置
if current_slot and current_slot not in ev["triggered_slots"] and not ev["is_scanning"]:
    ev["is_scanning"] = True
    ev["scan_idx"] = 0
    ev["found_list"] = [] # 每次新 Slot 清空結果
    ev["triggered_slots"].add(current_slot)

# ==============================
# 3. 掃描執行器 (Batch Executor)
# ==============================
if ev["is_scanning"]:
    universe = get_universe()
    idx = ev["scan_idx"]
    
    if idx < len(universe):
        # API 頻率控制 (每 3 秒跑一批)
        if time.time() - ev["last_api_time"] > 3.2:
            batch = universe[idx : idx + 18]
            st.info(f"📡 正在掃描: {idx}/{len(universe)} | Slot: {current_slot}")
            
            try:
                # yf.download 本身有多執行緒，不建議外層再套 ThreadPool
                raw = yf.download(tickers=batch, period="300d", group_by="ticker", 
                                 auto_adjust=False, threads=True, progress=False)
                
                for code in batch:
                    # 處理單/多標的結構
                    df_sub = raw.xs(code, level=0, axis=1) if len(batch) > 1 else raw
                    res = analyze_stock_logic(code, df_sub)
                    if res: ev["found_list"].append(res)
                
                ev["scan_idx"] += len(batch)
                ev["last_api_time"] = time.time()
            except Exception as e:
                st.error(f"掃描異常: {e}")
                ev["scan_idx"] += len(batch) # 發生錯誤亦推進，防止死循環
    else:
        ev["is_scanning"] = False
        st.success("✅ 本時段掃描完成")

# ==============================
# 4. 監控儀表板 (UI Render)
# ==============================
st.title("🛡️ Quantum Guard Engine v8.5")
st.markdown(f"**當前 Slot:** `{current_slot if current_slot else '監聽中...'}`")

c1, c2, c3 = st.columns(3)
c1.metric("台北時間", now_dt.strftime("%H:%M:%S"))
c2.metric("發現標的", len(ev["found_list"]))
c3.metric("剩餘檔數", len(get_universe()) - ev["scan_idx"])

if ev["found_list"]:
    res_df = pd.DataFrame(ev["found_list"])
    # 針對 Signal 5 進行排序（糾結度越低越強）
    st.dataframe(res_df.sort_values("糾結度", ascending=True), 
                 use_container_width=True, hide_index=True)
else:
    st.info("⌛ 系統待命中，符合條件之標的將顯示於此。")

with st.expander("📝 引擎日誌與排程"):
    st.write(f"已執行時段: {list(ev['triggered_slots'])}")
    st.write(f"當前索引: {ev['scan_idx']}")
    st.code("排程點: " + ", ".join(SCHEDULE))
