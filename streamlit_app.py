import streamlit as st
import yfinance as yf
import pandas as pd
import json, os, base64, requests
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# =============================
# 0. 時區與秒級刷新
# =============================
tz = timezone(timedelta(hours=8))
def now_taipei():
    return datetime.now(tz)

# 秒級刷新頁面
st_autorefresh(interval=1000, key="sec_refresh")

# =============================
# 1. 頁面配置與 GitHub
# =============================
st.set_page_config(page_title="台股多頭排列融合掃描器 v2", layout="wide")

GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_FILE = st.secrets.get("GITHUB_FILE", "scan_cache.json")
CACHE_PATH = "scan_cache.json"

def upload_to_github(content_dict):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.get(url, headers=headers)
        sha = res.json().get("sha") if res.status_code == 200 else None
        content_str = json.dumps(content_dict, ensure_ascii=False, indent=2)
        content_b64 = base64.b64encode(content_str.encode()).decode()
        data = {
            "message": f"🤖 自動更新報告: {now_taipei().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch": "main"
        }
        if sha: data["sha"] = sha
        put_res = requests.put(url, headers=headers, json=data)
        if put_res.status_code not in [200,201]:
            st.error(f"GitHub 上傳失敗: {put_res.text}")
    except Exception as e:
        st.error(f"GitHub 更新失敗: {e}")

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH,"r",encoding="utf-8") as f:
                data = json.load(f)
                return pd.DataFrame(data.get("data",[])), data.get("last_update","尚未執行")
        except: pass
    return pd.DataFrame(), "尚未執行"

def save_cache(df, update_time):
    data = {"data": df.to_dict(orient="records"), "last_update": update_time}
    with open(CACHE_PATH,"w",encoding="utf-8") as f:
        json.dump(data,f,ensure_ascii=False,indent=2)
    upload_to_github(data)

# =============================
# 2. 技術指標計算
# =============================
def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5,10,20,60,100,200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()
        df[f"ma{w}_b"] = (close - df[f"ma{w}"])/df[f"ma{w}"]
    for w in [5,10,20,60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)
    df["pre_close"] = df['Close'].shift(1)
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    df["mv20"] = df['Volume'].rolling(20).mean()/1000
    df["RK_p"] = (close - df['Open'])*100/df['Open']
    return df

def extract_latest(df, ind):
    if len(df)<3: return None
    last_3 = df.tail(3)
    d = {
        'price': last_3['Close'].iloc[-1],
        'open': last_3['Open'].iloc[-1],
        'high': last_3['High'].iloc[-1],
        'vol': last_3['Volume'].iloc[-1]/1000,
        'pre_close': last_3['Close'].iloc[-2],
        'pre_high': last_3['High'].iloc[-2],
        'pre_vol': last_3['Volume'].iloc[-2]/1000
    }
    if not ind.empty:
        d.update(ind.iloc[-1].to_dict())
    return d

# =============================
# 3. 掃描核心函數
# =============================
def run_scan_logic(stock_codes, status_placeholder):
    all_found=[]
    batch_size=50
    total=len(stock_codes)
    pbar=st.progress(0)

    for i in range(0,total,batch_size):
        batch=stock_codes[i:i+batch_size]
        status_placeholder.info(f"⏳ 正在掃描批次 {i//batch_size+1}/{(total//batch_size+batch_size)//batch_size}...")
        try:
            raw=yf.download(tickers=batch, period="300d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
            if raw.empty: continue
            for code in batch:
                try:
                    df = raw[code].dropna() if isinstance(raw.columns,pd.MultiIndex) else raw.dropna()
                    if len(df)<200: continue
                    ind = calc_indicators(df)
                    d = extract_latest(df,ind)
                    if not d: continue
                    price,RK_p,vol = round(d['price'],2),round(d['RK_p'],1),int(d['vol'])
                    ma = {w:d[f'ma{w}'] for w in [5,10,20,60,100,200]}
                    ma_d = {w:d[f'ma{w}_d'] for w in [5,10,20,60,100,200]}
                    pre_ma = {w:ind[f'ma{w}'].iloc[-2] for w in [5,10,20,60]}

                    if not (1<RK_p<7): continue
                    cond_basic=(price>d['pre_high'] and d['mv20']>100 and vol>100 and
                                price>ma[20] and price>ma[60] and price>ma[100] and price>ma[200] and
                                all(v>0 for v in ma_d.values()) and vol>d['pre_vol']*1.5)
                    if not cond_basic: continue
                    is_breakout=any(d['pre_close']<pre_ma[w] for w in [5,10,20,60])
                    if not is_breakout: continue

                    # 判斷多頭排列型態
                    res_type=""
                    mv=list(ma.values())
                    if max(mv)/min(mv)<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60]>ma[100]>ma[200] and d['ma200_b']<0.08:
                        res_type="六線多排"
                    elif max(mv[:-1])/min(mv[:-1])<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60]>ma[100] and d['ma100_b']<0.08:
                        res_type="五線多排"
                    elif max(mv[:4])/min(mv[:4])<1.06 and price>ma[5]>ma[10]>ma[20]>ma[60] and d['ma60_b']<0.08:
                        res_type="四線多排"
                    elif max(mv[:3])/min(mv[:3])<1.06 and price>ma[5]>ma[10]>ma[20] and d['ma20_b']<0.08:
                        res_type="三線多排"
                    elif max(mv[:2])/min(mv[:2])<1.06 and price>ma[5]>ma[10] and d['ma10_b']<0.15:
                        res_type="二線多排"

                    if res_type:
                        all_found.append({"股票代號":code,"價格":price,"漲幅%":RK_p,"成交量":vol,
                                          "型態":res_type,"更新時間":now_taipei().strftime("%H:%M:%S")})
                except: continue
        except: continue
        pbar.progress(min((i+batch_size)/total,1.0))
    status_placeholder.empty()
    pbar.empty()
    return pd.DataFrame(all_found)

# =============================
# 4. 初始化 Session State 與排程
# =============================
# 排程設定
SCHEDULE_TIMES=["07:00","09:20","10:20","11:30","12:30","13:20","15:00","18:00","22:30","23:30"]

# 股票清單
os.makedirs("db/results",exist_ok=True)
json_path=os.path.join('db','taiwan_Full.json')
try:
    with open(json_path,'r',encoding="utf-8") as f:
        stock_list=json.load(f)['stocks']
except:
    stock_list=["2330.TW","2303.TW","2454.TW"]

# Session State 初始化
if "df_results" not in st.session_state: st.session_state.df_results=pd.DataFrame()
if "last_update" not in st.session_state: st.session_state.last_update="尚未執行"
if "last_run_min" not in st.session_state: st.session_state.last_run_min=""
if "seen_keys" not in st.session_state: st.session_state.seen_keys=set()

# 檢查是否到排程
curr_min=now_taipei().strftime("%H:%M")
if curr_min in SCHEDULE_TIMES and st.session_state.last_run_min != curr_min:
    st.session_state.last_run_min = curr_min
    status_placeholder=st.empty()
    new_results=run_scan_logic(stock_list,status_placeholder)
    if not new_results.empty:
        new_results["key"]=new_results["股票代號"]+"_"+new_results["型態"]
        filtered=new_results[~new_results["key"].isin(st.session_state.seen_keys)]
        st.session_state.seen_keys.update(new_results["key"].tolist())
        st.session_state.df_results=filtered.drop(columns=["key"])
    else:
        st.session_state.df_results=pd.DataFrame()
    st.session_state.last_update=now_taipei().strftime("%Y-%m-%d %H:%M:%S")
    save_cache(st.session_state.df_results, st.session_state.last_update)
    status_placeholder.success(f"✅ {curr_min} 掃描完成！")
    st.rerun()

# =============================
# 5. UI 顯示
# =============================
st.title("🚀 台股多頭排列融合掃描器 v2")

with st.container():
    c1,c2,c3=st.columns(3)
    c1.metric("⏰ 系統時間",now_taipei().strftime("%H:%M:%S"))
    c2.metric("📡 最後掃描完成",st.session_state.last_update)
    c3.metric("📊 監控總檔數",len(stock_list))

st.divider()

with st.sidebar:
    st.header("⚙️ 系統設定")
    st.info(f"監控檔數: {len(stock_list)}")
    st.write("排程時間:",SCHEDULE_TIMES)
    if st.button("🔄 重置新訊號"):
        st.session_state.seen_keys=set()
        st.session_state.df_results=pd.DataFrame()
        st.rerun()

st.subheader("📊 本輪新訊號")
st.caption("只顯示本次排程觸發的新訊號")

if not st.session_state.df_results.empty:
    tab1,tab2=st.tabs(["📋 所有結果","🔍 依型態篩選"])
    with tab1:
        st.dataframe(st.session_state.df_results.sort_values("漲幅%",ascending=False),use_container_width=True)
    with tab2:
        types=st.session_state.df_results['型態'].unique()
        selected_type=st.selectbox("選擇型態",types)
        st.table(st.session_state.df_results[st.session_state.df_results['型態']==selected_type])
else:
    st.info("⌛ 尚無新訊號，等待排程觸發中。")
