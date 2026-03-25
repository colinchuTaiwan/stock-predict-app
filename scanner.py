import yfinance as yf
import pandas as pd
import json, base64, requests, os
from datetime import datetime, timedelta, timezone

# --- 配置 ---
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
GITHUB_FILE = "scan_cache.json"
tz = timezone(timedelta(hours=8))

def now_taipei():
    return datetime.now(tz)

def calc_indicators(df):
    df = df.copy()
    close = df['Close']
    for w in [5,10,20,60,100,200]:
        df[f"ma{w}"] = close.rolling(w).mean()
        df[f"ma{w}_d"] = df[f"ma{w}"].diff()
    df["pre_close"] = df['Close'].shift(1)
    df["pre_high"] = df['High'].shift(1)
    df["pre_vol"] = df['Volume'].shift(1)
    df["mv20"] = df['Volume'].rolling(20).mean()/1000
    df["RK_p"] = (close - df['Open'])*100/df['Open']
    for w in [5,10,20,60]:
        df[f"pre_ma{w}"] = df[f"ma{w}"].shift(1)
    return df

def run_scan():
    # 載入清單 (請確保 GitHub 倉庫中有此檔案)
    try:
        with open("db/taiwan_Full.json","r",encoding="utf-8") as f:
            stock_list = json.load(f)['stocks']
    except:
        stock_list = ["2330.TW", "2454.TW", "2317.TW"]

    all_found = []
    batch_size = 50
    for i in range(0, len(stock_list), batch_size):
        batch = stock_list[i:i+batch_size]
        try:
            raw = yf.download(tickers=batch, period="300d", group_by="ticker", progress=False)
            for code in batch:
                df = raw[code].dropna() if len(batch)>1 else raw.dropna()
                if len(df) < 200: continue
                ind = calc_indicators(df)
                last = ind.iloc[-1]
                
                # 篩選邏輯
                price, rk_p, vol = last['Close'], last['RK_p'], last['Volume']/1000
                ma = {w: last[f'ma{w}'] for w in [5,10,20,60,100,200]}
                ma_d = {w: last[f'ma{w}_d'] for w in [5,10,20,60,100,200]}
                
                if 1 < rk_p < 7 and price > last['pre_high'] and vol > 100:
                    if price > ma[20] > ma[60] and all(v > 0 for v in ma_d.values()):
                        res_type = ""
                        mv = list(ma.values())
                        if max(mv)/min(mv) < 1.06: res_type = "多頭排列(強)"
                        elif price > ma[5] > ma[10] > ma[20]: res_type = "三線多排"
                        
                        if res_type:
                            all_found.append({
                                "股票代號": code, "價格": round(price, 2), "漲幅%": round(rk_p, 1),
                                "成交量": int(vol), "型態": res_type, "更新時間": now_taipei().strftime("%Y-%m-%d %H:%M:%S")
                            })
        except: continue
    
    # 存檔與上傳
    final_data = {"data": all_found, "last_update": now_taipei().strftime("%Y-%m-%d %H:%M:%S")}
    
    if GITHUB_TOKEN and GITHUB_REPO:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        res = requests.get(url, headers=headers)
        sha = res.json().get("sha") if res.status_code == 200 else None
        content_b64 = base64.b64encode(json.dumps(final_data).encode()).decode()
        payload = {"message": "Auto Scan Update", "content": content_b64, "branch": "main"}
        if sha: payload["sha"] = sha
        requests.put(url, headers=headers, json=payload)
        print("✅ 掃描完成並更新至 GitHub")

if __name__ == "__main__":
    run_scan()
