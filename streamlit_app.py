import streamlit as st
import pandas as pd
import json, base64, requests

st.set_page_config(page_title="台股監控儀表板", layout="wide")

GITHUB_REPO = st.secrets.get("GITHUB_REPO", "")
GITHUB_FILE = "scan_cache.json"

def get_cloud_data():
    url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/{GITHUB_FILE}"
    try:
        res = requests.get(url)
        if res.status_code == 200:
            return res.json()
    except: return None

st.title("📊 台股多頭排列監控系統")
st.caption("數據由後端 GitHub Actions 定時自動掃描產生")

data_json = get_cloud_data()

if data_json:
    st.metric("📡 最後更新時間", data_json.get("last_update"))
    df = pd.DataFrame(data_json.get("data", []))
    if not df.empty:
        st.dataframe(df.sort_values("漲幅%", ascending=False), use_container_width=True)
    else:
        st.info("目前無符合型態之股票")
else:
    st.error("無法取得雲端數據，請確認 GitHub 設定。")

if st.button("🔄 立即同步最新數據"):
    st.rerun()
