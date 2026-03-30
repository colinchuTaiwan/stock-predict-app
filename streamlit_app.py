import streamlit as st
import requests, base64, json, os, socket, time, threading
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# ==============================
# 0. 環境診斷 (請務必檢查 Secrets)
# ==============================
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN")
REPO_NAME = st.secrets.get("GITHUB_REPO")  # 必須是 "username/repo"
LOG_PATH = "app.log" # 建議先放在根目錄測試，避開 db/ 資料夾權限

tz = timezone(timedelta(hours=8))
def now_taipei(): return datetime.now(tz)

# ==============================
# 1. 強化版 GitHub 引擎
# ==============================
class GitHubEngine:
    @staticmethod
    def get_headers():
        return {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }

    @staticmethod
    def add_log_safe(message):
        """具備自動診斷功能的 Log 寫入"""
        try:
            ts = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
            new_line = f"[{ts}] {message}"
            url = f"https://api.github.com/repos/{REPO_NAME}/contents/{LOG_PATH}"
            
            # 1. 嘗試獲取舊檔案
            res = requests.get(url, headers=GitHubEngine.get_headers(), timeout=10)
            sha = None
            content = "=== Log Start ==="
            
            if res.status_code == 200:
                data = res.json()
                content = base64.b64decode(data["content"]).decode("utf-8")
                sha = data["sha"]
            elif res.status_code == 404:
                # 如果是 404，可能是檔案不存在(正常)，也可能是 Repo 路徑錯(異常)
                # 我們嘗試建立新檔案，不帶 SHA
                pass
            else:
                return False, f"讀取失敗 HTTP {res.status_code}"

            # 2. 組合並限制長度
            lines = content.splitlines()
            updated_content = "\n".join(lines[-100:] + [new_line])
            
            # 3. 提交
            payload = {
                "message": f"System Log Update",
                "content": base64.b64encode(updated_content.encode("utf-8")).decode("utf-8")
            }
            if sha: payload["sha"] = sha
            
            put_res = requests.put(url, headers=GitHubEngine.get_headers(), json=payload, timeout=15)
            
            if put_res.status_code in [200, 201]:
                return True, "成功"
            else:
                return False, f"寫入失敗 HTTP {put_res.status_code}: {put_res.text}"
        except Exception as e:
            return False, str(e)

# ==============================
# 2. UI 診斷面板
# ==============================
st.set_page_config(page_title="v13.9 診斷核心")
st_autorefresh(interval=30000, key="v139_refresh") # 增加到 30 秒，避免觸發 GitHub 頻率限制

st.title("🛡️ 系統診斷控制台 v13.9")

with st.sidebar:
    st.header("🔍 API 狀態檢查")
    
    if st.button("📡 開始連線測試"):
        # 測試 1: 檢查 Token 是否能讀取 Repo 資訊
        test_url = f"https://api.github.com/repos/{REPO_NAME}"
        r = requests.get(test_url, headers=GitHubEngine.get_headers())
        
        if r.status_code == 200:
            repo_info = r.json()
            st.success(f"✅ 找到倉庫: {REPO_NAME}")
            st.write(f"私有狀態: {repo_info.get('private')}")
            st.write(f"預設分支: {repo_info.get('default_branch')}")
            
            # 測試 2: 嘗試寫入 Log
            ok, msg = GitHubEngine.add_log_safe("手動連線測試成功")
            if ok:
                st.balloons()
                st.success("📝 Log 寫入成功！")
            else:
                st.error(f"❌ Log 寫入失敗: {msg}")
                if "404" in msg:
                    st.warning("提示：請確認 REPO_NAME 包含使用者名稱，例如: `YourID/YourRepo`")
        else:
            st.error(f"❌ 無法連線至倉庫: HTTP {r.status_code}")
            st.json(r.json())

# ==============================
# 3. 掃描逻辑與顯示 (其餘不變)
# ==============================
st.info(f"當前設定倉庫: `{REPO_NAME}`")
st.info(f"當前設定路徑: `{LOG_PATH}`")

logs, _ = GitHubEngine.fetch_remote(LOG_PATH) if hasattr(GitHubEngine, 'fetch_remote') else (None, None)
if logs:
    st.subheader("📜 系統日誌回放")
    st.text_area("Log Content", value=logs, height=300)
else:
    st.warning("目前無可用日誌或讀取中...")
