"""連線 4 台 node 的券商真單通道 (本機 ops 腳本;hub 不碰)。

用法 (本機 Windows):
  python scripts/connect_real_nodes.py           # 早上: 切 real + 連線 → 驗 connected/healthy,留著
  python scripts/connect_real_nodes.py --test    # 晚上驗證: 連線 → 暫設預算 arm 煙霧測試 →
                                                 #   解除+預算歸零 → 斷線 (留 mode=real/未連線)

機密處理: SSH 密碼讀本機 ../monitor_hosts.json (不進 repo);交易帳密/憑證由**各 node 機上**的
.env(load_config,與 runner 同源)+ /opt/hit_limit_up/certs/*.pfx 現讀,只在該機記憶體轉 base64
打 localhost API — 憑證不經過本機、不落地新檔。
"""
import argparse
import json
import sys
from pathlib import Path

try:
    import paramiko
except ImportError:
    print("需要 paramiko: pip install paramiko")
    sys.exit(1)

# 在 node 機上跑的 python (venv): 讀 .env 憑證 + cert → 打 localhost:8100 交易 API
REMOTE_PY = r'''
import base64, json, os, sys, time, urllib.request, urllib.error
MODE = os.environ.get("CRN_MODE", "morning")
sys.path.insert(0, "/opt/hit_limit_up/repo")
from config import load_config
cfg = load_config()

def post(path, payload):
    req = urllib.request.Request("http://localhost:8100" + path,
                                 data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode()[:160]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:160]

def status():
    with urllib.request.urlopen("http://localhost:8100/api/trading/status", timeout=10) as r:
        return json.loads(r.read().decode())

print("  mode->real:", post("/api/trading/mode", {"mode": "real"})[0])
b64 = base64.b64encode(open(cfg.pfx_path, "rb").read()).decode()
code, body = post("/api/trading/connect", {
    "account_id": cfg.account_id, "password": cfg.password,
    "pfx_password": cfg.pfx_password, "is_test": False,
    "pfx_b64": b64, "pfx_filename": os.path.basename(cfg.pfx_path)})
print("  connect:", code)
st = {}
for _ in range(30):
    st = status()
    if st.get("connected") or st.get("connect_error"):
        break
    time.sleep(1)
print("  => connected=%s healthy=%s acct=%s err=%r" % (
    st.get("connected"), st.get("healthy"), st.get("account_masked"),
    st.get("connect_error") or ""))
ok = bool(st.get("connected") and st.get("healthy"))

if MODE == "test":
    if ok:
        print("  [test] params(暫設):", post("/api/trading/params",
              {"total_budget": 1000000, "per_symbol_budget": 100000,
               "sizing_mode": "budget"})[0])
        code, body = post("/api/trading/arm", {"armed": True})
        print("  [test] arm 煙霧: HTTP %s %s" % (code, "✓ 預飛檢查全過" if code == 200 else body))
        print("  [test] disarm:", post("/api/trading/arm", {"armed": False})[0])
        print("  [test] params 歸零:", post("/api/trading/params",
              {"total_budget": 0, "per_symbol_budget": 0})[0])
    print("  [test] disconnect:", post("/api/trading/disconnect", {})[0])
    fin = status()
    print("  [test] 最終: mode=%s connected=%s armed=%s" % (
        fin.get("mode"), fin.get("connected"), fin.get("armed")))
print("NODE_RESULT", "PASS" if ok else "FAIL")
'''


def main():
    ap = argparse.ArgumentParser()
    default_hosts = Path(__file__).resolve().parent.parent.parent / "monitor_hosts.json"
    ap.add_argument("--hosts", default=str(default_hosts))
    ap.add_argument("--test", action="store_true",
                    help="驗證模式: 連線+arm 煙霧+歸零+斷線 (不留連線)")
    args = ap.parse_args()

    specs = [s for s in json.loads(Path(args.hosts).read_text(encoding="utf-8"))
             if s.get("role") != "hub"]
    mode = "test" if args.test else "morning"
    results = {}
    for s in specs:
        name, host = s.get("name", s["host"]), s["host"]
        print(f"===== {name} ({host}) [{mode}] =====")
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            c.connect(host, username="root", password=s["password"],
                      timeout=20, banner_timeout=20, auth_timeout=20)
            cmd = (f"CRN_MODE={mode} /opt/hit_limit_up/venv/bin/python - <<'PYEOF'\n"
                   + REMOTE_PY + "\nPYEOF")
            _i, out, err = c.exec_command(cmd, timeout=120)
            o = out.read().decode("utf-8", "replace")
            print(o)
            e = err.read().decode("utf-8", "replace").strip()
            if e:
                print("  [stderr]", e[:300])
            results[name] = "PASS" if "NODE_RESULT PASS" in o else "FAIL"
        except Exception as ex:
            print(f"  !! {type(ex).__name__}: {ex}")
            results[name] = "FAIL"
        finally:
            c.close()
    print("=" * 40)
    for n, r in results.items():
        print(f"  {n}: {'✅ ' + r if r == 'PASS' else '❌ ' + r}")
    sys.exit(0 if all(r == "PASS" for r in results.values()) else 1)


if __name__ == "__main__":
    main()
