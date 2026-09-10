"""盤後唯讀核對: 4 台 node 各拉一次 get_order_results,任何仍 live 的 hitlimit 買單 → 紅字告警。

2026-09-09 node3 事故 (4 筆管線多送市價買被誤標 cancelled、券商端 live 一整天) 的常駐防線 (計畫「驗證 5」):
每日 13:26 後在本機跑;任何 status ∈ {0,4,8,10} 的 user_def=="hitlimit" 買單都是**不該存在的裸掛單**
(13:23 收盤撤單 + 券商權威掃單後應為 0),出現即 exit 1 + 紅字 (之後 central 續做時接進 Signal)。

用法 (本機 Windows):
  python scripts/post_close_check.py                 # 讀 ../monitor_hosts.json (repo 外),4 台 node 全跑
  python scripts/post_close_check.py --only node3    # 只跑一台
  python scripts/post_close_check.py --force         # 盤中 (08:00–13:35) 預設拒跑;--force 才跑

機密處理: SSH 密碼讀本機 ../monitor_hosts.json (不進 repo);交易帳密/憑證由**各 node 機上** .env
(load_config,與 runner 同源) + pfx 現讀,只在該機記憶體登入 → 查詢 → logout。帳號印出一律遮罩。
**唯讀**: 不下單、不撤單、不寫任何檔 (遠端以 heredoc 餵 python -,連暫存檔都沒有)。
"""
import argparse
import json
import sys
from datetime import datetime, time as dtime
from pathlib import Path

try:
    import paramiko
except ImportError:
    print("需要 paramiko: pip install paramiko")
    sys.exit(1)

GREEN, RED, YEL, RST = "\033[92m", "\033[91m", "\033[93m", "\033[0m"
MARKET_GUARD = (dtime(8, 0), dtime(13, 35))       # 盤中拒跑 (除非 --force)

# 在 node 機上跑的 python (venv): 讀 .env 憑證 → 富邦登入 → get_order_results → 印 live hitlimit 買單 → logout。
# 只印 PCC_ 開頭的機器可讀行;任何例外都印 PCC_RESULT ERROR。
REMOTE_PY = r'''
import json, os, sys
sys.path.insert(0, "/opt/hit_limit_up/repo")
os.chdir("/opt/hit_limit_up/repo")

def _attr(o, *names, default=None):
    for n in names:
        v = getattr(o, n, None)
        if v is not None:
            return v
    return default

def _norm(v):
    s = "" if v is None else str(v)
    return s.split(".")[-1] if "." in s else s

LIVE = ("0", "4", "8", "10")
sdk = None
try:
    from config import load_config
    cfg = load_config()
    masked = cfg.account_id[:3] + "***"
    from fubon_neo.sdk import FubonSDK
    sdk = FubonSDK()
    if cfg.pfx_password == "":
        accounts = sdk.login(cfg.account_id, cfg.password, cfg.pfx_path)
    else:
        accounts = sdk.login(cfg.account_id, cfg.password, cfg.pfx_path, cfg.pfx_password)
    if not accounts or getattr(accounts, "is_success", None) is not True:
        print("PCC_RESULT ERROR login: %s" % (getattr(accounts, "message", None) or "login 回空/失敗"))
        sys.exit(2)
    data = getattr(accounts, "data", None) or []
    if not data:
        print("PCC_RESULT ERROR login 成功但無帳戶")
        sys.exit(2)
    acct = data[0]
    print("PCC_ACCOUNT", masked)
    r = sdk.stock.get_order_results(acct)
    ok = bool(r) and getattr(r, "is_success", None) is True
    if not ok:
        # 例「Login Error, 業務系統流量控管」— 查詢失敗 ≠ 清單為空 (踩雷點 #9)
        print("PCC_RESULT ERROR query: %s" % (getattr(r, "message", None) if r else "result 回空"))
        sys.exit(2)
    rows = list(getattr(r, "data", None) or [])
    live = []
    n_hit = 0
    for o in rows:
        user_def = str(_attr(o, "user_def", "userDef", default="") or "")
        bs = _norm(_attr(o, "buy_sell", default=""))
        status = _norm(_attr(o, "status", default=""))
        if user_def != "hitlimit" or bs != "Buy":
            continue
        n_hit += 1
        if status not in LIVE:
            continue
        qty = int(_attr(o, "quantity", default=0) or 0)
        filled = int(_attr(o, "filled_qty", "filledQty", default=0) or 0)
        after = _attr(o, "after_qty", "afterQty", default=None)
        live.append({
            "order_no": str(_attr(o, "order_no", "orderNo", default="") or ""),
            "symbol": str(_attr(o, "stock_no", "symbol", default="") or ""),
            "status": status,
            "quantity": qty,
            "filled_qty": filled,
            "after_qty": None if after is None else int(after),
            "price": str(_attr(o, "price", default="") or ""),
            "last_time": str(_attr(o, "last_time", "lastTime", default="") or ""),
            "date": str(_attr(o, "date", default="") or ""),
        })
    print("PCC_TOTAL", len(rows), n_hit)
    for row in live:
        print("PCC_LIVE", json.dumps(row, ensure_ascii=False))
    print("PCC_RESULT", "LIVE" if live else "OK", len(live))
except SystemExit:
    raise
except Exception as e:
    print("PCC_RESULT ERROR %s: %s" % (type(e).__name__, e))
finally:
    if sdk is not None:
        try:
            sdk.logout()
        except Exception:
            pass
'''


def _in_market_hours(now: datetime) -> bool:
    return MARKET_GUARD[0] <= now.time() < MARKET_GUARD[1]


def check_node(spec: dict, timeout: int = 90) -> dict:
    """SSH 到一台 node 跑 REMOTE_PY,解析 PCC_ 行。回 {name, result, n_live, live[], account, total, hit, err}。"""
    name, host = spec.get("name", spec["host"]), spec["host"]
    out = {"name": name, "host": host, "result": "ERROR", "n_live": 0, "live": [],
           "account": "", "total": 0, "hit": 0, "err": ""}
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        c.connect(host, username="root", password=spec["password"],
                  timeout=20, banner_timeout=20, auth_timeout=20)
        cmd = "/opt/hit_limit_up/venv/bin/python - <<'PYEOF'\n" + REMOTE_PY + "\nPYEOF"
        _i, o, e = c.exec_command(cmd, timeout=timeout)
        text = o.read().decode("utf-8", "replace")
        err = e.read().decode("utf-8", "replace").strip()
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("PCC_ACCOUNT "):
                out["account"] = line.split(" ", 1)[1]
            elif line.startswith("PCC_TOTAL "):
                parts = line.split()
                out["total"], out["hit"] = int(parts[1]), int(parts[2])
            elif line.startswith("PCC_LIVE "):
                try:
                    out["live"].append(json.loads(line[len("PCC_LIVE "):]))
                except Exception:
                    out["live"].append({"raw": line})
            elif line.startswith("PCC_RESULT "):
                rest = line[len("PCC_RESULT "):]
                if rest.startswith("ERROR"):
                    out["result"] = "ERROR"
                    out["err"] = rest[len("ERROR"):].strip()
                else:
                    tag, n = rest.split()
                    out["result"], out["n_live"] = tag, int(n)
        if out["result"] == "ERROR" and not out["err"]:
            out["err"] = (err or text or "無輸出")[:300]
    except Exception as ex:
        out["err"] = f"{type(ex).__name__}: {ex}"
    finally:
        c.close()
    return out


def _print_node(r: dict):
    head = f"===== {r['name']} ({r['host']}) acct={r['account'] or '?'} ====="
    if r["result"] == "OK":
        print(f"{head}\n  {GREEN}OK{RST} — 無 live hitlimit 買單 "
              f"(當日委託 {r['total']} 筆,其中 hitlimit 買 {r['hit']} 筆)")
    elif r["result"] == "LIVE":
        print(f"{head}\n  {RED}⚠⚠ {r['n_live']} 筆 hitlimit 買單仍 live — 需人工到券商端撤單!{RST} "
              f"(當日委託 {r['total']} 筆)")
        for row in r["live"]:
            print(f"  {RED}  order={row.get('order_no')} {row.get('symbol')} status={row.get('status')} "
                  f"qty={row.get('quantity')} filled={row.get('filled_qty')} after={row.get('after_qty')} "
                  f"price={row.get('price')} last={row.get('last_time')}{RST}")
    else:
        print(f"{head}\n  {YEL}ERROR{RST} {r['err']}")


def main():
    ap = argparse.ArgumentParser(description="盤後唯讀核對 4 台 node 券商端是否仍有 live hitlimit 買單")
    default_hosts = Path(__file__).resolve().parent.parent.parent / "monitor_hosts.json"
    ap.add_argument("--hosts", default=str(default_hosts))
    ap.add_argument("--only", default="", help="只跑此 name (例 node3)")
    ap.add_argument("--force", action="store_true", help="盤中 (08:00–13:35) 也跑")
    ap.add_argument("--timeout", type=int, default=90, help="遠端執行逾時秒數")
    args = ap.parse_args()

    now = datetime.now()
    if _in_market_hours(now) and not args.force:
        print(f"{YEL}現在 {now.strftime('%H:%M:%S')} 在盤中 08:00–13:35 — 拒跑 (13:23 撤單/13:24 收盤前查到 live "
              f"是正常的;要硬跑加 --force){RST}")
        sys.exit(3)

    specs = [s for s in json.loads(Path(args.hosts).read_text(encoding="utf-8"))
             if s.get("role") != "hub"]
    if args.only:
        specs = [s for s in specs if s.get("name") == args.only]
        if not specs:
            print(f"{RED}--only {args.only}: hosts 檔無此 node{RST}")
            sys.exit(3)
    print(f"盤後核對 {len(specs)} 台 node ({now.strftime('%Y-%m-%d %H:%M:%S')}) — 唯讀,不下單不撤單不寫檔")
    results = [check_node(s, timeout=args.timeout) for s in specs]
    for r in results:
        _print_node(r)
    n_live = sum(r["n_live"] for r in results)
    n_err = sum(1 for r in results if r["result"] == "ERROR")
    print("=" * 48)
    if n_live == 0 and n_err == 0:
        print(f"  {GREEN}ALL CLEAR ✅ — 4 台券商端無 live hitlimit 買單{RST}")
        sys.exit(0)
    if n_live:
        print(f"  {RED}❌ 共 {n_live} 筆 live hitlimit 買單 — 立即到券商端手動撤單,並查 journal「撤單未確認/放棄」{RST}")
    if n_err:
        print(f"  {YEL}⚠ {n_err} 台查詢失敗 (SSH/登入/流量控管) — 稍後重跑{RST}")
    sys.exit(1)


if __name__ == "__main__":
    main()
