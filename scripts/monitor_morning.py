"""明早 Hub/Node GO/NO-GO 監控 (本機跑,SSH 進各主機 curl localhost:8100)。

用法 (本機 Windows):
  python scripts/monitor_morning.py                 # 讀 ../monitor_hosts.json (repo 外)
  python scripts/monitor_morning.py --hosts path.json --once     # 單輪測試

hosts JSON 格式 (機密留在本機檔案,**不進 repo**;上線後可刪):
  [{"host": "1.2.3.4", "password": "...", "role": "hub", "name": "hub"},
   {"host": "5.6.7.8", "password": "...", "role": "node", "name": "node1"}, ...]

輪詢: 08:00–09:05 每 5s (08:59:40–09:00:10 加密到 1s)。每輪印:
  hub: phase / universe / limit_up 進度 / T30 count / marked(final?)
  node×4: phase / mode+armed+connected / 自己的 marked
關鍵斷言 (紅字 → 不 arm):
  ① 08:05 後 hub phase 非 idle/error   ② 08:30 後 limit_ups 抓完
  ③ T30 count>0 (=0 黃色警告)          ④ ≥08:59:50 hub final=true 且 marked 非空可交易
  ⑤ ≥08:59:52 每個 node marked == hub marked (T30 檔會在預掛時被 node 擋,清單仍相同)
GO 條件全綠 → 你在 08:59:58 前到各 node UI 切 real+設預算+arm。任何紅 → 不 arm (sim=硬煞車)。
"""
import argparse
import json
import sys
import time
from datetime import datetime, time as dtime
from pathlib import Path

try:
    import paramiko
except ImportError:
    print("需要 paramiko: pip install paramiko")
    sys.exit(1)

GREEN, RED, YEL, RST = "\033[92m", "\033[91m", "\033[93m", "\033[0m"


def _curl(ssh, path):
    """在遠端 curl localhost:8100{path},回 dict 或 None。"""
    try:
        _i, out, _e = ssh.exec_command(f"curl -s -m 4 localhost:8100{path}", timeout=8)
        raw = out.read().decode("utf-8", "replace").strip()
        return json.loads(raw) if raw else None
    except Exception:
        return None


class Host:
    def __init__(self, spec):
        self.host = spec["host"]
        self.password = spec["password"]
        self.role = spec.get("role", "node")
        self.name = spec.get("name", self.host)
        self.ssh = None

    def connect(self):
        if self.ssh is not None:
            try:
                self.ssh.get_transport().send_ignore()
                return True
            except Exception:
                self.ssh = None
        try:
            c = paramiko.SSHClient()
            c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            c.connect(self.host, username="root", password=self.password,
                      timeout=10, banner_timeout=10, auth_timeout=10)
            self.ssh = c
            return True
        except Exception:
            self.ssh = None
            return False

    def poll(self):
        if not self.connect():
            return {"error": "SSH 連不上"}
        return {
            "status": _curl(self.ssh, "/api/status"),
            "snap": _curl(self.ssh, "/api/marked-snapshot"),
            "trading": _curl(self.ssh, "/api/trading/status"),
            "t30": _curl(self.ssh, "/api/t30") if self.role == "hub" else None,
        }


def _fmt_hub(r, now_t):
    if "error" in r:
        return f"{RED}HUB {r['error']}{RST}", False
    st, snap, t30 = r.get("status") or {}, r.get("snap") or {}, r.get("t30") or {}
    phase = st.get("phase", "?")
    prog = st.get("limit_up_progress") or {}
    marked = [s["symbol"] for s in (snap.get("symbols") or [])]
    final = snap.get("final")
    ok = True
    p_c = GREEN if phase not in ("idle", "error") else (RED if now_t >= dtime(8, 5) else YEL)
    if p_c == RED:
        ok = False
    lu_done = prog.get("done", 0) == prog.get("total", 0) and prog.get("total", 0) > 0
    lu_c = GREEN if lu_done else (RED if now_t >= dtime(8, 35) else YEL)
    if lu_c == RED:
        ok = False
    t30_n = t30.get("count", 0)
    t30_c = GREEN if t30_n > 0 else YEL
    fin_c = GREEN if final else (RED if now_t >= dtime(8, 59, 51) else "")
    if fin_c == RED:
        ok = False
    line = (f"HUB  {p_c}phase={phase}{RST} 母體={st.get('universe_size', '?')} "
            f"{lu_c}limit_ups={prog.get('done', 0)}/{prog.get('total', 0)}{RST} "
            f"{t30_c}T30={t30_n}{RST} "
            f"{fin_c}final={final}{RST} marked({len(marked)})={' '.join(marked) or '—'}")
    return line, ok


def _fmt_node(name, r, hub_marked, now_t):
    if "error" in r:
        return f"{RED}{name} {r['error']}{RST}", False
    st, snap, tr = r.get("status") or {}, r.get("snap") or {}, r.get("trading") or {}
    marked = [s["symbol"] for s in (snap.get("symbols") or [])]
    ok = True
    match = ""
    if now_t >= dtime(8, 59, 52) and hub_marked is not None:
        if set(marked) == set(hub_marked):
            match = f" {GREEN}==hub✓{RST}"
        else:
            match = f" {RED}≠hub! node={marked} hub={hub_marked}{RST}"
            ok = False
    mode, armed = tr.get("mode", "?"), tr.get("armed")
    conn = tr.get("connected")
    arm_c = GREEN if (mode == "real" and armed) else YEL
    line = (f"{name} phase={st.get('phase', '?')} "
            f"{arm_c}{mode}/{'ARMED' if armed else 'unarmed'}{RST}"
            f"/{'conn' if conn else 'NOCONN'} marked({len(marked)})={' '.join(marked) or '—'}{match}")
    return line, ok


def main():
    ap = argparse.ArgumentParser()
    default_hosts = Path(__file__).resolve().parent.parent.parent / "monitor_hosts.json"
    ap.add_argument("--hosts", default=str(default_hosts))
    ap.add_argument("--once", action="store_true", help="只跑一輪 (連線測試)")
    args = ap.parse_args()

    specs = json.loads(Path(args.hosts).read_text(encoding="utf-8"))
    hosts = [Host(s) for s in specs]
    hub = next((h for h in hosts if h.role == "hub"), None)
    nodes = [h for h in hosts if h.role != "hub"]

    print(f"監控 {len(hosts)} 台 (hub={hub.host if hub else '無'} + {len(nodes)} node)。Ctrl+C 停。")
    while True:
        now = datetime.now()
        now_t = now.time()
        all_ok = True
        hub_marked = None
        print(f"\n──── {now.strftime('%H:%M:%S')} ────")
        if hub:
            r = hub.poll()
            line, ok = _fmt_hub(r, now_t)
            all_ok &= ok
            snap = r.get("snap") or {}
            if snap.get("final"):
                hub_marked = [s["symbol"] for s in (snap.get("symbols") or [])]
            print(" " + line)
        for h in nodes:
            line, ok = _fmt_node(h.name, h.poll(), hub_marked, now_t)
            all_ok &= ok
            print(" " + line)
        tag = f"{GREEN}GO ✅{RST}" if all_ok else f"{RED}NO-GO ❌ (不 arm){RST}"
        print(f" ⇒ {tag}")
        if args.once:
            break
        if now_t >= dtime(9, 5):
            print("09:05 到,監控結束。")
            break
        dense = dtime(8, 59, 40) <= now_t <= dtime(9, 0, 10)
        time.sleep(1 if dense else 5)


if __name__ == "__main__":
    main()
