#!/usr/bin/env bash
# 在**目標主機**上執行 (由 CI bootstrap job 先 rsync 好 repo + frontend/dist 後呼叫)。
# 首次安裝: OS deps → venv + Python deps + fubon_neo whl → .env symlink → systemd units → 啟動。
#
# ⚠ 前置 (人工先做一次,secrets 不進 git/CI):
#     mkdir -p /opt/hit_limit_up/{secrets,certs,wheels}
#     scp 上傳: secrets/.env (PFX 路徑改 Linux) + certs/*.pfx + wheels/fubon_neo*manylinux*.whl
set -euo pipefail
BASE=/opt/hit_limit_up
APP=$BASE/repo

# 0) 時區檢查 (APScheduler 8:00 靠系統時區)
tz=$(timedatectl show -p Timezone --value 2>/dev/null || cat /etc/timezone 2>/dev/null || echo unknown)
[ "$tz" = "Asia/Taipei" ] || echo "⚠ timezone=$tz 非 Asia/Taipei — 請 timedatectl set-timezone Asia/Taipei"

# 1) OS deps
if command -v apt-get >/dev/null; then
    apt-get update -qq && apt-get install -y -qq python3-venv git rsync || true
fi
mkdir -p "$BASE"/secrets "$BASE"/certs "$BASE"/wheels "$APP"/frontend

# 2) secrets 檢查 (缺就中止 — 要人工先上傳)
[ -f "$BASE"/secrets/.env ] || { echo "✗ 缺 $BASE/secrets/.env — 請先人工 scp 上傳 secrets"; exit 2; }
ls "$BASE"/certs/*.pfx >/dev/null 2>&1 || { echo "✗ 缺 $BASE/certs/*.pfx 憑證"; exit 2; }
if grep -qE 'PFX_PATH=.*[A-Za-z]:[\\/]' "$BASE"/secrets/.env; then
    echo "✗ .env 內 PFX_PATH 還是 Windows 路徑 — 要改成 $BASE/certs/xxx.pfx"; exit 2
fi

# 3) venv + Python deps + fubon_neo whl
[ -d "$BASE"/venv ] || python3 -m venv "$BASE"/venv
"$BASE"/venv/bin/pip install -q --upgrade pip
"$BASE"/venv/bin/pip install -q -r "$APP"/requirements.txt
if ! "$BASE"/venv/bin/python -c 'import fubon_neo' 2>/dev/null; then
    whl=$(ls "$BASE"/wheels/fubon_neo*manylinux*.whl 2>/dev/null | head -1)
    [ -n "$whl" ] || { echo "✗ 缺 fubon_neo manylinux whl (放 $BASE/wheels/)"; exit 2; }
    "$BASE"/venv/bin/pip install -q "$whl"
fi

# 4) .env symlink (config.py 讀 server.py 同層 .env) + 權限
ln -sf "$BASE"/secrets/.env "$APP"/.env
chmod 600 "$BASE"/secrets/.env "$BASE"/certs/*.pfx

# 5) systemd units (主服務 + 夜間 avgvol timer + t30 timer)
cp "$APP"/deploy/hit-limit-up.service /etc/systemd/system/
cp "$APP"/deploy/hit-limit-up-avgvol.service "$APP"/deploy/hit-limit-up-avgvol.timer /etc/systemd/system/
if [ -f "$APP"/deploy/hit-limit-up-t30.timer ]; then
    cp "$APP"/deploy/hit-limit-up-t30.service "$APP"/deploy/hit-limit-up-t30.timer /etc/systemd/system/ || true
fi
systemctl daemon-reload
systemctl enable --now hit-limit-up
systemctl enable --now hit-limit-up-avgvol.timer
[ -f /etc/systemd/system/hit-limit-up-t30.timer ] && systemctl enable --now hit-limit-up-t30.timer || true

sleep 2
systemctl is-active hit-limit-up
echo "bootstrap OK on $(hostname)"
