#!/usr/bin/env bash
# 在**目標主機**上執行 (由 CI deploy job 先 rsync 好 repo + frontend/dist 後呼叫)。
# 更新用: 同步 systemd unit → 重啟主服務 → 確保夜間 timer 已排程。
# secrets (.env / certs / venv) 已存在 (bootstrap 建立過),這裡不碰。
set -euo pipefail
APP=/opt/hit_limit_up/repo

cp "$APP"/deploy/hit-limit-up.service /etc/systemd/system/
cp "$APP"/deploy/hit-limit-up-avgvol.service "$APP"/deploy/hit-limit-up-avgvol.timer /etc/systemd/system/
if [ -f "$APP"/deploy/hit-limit-up-t30.timer ]; then
    cp "$APP"/deploy/hit-limit-up-t30.service "$APP"/deploy/hit-limit-up-t30.timer /etc/systemd/system/ || true
fi

systemctl daemon-reload
systemctl restart hit-limit-up
systemctl enable --now hit-limit-up-avgvol.timer >/dev/null 2>&1 || true

sleep 2
systemctl is-active hit-limit-up
echo "deploy OK on $(hostname) — $(cat "$APP"/.deployed_commit 2>/dev/null || echo '?')"
