#!/usr/bin/env bash
# hit_limit_up VPS 部署 script — 在 VPS (root@45.76.222.150) 上執行
#
# 用法:
#   ./deploy_vps.sh [fubon_neo_whl路徑]
#   whl 路徑不給的話會在 /opt/hit_limit_up/wheels/ 找 fubon_neo*manylinux*.whl
#
# 前置 (從 Windows 先 scp 上來，見 README_DEPLOY.md):
#   /opt/hit_limit_up/secrets/.env          — 憑證設定 (PFX 路徑要指到 certs/)
#   /opt/hit_limit_up/certs/*.pfx           — 兩帳號憑證
#   /opt/hit_limit_up/wheels/fubon_neo*.whl — manylinux whl
#   (無 node 時) /opt/hit_limit_up/dist_upload/ — 本機 build 好的前端 dist
set -euo pipefail

BASE=/opt/hit_limit_up
REPO_DIR=$BASE/repo
APP_DIR=$REPO_DIR/hit_limit_up
VENV=$BASE/venv
REPO_URL=https://github.com/TeralionTech/twse_day_trade.git
BRANCH=limit_up

info()  { echo -e "\033[1;34m[deploy]\033[0m $*"; }
warn()  { echo -e "\033[1;33m[warn]\033[0m $*"; }
die()   { echo -e "\033[1;31m[error]\033[0m $*"; exit 1; }

# root 直接跑就不用 sudo
SUDO="sudo"
[[ $EUID -eq 0 ]] && SUDO=""

# ─── 0. 環境檢查 ─────────────────────────────────────────────
info "檢查環境..."

TZ_NOW=$(timedatectl show -p Timezone --value 2>/dev/null || cat /etc/timezone 2>/dev/null || echo unknown)
if [[ "$TZ_NOW" != "Asia/Taipei" ]]; then
    warn "Timezone 是 $TZ_NOW 不是 Asia/Taipei — APScheduler 8:00 cron 會跑錯時間!"
    warn "修正: $SUDO timedatectl set-timezone Asia/Taipei"
    read -rp "仍要繼續? [y/N] " ans; [[ "$ans" == "y" ]] || exit 1
fi

command -v python3 >/dev/null || die "缺 python3"
PYVER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
info "python3 = $PYVER, timezone = $TZ_NOW"

if ss -tlnp 2>/dev/null | grep -q ':8100 '; then
    warn "port 8100 已被佔用:"
    ss -tlnp | grep ':8100 '
    read -rp "仍要繼續? [y/N] " ans; [[ "$ans" == "y" ]] || exit 1
fi

# ─── 1. 目錄 + clone/pull ────────────────────────────────────
if [[ ! -d $BASE ]]; then
    info "建 $BASE (需要 sudo)..."
    $SUDO mkdir -p $BASE
    $SUDO chown "$USER" $BASE
fi
mkdir -p $BASE/secrets $BASE/certs $BASE/wheels

if [[ -d $REPO_DIR/.git ]]; then
    info "repo 已存在 → git pull"
    git -C $REPO_DIR fetch origin $BRANCH
    git -C $REPO_DIR checkout $BRANCH
    git -C $REPO_DIR reset --hard origin/$BRANCH
else
    info "clone $REPO_URL ($BRANCH branch)..."
    git clone -b $BRANCH "$REPO_URL" $REPO_DIR
fi

# ─── 2. Python venv + deps ───────────────────────────────────
if [[ ! -d $VENV ]]; then
    info "建 venv..."
    python3 -m venv $VENV
fi

WHL="${1:-}"
if [[ -z "$WHL" ]]; then
    WHL=$(ls $BASE/wheels/fubon_neo*manylinux*.whl 2>/dev/null | head -1 || true)
fi
if ! $VENV/bin/python -c 'import fubon_neo' 2>/dev/null; then
    [[ -n "$WHL" && -f "$WHL" ]] || die "找不到 fubon_neo manylinux whl — scp 到 $BASE/wheels/ 或當參數給"
    info "安裝 $WHL ..."
    $VENV/bin/pip install "$WHL"
else
    info "fubon_neo 已安裝，跳過 whl"
fi

info "安裝 Python deps..."
$VENV/bin/pip install -q --upgrade pip
$VENV/bin/pip install -q python-dotenv fastapi 'uvicorn[standard]' apscheduler pydantic

# ─── 3. 前端 ─────────────────────────────────────────────────
if [[ -f $APP_DIR/frontend/dist/index.html ]]; then
    info "前端 dist 已存在 (repo/上次 build)"
elif command -v node >/dev/null && command -v npm >/dev/null; then
    info "node $(node --version) → build 前端..."
    (cd $APP_DIR/frontend && npm ci && npm run build)
elif [[ -f $BASE/dist_upload/index.html ]]; then
    info "無 node，用上傳的 dist_upload/"
    mkdir -p $APP_DIR/frontend/dist
    cp -r $BASE/dist_upload/* $APP_DIR/frontend/dist/
else
    die "無 node 也無 $BASE/dist_upload/ — 本機 build 後 scp dist 上來 (見 README)"
fi

# ─── 4. Secrets 接線 ─────────────────────────────────────────
[[ -f $BASE/secrets/.env ]] || die "缺 $BASE/secrets/.env — 從 Windows scp 上來 (見 README)"
ls $BASE/certs/*.pfx >/dev/null 2>&1 || die "缺 $BASE/certs/*.pfx 憑證"
chmod 600 $BASE/secrets/.env $BASE/certs/*.pfx

# .env symlink 到 app 目錄 (config.py 讀 script 同層 .env)
ln -sf $BASE/secrets/.env $APP_DIR/.env
info "secrets OK (.env symlink + certs)"

# 檢查 .env 內 PFX 路徑指到 VPS 位置
if grep -qE 'PFX_PATH=.*[A-Z]:[\\/]' $BASE/secrets/.env; then
    warn ".env 內 PFX_PATH 看起來還是 Windows 路徑! 要改成 $BASE/certs/xxx.pfx"
    grep 'PFX_PATH' $BASE/secrets/.env || true
    exit 1
fi

# ─── 5. systemd unit ─────────────────────────────────────────
info "安裝 systemd unit (User=$(whoami))..."
sed "s/^User=.*/User=$(whoami)/" $APP_DIR/deploy/hit-limit-up.service | \
    $SUDO tee /etc/systemd/system/hit-limit-up.service >/dev/null
$SUDO systemctl daemon-reload
$SUDO systemctl enable --now hit-limit-up
sleep 2
$SUDO systemctl status hit-limit-up --no-pager -l | head -12

# ─── 6. 驗證 ─────────────────────────────────────────────────
info "打 API 驗證..."
sleep 2
if curl -sf localhost:8100/api/status >/dev/null; then
    info "✓ localhost:8100/api/status OK"
    curl -s localhost:8100/api/status | head -c 300; echo
else
    warn "API 沒回應 — journalctl -u hit-limit-up -n 50 看 log"
fi

# ─── 7. cloudflared 指引 (不自動改，避免影響 day_trade_system) ─
echo
info "════ 最後一步: cloudflared 加 ingress (手動) ════"
if [[ -f /etc/cloudflared/config.yml ]]; then
    echo "現有 /etc/cloudflared/config.yml:"
    echo "----------------------------------------"
    $SUDO cat /etc/cloudflared/config.yml
    echo "----------------------------------------"
    cat <<'EOF'
在 ingress: 列表的 catch-all (http_status:404) **之前** 插入:

  - hostname: limitup.teraliontech.com
    service: http://localhost:8100

然後:
  cloudflared tunnel route dns <tunnel名或UUID> limitup.teraliontech.com
  systemctl restart cloudflared    # day_trade_system 斷線 <1 秒
EOF
else
    cat <<'EOF'
沒有 /etc/cloudflared/config.yml → tunnel 是 Cloudflare dashboard 管理:
  1. Zero Trust → Networks → Tunnels → 選現有 tunnel → Public Hostname → Add
  2. Subdomain: limitup / Domain: teraliontech.com
  3. Service: HTTP / localhost:8100 → Save
  (dashboard 管理零中斷，不用 restart)
EOF
fi
echo
info "完成後瀏覽器開 https://limitup.teraliontech.com 驗證"
