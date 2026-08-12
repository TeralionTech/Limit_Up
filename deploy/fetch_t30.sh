#!/usr/bin/env bash
# fetch_t30.sh — 每日 8:30 前從券商檔案主機抓 T30V.TSE / T30V.OTC 到 VPS。
#
# 鏈路: VPS --(ssh 金鑰)--> 券商主機(Linux) --(sftp 密碼)--> 10.83.11.100
# 主路徑: ProxyJump 一條命令打穿兩跳 (sshpass 只需裝在 VPS: apt-get install -y sshpass)
# 備援:   券商主機禁 TCP forwarding 時,ssh 進去跑 sftp batch 再 scp 回來
#         (需券商主機也有 sshpass)
#
# 設定檔: /opt/hit_limit_up/secrets/t30.env (chmod 600,不進 git):
#   BROKER_HOST=user@券商主機IP
#   SFTP_HOST=10.83.11.100
#   SFTP_USER=kc
#   SFTP_PASS=********
#   DEST_DIR=/opt/hit_limit_up/repo/input/t30   (可省略,預設此值)
#
# 行為: 08:25 前每 3 分鐘重試;抓到先驗檔 (size 為 100 倍數 = T30 定長記錄)
#       才原子覆蓋;全失敗 → exit 1 並保留舊檔 (runner 端會對舊檔 CRITICAL)。
set -u

ENV_FILE="${ENV_FILE:-/opt/hit_limit_up/secrets/t30.env}"
[ -f "$ENV_FILE" ] || { echo "[fetch_t30] 缺設定檔 $ENV_FILE"; exit 1; }
# shellcheck disable=SC1090
. "$ENV_FILE"
: "${BROKER_HOST:?t30.env 缺 BROKER_HOST}"
: "${SFTP_HOST:?t30.env 缺 SFTP_HOST}"
: "${SFTP_USER:?t30.env 缺 SFTP_USER}"
: "${SFTP_PASS:?t30.env 缺 SFTP_PASS}"
DEST_DIR="${DEST_DIR:-/opt/hit_limit_up/repo/input/t30}"
DEADLINE="${DEADLINE:-08:25}"
RETRY_SEC="${RETRY_SEC:-180}"

command -v sshpass >/dev/null || { echo "[fetch_t30] VPS 缺 sshpass: apt-get install -y sshpass"; exit 1; }
mkdir -p "$DEST_DIR"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=15"

fetch_proxyjump() {
    local f
    for f in T30V.TSE T30V.OTC; do
        # shellcheck disable=SC2086
        sshpass -p "$SFTP_PASS" sftp $SSH_OPTS -o ProxyJump="$BROKER_HOST" \
            "$SFTP_USER@$SFTP_HOST:$f" "$TMP/" || return 1
    done
}

fetch_twohop() {
    # 備援 — 密碼會出現在券商主機的行程列表,僅在 ProxyJump 不可用時使用
    # shellcheck disable=SC2086
    ssh $SSH_OPTS "$BROKER_HOST" "
        command -v sshpass >/dev/null || { echo '券商主機缺 sshpass (備援路徑需要)'; exit 9; }
        rm -f /tmp/T30V.TSE /tmp/T30V.OTC
        sshpass -p '$SFTP_PASS' sftp -o StrictHostKeyChecking=accept-new $SFTP_USER@$SFTP_HOST:T30V.TSE /tmp/ &&
        sshpass -p '$SFTP_PASS' sftp -o StrictHostKeyChecking=accept-new $SFTP_USER@$SFTP_HOST:T30V.OTC /tmp/
    " || return 1
    # shellcheck disable=SC2086
    scp $SSH_OPTS "$BROKER_HOST:/tmp/T30V.TSE" "$BROKER_HOST:/tmp/T30V.OTC" "$TMP/" || return 1
}

valid() {  # $1=檔案 → size > 0 且為 100 的整數倍 (T30 定長記錄)
    local sz
    sz=$(stat -c%s "$1" 2>/dev/null || echo 0)
    [ "$sz" -gt 0 ] && [ $((sz % 100)) -eq 0 ]
}

while :; do
    rm -f "$TMP/T30V.TSE" "$TMP/T30V.OTC"
    if fetch_proxyjump || fetch_twohop; then
        if valid "$TMP/T30V.TSE" && valid "$TMP/T30V.OTC"; then
            mv -f "$TMP/T30V.TSE" "$DEST_DIR/T30V.TSE"
            mv -f "$TMP/T30V.OTC" "$DEST_DIR/T30V.OTC"
            echo "[fetch_t30] OK $(date '+%F %T') → $DEST_DIR"
            exit 0
        fi
        echo "[fetch_t30] 檔案驗證失敗 (size 非 100 倍數) — 不覆蓋舊檔"
    else
        echo "[fetch_t30] 取檔失敗 $(date '+%T')"
    fi
    if [ "$(date +%H:%M)" \> "$DEADLINE" ] || [ "$(date +%H:%M)" = "$DEADLINE" ]; then
        echo "[fetch_t30] 超過 $DEADLINE 放棄 — 沿用舊檔 (runner 會 CRITICAL 提示)"
        exit 1
    fi
    echo "[fetch_t30] ${RETRY_SEC}s 後重試..."
    sleep "$RETRY_SEC"
done
