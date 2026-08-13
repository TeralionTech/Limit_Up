#!/usr/bin/env bash
# fetch_t30.sh — 每日 8:30 前從券商檔案主機抓 T30V.TSE / T30V.OTC 到本機 (limit-up VPS)。
#
# 鏈路 (limit-up VPS 連不到券商主機,要多經過一台中繼 VPS):
#   limit-up VPS --(ssh 金鑰)--> 中繼 VPS (JUMP_HOST, 例 root@45.32.53.219)
#                --(ssh 金鑰)--> 券商主機 (BROKER_HOST, Linux)
#                --(sftp 密碼)--> 檔案主機 10.83.11.100
# 主路徑: ProxyJump 逗號鏈一條命令打穿三跳 (sshpass 只需裝在 limit-up VPS)
# 備援:   券商主機禁 TCP forwarding 時,ssh -J 進券商主機跑 sftp 再 scp 回來
#         (需券商主機也有 sshpass)
# 金鑰需求: limit-up VPS 的公鑰要同時授權在「中繼 VPS」與「券商主機」
#           (中繼 VPS → 券商主機 這段由 ProxyJump 隧道打穿,不需中繼機本身有金鑰)
#
# 設定檔: /opt/hit_limit_up/secrets/t30.env (chmod 600,不進 git):
#   JUMP_HOST=root@45.32.53.219               (中繼 VPS;留空 = 不經中繼直連)
#   BROKER_HOST=user@券商主機IP
#   SFTP_HOST=10.83.11.100
#   SFTP_USER=kc
#   SFTP_PASS=********
#   DEST_DIR=/opt/hit_limit_up/repo/input/t30   (可省略,預設此值)
#
# 行為: timer 08:05 起跑 (券商 08:00 更新檔,留 5 分鐘餘裕;太早取會拿到昨日內容),
#       08:25 前每 3 分鐘重試;抓到先驗檔 (size 為 100 倍數 = T30 定長記錄)
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
JUMP_HOST="${JUMP_HOST:-}"
BROKER_PORT="${BROKER_PORT:-22}"     # 券商主機 SSH port (實際環境是 3350)
SFTP_DIR="${SFTP_DIR:-}"             # 檔案主機上 T30V.* 所在目錄 (空 = 登入預設目錄)
RPATH="${SFTP_DIR:+$SFTP_DIR/}"      # 遠端路徑前綴

command -v sshpass >/dev/null || { echo "[fetch_t30] VPS 缺 sshpass: apt-get install -y sshpass"; exit 1; }
mkdir -p "$DEST_DIR"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=15"
# ProxyJump 鏈: 有中繼 VPS → "中繼,券商主機:port";沒有 → 只有券商主機
if [ -n "$JUMP_HOST" ]; then
    JUMP_CHAIN="$JUMP_HOST,$BROKER_HOST:$BROKER_PORT"
    BROKER_JUMP_OPT="-o ProxyJump=$JUMP_HOST"     # 備援路徑連券商主機用
else
    JUMP_CHAIN="$BROKER_HOST:$BROKER_PORT"
    BROKER_JUMP_OPT=""
fi

fetch_proxyjump() {
    local f
    for f in T30V.TSE T30V.OTC; do
        # shellcheck disable=SC2086
        sshpass -p "$SFTP_PASS" sftp $SSH_OPTS -o ProxyJump="$JUMP_CHAIN" \
            "$SFTP_USER@$SFTP_HOST:${RPATH}$f" "$TMP/" || return 1
    done
}

fetch_twohop() {
    # 備援 — 密碼會出現在券商主機的行程列表,僅在 ProxyJump 全鏈不可用時使用
    # shellcheck disable=SC2086
    ssh -p "$BROKER_PORT" $SSH_OPTS $BROKER_JUMP_OPT "$BROKER_HOST" "
        command -v sshpass >/dev/null || { echo '券商主機缺 sshpass (備援路徑需要)'; exit 9; }
        rm -f /tmp/T30V.TSE /tmp/T30V.OTC
        sshpass -p '$SFTP_PASS' sftp -o StrictHostKeyChecking=accept-new $SFTP_USER@$SFTP_HOST:${RPATH}T30V.TSE /tmp/ &&
        sshpass -p '$SFTP_PASS' sftp -o StrictHostKeyChecking=accept-new $SFTP_USER@$SFTP_HOST:${RPATH}T30V.OTC /tmp/
    " || return 1
    # shellcheck disable=SC2086
    scp -P "$BROKER_PORT" $SSH_OPTS $BROKER_JUMP_OPT \
        "$BROKER_HOST:/tmp/T30V.TSE" "$BROKER_HOST:/tmp/T30V.OTC" "$TMP/" || return 1
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
