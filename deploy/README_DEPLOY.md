# hit_limit_up 部署到 VPS (root@45.76.222.150)

目標: `https://limitup.teraliontech.com` (與 day_trade_system 同機、同一個 cloudflare tunnel 不同 hostname)

架構: systemd 跑 `server.py` (uvicorn port 8100，static serve 前端) → cloudflared ingress 導流。

---

## Step 1 — Windows 端: 上傳檔案

在**本機 PowerShell** (`experiment/Limit_Up` 目錄) 執行:

```powershell
# 建目錄
ssh root@45.76.222.150 "mkdir -p /opt/hit_limit_up/secrets /opt/hit_limit_up/certs /opt/hit_limit_up/wheels /opt/hit_limit_up/dist_upload"
# 若 /opt/hit_limit_up 不存在會失敗 → 先: ssh root@45.76.222.150 "sudo mkdir -p /opt/hit_limit_up && sudo chown \$USER /opt/hit_limit_up"

# 1. .env (上傳後記得改 PFX 路徑，見下)
scp .env root@45.76.222.150:/opt/hit_limit_up/secrets/.env

# 2. 兩帳號 PFX 憑證
scp O100596041.pfx root@45.76.222.150:/opt/hit_limit_up/certs/
scp <副帳號.pfx>   root@45.76.222.150:/opt/hit_limit_up/certs/

# 3. fubon_neo manylinux whl
scp <path-to>\fubon_neo-*-manylinux*.whl root@45.76.222.150:/opt/hit_limit_up/wheels/

# 4. 前端 dist (VPS 沒 node 才需要；先本機 build)
cd frontend; npm run build; cd ..
scp -r frontend/dist/* root@45.76.222.150:/opt/hit_limit_up/dist_upload/
```

### 改 `.env` (SSH 進去改，或上傳前先改)

```bash
FUBON_PFX_PATH=/opt/hit_limit_up/certs/O100596041.pfx      # ← Windows 路徑改成這樣
FUBON_PFX_PATH_2=/opt/hit_limit_up/certs/<副帳號>.pfx
SKIP_TRADER=false        # trader 已是純監控 (不下單)，開了才有模擬執行頁資料
```

---

## Step 2 — VPS 端: 跑 deploy script

```bash
ssh root@45.76.222.150
# 第一次: 先拿 script (repo 還沒 clone)
curl -fsSL https://raw.githubusercontent.com/TeralionTech/Limit_Up/main/deploy/deploy_vps.sh -o /tmp/deploy_vps.sh
# (private repo curl 拿不到的話: 本機 scp deploy/deploy_vps.sh root@45.76.222.150:/tmp/)
chmod +x /tmp/deploy_vps.sh
/tmp/deploy_vps.sh
```

Script 會做: 環境檢查 (timezone/port) → clone `main` branch (repo=`TeralionTech/Limit_Up`) →
venv + whl + deps → 前端 → secrets 檢查 → systemd unit 安裝啟動 → API 驗證 → 印 cloudflared 指引。

之後更新版本只要重跑同一支 script (會 git pull + restart)。`output/` 為 gitignore,更新不會動到。

---

## Step 3 — cloudflared 加 ingress

Script 最後會印出現有 config。兩種情況:

**A. config.yml 管理** (`/etc/cloudflared/config.yml` 存在):

在 `ingress:` 列表 catch-all (`http_status:404`) **之前**插入:

```yaml
  - hostname: limitup.teraliontech.com
    service: http://localhost:8100
```

```bash
cloudflared tunnel route dns <tunnel名或UUID> limitup.teraliontech.com
sudo systemctl restart cloudflared     # day_trade_system 斷 <1 秒
```

**B. Dashboard 管理** (無 config.yml):
Zero Trust → Networks → Tunnels → 現有 tunnel → Public Hostname → Add:
`limitup` . `teraliontech.com` → Service `HTTP` `localhost:8100` → Save (零中斷)

---

## Step 4 — 驗證 checklist

- [ ] `systemctl status hit-limit-up` = active (running)
- [ ] `curl localhost:8100/api/status` 回 JSON (phase=idle)
- [ ] 瀏覽器 `https://limitup.teraliontech.com` → 三分頁 UI
- [ ] `timedatectl` = Asia/Taipei
- [ ] 隔日 8:00 自動觸發: `journalctl -u hit-limit-up --since 07:55 | head -50`
      應看到 login → universe → 抓漲停 (或讀 cache)
- [ ] 8:30 後 heartbeat log: `journalctl -u hit-limit-up -f | grep heartbeat`

## 常用維運

```bash
journalctl -u hit-limit-up -f            # 看即時 log
sudo systemctl restart hit-limit-up      # 重啟
/tmp/deploy_vps.sh                       # 更新到 limit_up branch 最新 + 重啟
ls /opt/hit_limit_up/repo/output/        # 每日 JSON/JSONL 輸出 (repo 根,獨立 repo 後不再有 /hit_limit_up 子層)
```

## 一次性遷移: 從 twse_day_trade monorepo 切到 Limit_Up 獨立 repo

VPS 上舊的 `/opt/hit_limit_up/repo` 是 clone 自 `twse_day_trade` (monorepo,app 在
`repo/hit_limit_up/` 子層)。獨立 repo 後 app 變 repo 根。**必須保留 `output/`**
(內含 `overnight_holdings.json` 隔日賣清單 + 下單 CSV,刪了會失去留倉追蹤)。

**收盤/週末做** (會重啟服務)。在 VPS:

```bash
# 0) 停服務
systemctl stop hit-limit-up

# 1) 備份舊 output (在舊的子層路徑)
cp -a /opt/hit_limit_up/repo/hit_limit_up/output /opt/hit_limit_up/output_backup

# 2) 移除舊 repo,clone 新的獨立 repo
rm -rf /opt/hit_limit_up/repo
git clone https://github.com/TeralionTech/Limit_Up.git /opt/hit_limit_up/repo

# 3) 還原 output 到新的 repo 根
mv /opt/hit_limit_up/output_backup /opt/hit_limit_up/repo/output

# 4) 還原 .env symlink (config.py 讀 server.py 同層 .env)
ln -sf /opt/hit_limit_up/secrets/.env /opt/hit_limit_up/repo/.env

# 5) 前端 dist: 新 clone 無 dist (gitignore) → 本機 build 後 scp 到 dist_upload,
#    再由 deploy script 佈署;或直接:
#    (本機) scp -r frontend/dist/* root@45.76.222.150:/opt/hit_limit_up/dist_upload/

# 6) 跑更新後的 deploy script (會裝新 systemd unit: WorkingDirectory=/opt/hit_limit_up/repo)
curl -fsSL https://raw.githubusercontent.com/TeralionTech/Limit_Up/main/deploy/deploy_vps.sh -o /tmp/deploy_vps.sh
chmod +x /tmp/deploy_vps.sh
/tmp/deploy_vps.sh

# 7) 驗證留倉清單還在
cat /opt/hit_limit_up/repo/output/overnight_holdings.json
systemctl status hit-limit-up --no-pager | head
```

之後日常更新就回到 Step 2 的重跑 script 即可 (git pull + restart,不動 output)。

## 注意

- 與 day_trade_system **不同**富邦帳號 → WS socket 額度無衝突 (各自 5/帳號)
- 8:00-9:00 為 CPU/RAM 高峰 (10 sockets + 2600 檔 REST)，與交易系統同機請觀察負載
- `output/` 會每日長 tick JSONL (可能數百 MB/日)，滿了要清: `find output -name '*_ticks.jsonl' -mtime +7 -delete`
