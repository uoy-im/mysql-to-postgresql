#!/usr/bin/env bash
set -euo pipefail

# ---------- 1. 检查必要环境变量 ----------
required_vars=(
  MYSQL_DB
  MYSQL_HOST
  MYSQL_PASSWORD
  MYSQL_PORT
  MYSQL_USER
  PG_DB
  PG_ENDPOINT_ID
  PG_PASSWORD
  PG_REGION
  PG_USER
)

for v in "${required_vars[@]}"; do
  if [[ -z "${!v:-}" ]]; then
    echo "❌ Missing required env var: $v" >&2
    exit 1
  fi
done

# ---------- 2. 生成临时 pgloader load 文件（变量展开） ----------
TMP_LOAD_FILE="$(mktemp)"

cat <<EOF > "$TMP_LOAD_FILE"
$(cat pgloader-config.load)
EOF

# ---------- 3. 执行迁移 ----------
echo "▶ Starting pgloader migration..."
pgloader "$TMP_LOAD_FILE"

# ---------- 4. 清理 ----------
rm -f "$TMP_LOAD_FILE"
echo "✅ Migration finished"
