#!/usr/bin/env bash
set -euo pipefail

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

template="$(cat pgloader-config.load)"

# ---------- 校验并替换 ----------
for var in "${required_vars[@]}"; do
  value="${!var:-}"

  if [[ -z "$value" ]]; then
    echo "❌ Missing required env var: $var" >&2
    exit 1
  fi

  template="${template//\$\{$var\}/$value}"
done

# ---------- 写入临时文件 ----------
TMP_LOAD_FILE="$(mktemp)"
printf '%s\n' "$template" > "$TMP_LOAD_FILE"

# ---------- 执行 pgloader ----------
echo "▶ Starting pgloader migration..."
# MySQL SSL 证书验证失败（AWS RDS 使用自签名证书）。需要在运行 pgloader 时添加 --no-ssl-cert-verification 参数。
pgloader --no-ssl-cert-verification "$TMP_LOAD_FILE"

rm -f "$TMP_LOAD_FILE"
