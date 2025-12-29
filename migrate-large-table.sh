#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# 大表流式迁移脚本
# 用于迁移超大表，pgloader 会内存溢出，改用 mysql + psql 流式迁移
#
# 用法: bash migrate-large-table.sh <表名>
# 示例: bash migrate-large-table.sh text_content
#       bash migrate-large-table.sh pipeline_snapshot
# ============================================================================

if [[ $# -lt 1 ]]; then
  echo "用法: $0 <表名>"
  echo "示例: $0 text_content"
  exit 1
fi

TABLE_NAME="$1"

# 记录开始时间
START_TIME=$(date +%s)
echo "▶ 开始迁移 ${TABLE_NAME} 表 ($(date '+%Y-%m-%d %H:%M:%S'))"
echo ""

echo "▶ 检查环境变量..."

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

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "❌ Missing required env var: $var" >&2
    exit 1
  fi
done

# 构建连接字符串
# 注意：psql 使用 options 参数传递 endpoint ID（与 pgloader 格式不同）
PG_CONN="postgres://${PG_USER}:${PG_PASSWORD}@${PG_ENDPOINT_ID}.${PG_REGION}.aws.neon.tech/${PG_DB}?sslmode=require&options=endpoint%3D${PG_ENDPOINT_ID}"

echo "▶ 测试 PostgreSQL 连接..."
if ! psql "$PG_CONN" -c "SELECT 1" > /dev/null 2>&1; then
  echo "❌ PostgreSQL 连接失败，请检查环境变量" >&2
  exit 1
fi
echo "✅ PostgreSQL 连接成功"

echo "▶ 测试 MySQL 连接..."
if ! mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -e "SELECT 1" > /dev/null 2>&1; then
  echo "❌ MySQL 连接失败，请检查环境变量" >&2
  exit 1
fi
echo "✅ MySQL 连接成功"

# ============================================================================
# 步骤 1：获取表结构并在 PostgreSQL 创建
# ============================================================================
echo "▶ 获取 ${TABLE_NAME} 表结构..."

# 获取列名列表（逗号分隔）
COLUMNS=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  -N -B -e "SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='$MYSQL_DB' AND TABLE_NAME='$TABLE_NAME'")

if [[ -z "$COLUMNS" ]]; then
  echo "❌ 表 ${TABLE_NAME} 不存在或没有列" >&2
  exit 1
fi

echo "   列: $COLUMNS"

# 获取 MySQL 建表语句
echo "▶ 获取 MySQL 建表语句..."
MYSQL_CREATE_TABLE=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  -N -B -e "SHOW CREATE TABLE \`$TABLE_NAME\`" | cut -f2)

# 转换为 PostgreSQL 语法（简化版，处理常见类型）
echo "▶ 创建 PostgreSQL 表结构..."
echo "   目标 schema: ${MYSQL_DB}"

# 生成 PostgreSQL 建表语句
PG_CREATE_TABLE=$(echo "$MYSQL_CREATE_TABLE" | \
  sed "s/\`//g" | \
  sed "s/CREATE TABLE /CREATE TABLE IF NOT EXISTS ${MYSQL_DB}./i" | \
  sed 's/bigint NOT NULL AUTO_INCREMENT/bigint PRIMARY KEY/gi' | \
  sed 's/int NOT NULL AUTO_INCREMENT/integer PRIMARY KEY/gi' | \
  sed 's/bigint/bigint/gi' | \
  sed 's/int(\([0-9]*\))/integer/gi' | \
  sed 's/tinyint(1)/boolean/gi' | \
  sed 's/tinyint(\([0-9]*\))/smallint/gi' | \
  sed 's/smallint(\([0-9]*\))/smallint/gi' | \
  sed 's/mediumint(\([0-9]*\))/integer/gi' | \
  sed 's/longtext/text/gi' | \
  sed 's/mediumtext/text/gi' | \
  sed 's/tinytext/text/gi' | \
  sed 's/varchar(\([0-9]*\))/varchar(\1)/gi' | \
  sed 's/datetime(\([0-9]*\))/timestamp(\1)/gi' | \
  sed 's/datetime/timestamp/gi' | \
  sed 's/json/jsonb/gi' | \
  sed 's/double/double precision/gi' | \
  sed 's/float/real/gi' | \
  sed 's/ unsigned//gi' | \
  sed 's/CHARACTER SET [a-zA-Z0-9_]*//gi' | \
  sed 's/COLLATE [a-zA-Z0-9_]*//gi' | \
  sed "s/COMMENT '[^']*'//gi" | \
  sed 's/ON UPDATE CURRENT_TIMESTAMP([0-9]*)//gi' | \
  sed 's/ON UPDATE CURRENT_TIMESTAMP//gi' | \
  sed 's/DEFAULT CURRENT_TIMESTAMP([0-9]*)/DEFAULT CURRENT_TIMESTAMP/gi' | \
  sed 's/ENGINE=[a-zA-Z]*//gi' | \
  sed 's/DEFAULT CHARSET=[a-zA-Z0-9]*//gi' | \
  sed 's/ROW_FORMAT=[a-zA-Z]*//gi' | \
  sed 's/AUTO_INCREMENT=[0-9]*//gi' | \
  sed '/^$/d' | \
  sed 's/,$//' | \
  grep -v "^\s*PRIMARY KEY" | \
  grep -v "^\s*KEY " | \
  grep -v "^\s*UNIQUE KEY" | \
  head -n -1)

# 添加结束括号
PG_CREATE_TABLE="${PG_CREATE_TABLE}
);"

psql "$PG_CONN" << EOF
-- 创建 schema（如果不存在）
CREATE SCHEMA IF NOT EXISTS ${MYSQL_DB};

-- 删除旧表（如果存在）
DROP TABLE IF EXISTS ${MYSQL_DB}.${TABLE_NAME} CASCADE;

-- 创建新表
${PG_CREATE_TABLE}
EOF

echo "✅ 表结构创建完成"

# ============================================================================
# 步骤 2：流式导入数据
# ============================================================================
echo "▶ 开始流式导入数据..."

# 获取总行数用于进度提示
TOTAL_ROWS=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  -N -B -e "SELECT COUNT(*) FROM \`$TABLE_NAME\`")
echo "   总行数: $TOTAL_ROWS"

# 流式导入：mysql 输出 -> psql COPY 输入（真正的流式，不存临时文件）
# --quick (-q): 强制流式查询，不缓冲整个结果集到内存（关键！）
# -N: 不显示列名
# -B: 批处理模式（tab 分隔）
# --default-character-set=utf8mb4: 强制 UTF-8 输出
# iconv -c: 过滤无效 UTF-8 字节（静默丢弃）
mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  --quick \
  --default-character-set=utf8mb4 \
  -N -B -e "SELECT ${COLUMNS} FROM \`$TABLE_NAME\`" | \
iconv -f UTF-8 -t UTF-8 -c | \
psql "$PG_CONN" -c "COPY ${MYSQL_DB}.${TABLE_NAME}(${COLUMNS}) FROM STDIN WITH (FORMAT text)"

echo "✅ 数据导入完成"

# ============================================================================
# 步骤 3：创建序列（如果有自增列）
# ============================================================================
echo "▶ 检查并创建序列..."

# 检查是否有自增列
AUTO_INCREMENT_COL=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  -N -B -e "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='$MYSQL_DB' AND TABLE_NAME='$TABLE_NAME' AND EXTRA LIKE '%auto_increment%'" || echo "")

if [[ -n "$AUTO_INCREMENT_COL" ]]; then
  echo "   自增列: $AUTO_INCREMENT_COL"
  psql "$PG_CONN" << EOF
-- 创建序列
CREATE SEQUENCE IF NOT EXISTS ${MYSQL_DB}.${TABLE_NAME}_${AUTO_INCREMENT_COL}_seq OWNED BY ${MYSQL_DB}.${TABLE_NAME}.${AUTO_INCREMENT_COL};
ALTER TABLE ${MYSQL_DB}.${TABLE_NAME} ALTER COLUMN ${AUTO_INCREMENT_COL} SET DEFAULT nextval('${MYSQL_DB}.${TABLE_NAME}_${AUTO_INCREMENT_COL}_seq');
-- 重置序列起始值
SELECT setval('${MYSQL_DB}.${TABLE_NAME}_${AUTO_INCREMENT_COL}_seq', (SELECT COALESCE(MAX(${AUTO_INCREMENT_COL}), 1) FROM ${MYSQL_DB}.${TABLE_NAME}));
EOF
  echo "✅ 序列创建完成"
else
  echo "   无自增列，跳过序列创建"
fi

# ============================================================================
# 验证
# ============================================================================
echo "▶ 验证迁移结果..."

PG_COUNT=$(psql "$PG_CONN" -t -c "SELECT COUNT(*) FROM ${MYSQL_DB}.${TABLE_NAME}" | tr -d ' ')
echo "   MySQL 行数: $TOTAL_ROWS"
echo "   PostgreSQL 行数: $PG_COUNT"

if [[ "$TOTAL_ROWS" == "$PG_COUNT" ]]; then
  echo "✅ 迁移成功！行数一致"
else
  echo "⚠️  警告：行数不一致，请检查"
fi

# 计算总耗时
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo "============================================"
echo "🎉 ${TABLE_NAME} 表迁移完成！"
echo "   总耗时: ${MINUTES}分${SECONDS}秒"
echo "   结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

