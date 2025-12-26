#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# text_content 表迁移脚本
# 用于迁移超大表，pgloader 会内存溢出，改用 mysql + psql 流式迁移
# ============================================================================

# 记录开始时间
START_TIME=$(date +%s)
echo "▶ 开始迁移 text_content 表 ($(date '+%Y-%m-%d %H:%M:%S'))"
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
# 步骤 1：在 PostgreSQL 创建表结构
# ============================================================================
echo "▶ 创建 text_content 表结构..."

psql "$PG_CONN" << 'EOF'
-- 如果表已存在则跳过
CREATE TABLE IF NOT EXISTS text_content (
  id bigint PRIMARY KEY,
  content text,
  dbctime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  dbutime timestamp(3) DEFAULT CURRENT_TIMESTAMP
);

-- 创建序列（用于后续插入自增）
CREATE SEQUENCE IF NOT EXISTS text_content_id_seq OWNED BY text_content.id;
ALTER TABLE text_content ALTER COLUMN id SET DEFAULT nextval('text_content_id_seq');

-- 清空表（如果重跑脚本）
TRUNCATE TABLE text_content;
EOF

echo "✅ 表结构创建完成"

# ============================================================================
# 步骤 2：流式导入数据
# ============================================================================
echo "▶ 开始流式导入数据（这可能需要几分钟）..."

# 获取总行数用于进度提示
TOTAL_ROWS=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  -N -B -e "SELECT COUNT(*) FROM text_content")
echo "   总行数: $TOTAL_ROWS"

# 流式导入：mysql 输出 -> psql COPY 输入
# -N: 不显示列名
# -B: 批处理模式（tab 分隔）
mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  -N -B -e "SELECT id, content, dbctime, dbutime FROM text_content" | \
psql "$PG_CONN" -c "COPY text_content(id, content, dbctime, dbutime) FROM STDIN WITH (FORMAT text)"

echo "✅ 数据导入完成"

# ============================================================================
# 步骤 3：重置序列起始值
# ============================================================================
echo "▶ 重置序列起始值..."

psql "$PG_CONN" -c "SELECT setval('text_content_id_seq', (SELECT COALESCE(MAX(id), 1) FROM text_content));"

echo "✅ 序列重置完成"

# ============================================================================
# 验证
# ============================================================================
echo "▶ 验证迁移结果..."

PG_COUNT=$(psql "$PG_CONN" -t -c "SELECT COUNT(*) FROM text_content" | tr -d ' ')
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
echo "🎉 text_content 表迁移完成！"
echo "   总耗时: ${MINUTES}分${SECONDS}秒"
echo "   结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

