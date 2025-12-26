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
echo "   目标 schema: ${MYSQL_DB}"

psql "$PG_CONN" << EOF
-- 创建 schema（如果不存在）
CREATE SCHEMA IF NOT EXISTS ${MYSQL_DB};

-- 如果表已存在则跳过
CREATE TABLE IF NOT EXISTS ${MYSQL_DB}.text_content (
  id bigint PRIMARY KEY,
  content text,
  dbctime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  dbutime timestamp(3) DEFAULT CURRENT_TIMESTAMP
);

-- 创建序列（用于后续插入自增）
CREATE SEQUENCE IF NOT EXISTS ${MYSQL_DB}.text_content_id_seq OWNED BY ${MYSQL_DB}.text_content.id;
ALTER TABLE ${MYSQL_DB}.text_content ALTER COLUMN id SET DEFAULT nextval('${MYSQL_DB}.text_content_id_seq');

-- 清空表（如果重跑脚本）
TRUNCATE TABLE ${MYSQL_DB}.text_content;
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
# --default-character-set=utf8mb4: 强制 UTF-8 输出
# iconv: 过滤掉无效的 UTF-8 字节（-c 忽略无法转换的字符）

# 先导出数据到临时文件，方便检测
TMP_MYSQL_DATA=$(mktemp)
TMP_CLEAN_DATA=$(mktemp)

echo "   导出 MySQL 数据..."
mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
  --default-character-set=utf8mb4 \
  -N -B -e "SELECT id, content, dbctime, dbutime FROM text_content" > "$TMP_MYSQL_DATA"

MYSQL_SIZE=$(wc -c < "$TMP_MYSQL_DATA")
echo "   MySQL 导出大小: $(numfmt --to=iec $MYSQL_SIZE)"

echo "   清理无效 UTF-8 字符..."
iconv -f UTF-8 -t UTF-8 -c < "$TMP_MYSQL_DATA" > "$TMP_CLEAN_DATA"

CLEAN_SIZE=$(wc -c < "$TMP_CLEAN_DATA")
LOST_BYTES=$((MYSQL_SIZE - CLEAN_SIZE))

if [[ $LOST_BYTES -gt 0 ]]; then
  echo "   ⚠️  丢弃了 $LOST_BYTES 字节无效数据 ($(echo "scale=4; $LOST_BYTES * 100 / $MYSQL_SIZE" | bc)%)"
  
  # 找出被丢弃的字符（最多显示前 20 个）
  echo ""
  echo "   被丢弃的字符（十六进制）:"
  # 使用 cmp 找出差异位置，然后用 xxd 显示
  diff <(xxd "$TMP_MYSQL_DATA") <(xxd "$TMP_CLEAN_DATA") | grep "^<" | head -20 | while read line; do
    echo "   $line"
  done
  echo ""
  
  # 统计无效字节分布
  echo "   无效字节统计:"
  # 找出所有非 UTF-8 字节
  cat "$TMP_MYSQL_DATA" | LC_ALL=C grep -oP '[\x80-\xff]' | sort | uniq -c | sort -rn | head -10
  echo ""
else
  echo "   ✅ 没有丢弃任何数据"
fi

echo "   导入到 PostgreSQL..."
psql "$PG_CONN" -c "COPY ${MYSQL_DB}.text_content(id, content, dbctime, dbutime) FROM STDIN WITH (FORMAT text)" < "$TMP_CLEAN_DATA"

# 清理临时文件
rm -f "$TMP_MYSQL_DATA" "$TMP_CLEAN_DATA"

echo "✅ 数据导入完成"

# ============================================================================
# 步骤 3：重置序列起始值
# ============================================================================
echo "▶ 重置序列起始值..."

psql "$PG_CONN" -c "SELECT setval('${MYSQL_DB}.text_content_id_seq', (SELECT COALESCE(MAX(id), 1) FROM ${MYSQL_DB}.text_content));"

echo "✅ 序列重置完成"

# ============================================================================
# 验证
# ============================================================================
echo "▶ 验证迁移结果..."

PG_COUNT=$(psql "$PG_CONN" -t -c "SELECT COUNT(*) FROM ${MYSQL_DB}.text_content" | tr -d ' ')
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

