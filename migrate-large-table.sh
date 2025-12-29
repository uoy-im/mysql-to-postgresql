#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# 大表流式迁移脚本
# 用于迁移超大表，pgloader 会内存溢出，改用 mysql + psql 流式迁移
#
# 用法: 
#   bash migrate-large-table.sh <表名>     # 迁移单个表
#   bash migrate-large-table.sh all        # 迁移所有大表
#
# 支持的表:
#   - text_content
#   - pipeline_snapshot
#   - pipeline_result_event
# ============================================================================

# 支持的大表列表
SUPPORTED_TABLES=("text_content" "pipeline_snapshot" "pipeline_result_event")

show_usage() {
  echo "用法: $0 <表名|all>"
  echo ""
  echo "支持的表:"
  for t in "${SUPPORTED_TABLES[@]}"; do
    echo "  - $t"
  done
  echo ""
  echo "示例:"
  echo "  $0 text_content        # 迁移单个表"
  echo "  $0 all                 # 迁移所有大表"
}

if [[ $# -lt 1 ]]; then
  show_usage
  exit 1
fi

# ============================================================================
# 内置的 PostgreSQL 建表语句
# ============================================================================
get_create_table_sql() {
  local table_name="$1"
  local schema="$2"
  
  case "$table_name" in
    text_content)
      cat << EOF
CREATE TABLE IF NOT EXISTS ${schema}.text_content (
  id bigint PRIMARY KEY,
  content text,
  dbctime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  dbutime timestamp(3) DEFAULT CURRENT_TIMESTAMP
);
EOF
      ;;
    pipeline_snapshot)
      cat << EOF
CREATE TABLE IF NOT EXISTS ${schema}.pipeline_snapshot (
  id bigint PRIMARY KEY,
  pipeline_id varchar(20) NOT NULL,
  status varchar(20) NOT NULL DEFAULT 'created',
  visible boolean NOT NULL DEFAULT true,
  last_event_id bigint NOT NULL,
  llm_virtual_key varchar(32) NOT NULL DEFAULT '',
  tool_token varchar(64) NOT NULL DEFAULT '',
  sandbox_id varchar(32) NOT NULL DEFAULT '',
  agent_pid integer NOT NULL DEFAULT 0,
  result text,
  error_message text,
  dbctime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  dbutime timestamp(3) DEFAULT CURRENT_TIMESTAMP
);
EOF
      ;;
    pipeline_result_event)
      cat << EOF
CREATE TABLE IF NOT EXISTS ${schema}.pipeline_result_event (
  id bigint PRIMARY KEY,
  pipeline_id varchar(64) NOT NULL,
  seq bigint NOT NULL,
  content text,
  dbctime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  dbutime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  created_ts bigint NOT NULL DEFAULT -1
);
EOF
      ;;
    *)
      echo "❌ 未知表: $table_name" >&2
      return 1
      ;;
  esac
}

# 获取表的列名列表
get_columns() {
  local table_name="$1"
  
  case "$table_name" in
    text_content)
      echo "id,content,dbctime,dbutime"
      ;;
    pipeline_snapshot)
      echo "id,pipeline_id,status,visible,last_event_id,llm_virtual_key,tool_token,sandbox_id,agent_pid,result,error_message,dbctime,dbutime"
      ;;
    pipeline_result_event)
      echo "id,pipeline_id,seq,content,dbctime,dbutime,created_ts"
      ;;
  esac
}

# ============================================================================
# 迁移单个表
# ============================================================================
migrate_table() {
  local TABLE_NAME="$1"
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "▶ 开始迁移 ${TABLE_NAME} 表 ($(date '+%Y-%m-%d %H:%M:%S'))"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  local TABLE_START_TIME=$(date +%s)
  local COLUMNS=$(get_columns "$TABLE_NAME")
  
  echo "   列: $COLUMNS"
  
  # 步骤 1：创建表结构
  echo "▶ 创建表结构..."
  local CREATE_SQL=$(get_create_table_sql "$TABLE_NAME" "$MYSQL_DB")
  
  psql "$PG_CONN" << EOF
-- 创建 schema（如果不存在）
CREATE SCHEMA IF NOT EXISTS ${MYSQL_DB};

-- 删除旧表（如果存在）
DROP TABLE IF EXISTS ${MYSQL_DB}.${TABLE_NAME} CASCADE;

-- 创建新表
${CREATE_SQL}

-- 创建序列
CREATE SEQUENCE IF NOT EXISTS ${MYSQL_DB}.${TABLE_NAME}_id_seq OWNED BY ${MYSQL_DB}.${TABLE_NAME}.id;
ALTER TABLE ${MYSQL_DB}.${TABLE_NAME} ALTER COLUMN id SET DEFAULT nextval('${MYSQL_DB}.${TABLE_NAME}_id_seq');
EOF
  
  echo "✅ 表结构创建完成"
  
  # 步骤 2：流式导入数据
  echo "▶ 开始流式导入数据..."
  
  local TOTAL_ROWS=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    -N -B -e "SELECT COUNT(*) FROM \`$TABLE_NAME\`")
  echo "   总行数: $TOTAL_ROWS"
  
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    --quick \
    --default-character-set=utf8mb4 \
    -N -B -e "SELECT ${COLUMNS} FROM \`$TABLE_NAME\`" | \
  iconv -f UTF-8 -t UTF-8 -c | \
  psql "$PG_CONN" -c "COPY ${MYSQL_DB}.${TABLE_NAME}(${COLUMNS}) FROM STDIN WITH (FORMAT text)"
  
  echo "✅ 数据导入完成"
  
  # 步骤 3：重置序列
  echo "▶ 重置序列起始值..."
  psql "$PG_CONN" -c "SELECT setval('${MYSQL_DB}.${TABLE_NAME}_id_seq', (SELECT COALESCE(MAX(id), 1) FROM ${MYSQL_DB}.${TABLE_NAME}));"
  
  # 验证
  echo "▶ 验证迁移结果..."
  local PG_COUNT=$(psql "$PG_CONN" -t -c "SELECT COUNT(*) FROM ${MYSQL_DB}.${TABLE_NAME}" | tr -d ' ')
  echo "   MySQL 行数: $TOTAL_ROWS"
  echo "   PostgreSQL 行数: $PG_COUNT"
  
  if [[ "$TOTAL_ROWS" == "$PG_COUNT" ]]; then
    echo "✅ 迁移成功！行数一致"
  else
    echo "⚠️  警告：行数不一致，请检查"
  fi
  
  local TABLE_END_TIME=$(date +%s)
  local TABLE_ELAPSED=$((TABLE_END_TIME - TABLE_START_TIME))
  echo "   耗时: ${TABLE_ELAPSED}秒"
}

# ============================================================================
# 主流程
# ============================================================================

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

# 记录开始时间
START_TIME=$(date +%s)

# 确定要迁移的表
TARGET="$1"

if [[ "$TARGET" == "all" ]]; then
  TABLES_TO_MIGRATE=("${SUPPORTED_TABLES[@]}")
  echo ""
  echo "▶ 将迁移所有大表: ${TABLES_TO_MIGRATE[*]}"
else
  # 验证表名
  VALID=false
  for t in "${SUPPORTED_TABLES[@]}"; do
    if [[ "$t" == "$TARGET" ]]; then
      VALID=true
      break
    fi
  done
  
  if [[ "$VALID" != "true" ]]; then
    echo "❌ 不支持的表: $TARGET" >&2
    echo ""
    show_usage
    exit 1
  fi
  
  TABLES_TO_MIGRATE=("$TARGET")
fi

# 执行迁移
for table in "${TABLES_TO_MIGRATE[@]}"; do
  migrate_table "$table"
done

# 计算总耗时
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo "============================================"
echo "🎉 大表迁移完成！"
echo "   迁移表: ${TABLES_TO_MIGRATE[*]}"
echo "   总耗时: ${MINUTES}分${SECONDS}秒"
echo "   结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
