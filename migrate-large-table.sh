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
# 迁移 text_content 表
# ============================================================================
migrate_text_content() {
  local SCHEMA="$1"
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "▶ 开始迁移 text_content 表 ($(date '+%Y-%m-%d %H:%M:%S'))"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  local TABLE_START_TIME=$(date +%s)
  
  # 创建表结构
  echo "▶ 创建表结构..."
  psql "$PG_CONN" << EOF
CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
DROP TABLE IF EXISTS ${SCHEMA}.text_content CASCADE;
CREATE TABLE ${SCHEMA}.text_content (
  id bigint PRIMARY KEY,
  content text,
  dbctime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  dbutime timestamp(3) DEFAULT CURRENT_TIMESTAMP
);
CREATE SEQUENCE IF NOT EXISTS ${SCHEMA}.text_content_id_seq OWNED BY ${SCHEMA}.text_content.id;
ALTER TABLE ${SCHEMA}.text_content ALTER COLUMN id SET DEFAULT nextval('${SCHEMA}.text_content_id_seq');
EOF
  echo "✅ 表结构创建完成"
  
  # 流式导入数据
  echo "▶ 开始流式导入数据..."
  local TOTAL_ROWS=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    -N -B -e "SELECT COUNT(*) FROM text_content")
  echo "   总行数: $TOTAL_ROWS"
  
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    --quick --default-character-set=utf8mb4 -N -B \
    -e "SELECT id,content,dbctime,dbutime FROM text_content" | \
  iconv -f UTF-8 -t UTF-8 -c | \
  psql "$PG_CONN" -c "COPY ${SCHEMA}.text_content(id,content,dbctime,dbutime) FROM STDIN WITH (FORMAT text)"
  echo "✅ 数据导入完成"
  
  # 重置序列
  echo "▶ 重置序列..."
  psql "$PG_CONN" -c "SELECT setval('${SCHEMA}.text_content_id_seq', (SELECT COALESCE(MAX(id), 1) FROM ${SCHEMA}.text_content));"
  
  # 验证
  echo "▶ 验证..."
  local PG_COUNT=$(psql "$PG_CONN" -t -c "SELECT COUNT(*) FROM ${SCHEMA}.text_content" | tr -d ' ')
  echo "   MySQL: $TOTAL_ROWS, PostgreSQL: $PG_COUNT"
  [[ "$TOTAL_ROWS" == "$PG_COUNT" ]] && echo "✅ 成功" || echo "⚠️ 行数不一致"
  
  local TABLE_END_TIME=$(date +%s)
  echo "   耗时: $((TABLE_END_TIME - TABLE_START_TIME))秒"
}

# ============================================================================
# 迁移 pipeline_snapshot 表
# ============================================================================
migrate_pipeline_snapshot() {
  local SCHEMA="$1"
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "▶ 开始迁移 pipeline_snapshot 表 ($(date '+%Y-%m-%d %H:%M:%S'))"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  local TABLE_START_TIME=$(date +%s)
  
  # 创建表结构
  echo "▶ 创建表结构..."
  psql "$PG_CONN" << EOF
CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
DROP TABLE IF EXISTS ${SCHEMA}.pipeline_snapshot CASCADE;
CREATE TABLE ${SCHEMA}.pipeline_snapshot (
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
CREATE SEQUENCE IF NOT EXISTS ${SCHEMA}.pipeline_snapshot_id_seq OWNED BY ${SCHEMA}.pipeline_snapshot.id;
ALTER TABLE ${SCHEMA}.pipeline_snapshot ALTER COLUMN id SET DEFAULT nextval('${SCHEMA}.pipeline_snapshot_id_seq');
EOF
  echo "✅ 表结构创建完成"
  
  # 流式导入数据
  echo "▶ 开始流式导入数据..."
  local TOTAL_ROWS=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    -N -B -e "SELECT COUNT(*) FROM pipeline_snapshot")
  echo "   总行数: $TOTAL_ROWS"
  
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    --quick --default-character-set=utf8mb4 -N -B \
    -e "SELECT id,pipeline_id,status,visible,last_event_id,llm_virtual_key,tool_token,sandbox_id,agent_pid,result,error_message,dbctime,dbutime FROM pipeline_snapshot" | \
  iconv -f UTF-8 -t UTF-8 -c | \
  psql "$PG_CONN" -c "COPY ${SCHEMA}.pipeline_snapshot(id,pipeline_id,status,visible,last_event_id,llm_virtual_key,tool_token,sandbox_id,agent_pid,result,error_message,dbctime,dbutime) FROM STDIN WITH (FORMAT text)"
  echo "✅ 数据导入完成"
  
  # 重置序列
  echo "▶ 重置序列..."
  psql "$PG_CONN" -c "SELECT setval('${SCHEMA}.pipeline_snapshot_id_seq', (SELECT COALESCE(MAX(id), 1) FROM ${SCHEMA}.pipeline_snapshot));"
  
  # 验证
  echo "▶ 验证..."
  local PG_COUNT=$(psql "$PG_CONN" -t -c "SELECT COUNT(*) FROM ${SCHEMA}.pipeline_snapshot" | tr -d ' ')
  echo "   MySQL: $TOTAL_ROWS, PostgreSQL: $PG_COUNT"
  [[ "$TOTAL_ROWS" == "$PG_COUNT" ]] && echo "✅ 成功" || echo "⚠️ 行数不一致"
  
  local TABLE_END_TIME=$(date +%s)
  echo "   耗时: $((TABLE_END_TIME - TABLE_START_TIME))秒"
}

# ============================================================================
# 迁移 pipeline_result_event 表
# ============================================================================
migrate_pipeline_result_event() {
  local SCHEMA="$1"
  
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "▶ 开始迁移 pipeline_result_event 表 ($(date '+%Y-%m-%d %H:%M:%S'))"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  local TABLE_START_TIME=$(date +%s)
  
  # 创建表结构
  echo "▶ 创建表结构..."
  psql "$PG_CONN" << EOF
CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
DROP TABLE IF EXISTS ${SCHEMA}.pipeline_result_event CASCADE;
CREATE TABLE ${SCHEMA}.pipeline_result_event (
  id bigint PRIMARY KEY,
  pipeline_id varchar(64) NOT NULL,
  seq bigint NOT NULL,
  content text,
  dbctime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  dbutime timestamp(3) DEFAULT CURRENT_TIMESTAMP,
  created_ts bigint NOT NULL DEFAULT -1
);
CREATE SEQUENCE IF NOT EXISTS ${SCHEMA}.pipeline_result_event_id_seq OWNED BY ${SCHEMA}.pipeline_result_event.id;
ALTER TABLE ${SCHEMA}.pipeline_result_event ALTER COLUMN id SET DEFAULT nextval('${SCHEMA}.pipeline_result_event_id_seq');
EOF
  echo "✅ 表结构创建完成"
  
  # 流式导入数据
  echo "▶ 开始流式导入数据..."
  local TOTAL_ROWS=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    -N -B -e "SELECT COUNT(*) FROM pipeline_result_event")
  echo "   总行数: $TOTAL_ROWS"
  
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    --quick --default-character-set=utf8mb4 -N -B \
    -e "SELECT id,pipeline_id,seq,content,dbctime,dbutime,created_ts FROM pipeline_result_event" | \
  iconv -f UTF-8 -t UTF-8 -c | \
  psql "$PG_CONN" -c "COPY ${SCHEMA}.pipeline_result_event(id,pipeline_id,seq,content,dbctime,dbutime,created_ts) FROM STDIN WITH (FORMAT text)"
  echo "✅ 数据导入完成"
  
  # 重置序列
  echo "▶ 重置序列..."
  psql "$PG_CONN" -c "SELECT setval('${SCHEMA}.pipeline_result_event_id_seq', (SELECT COALESCE(MAX(id), 1) FROM ${SCHEMA}.pipeline_result_event));"
  
  # 验证
  echo "▶ 验证..."
  local PG_COUNT=$(psql "$PG_CONN" -t -c "SELECT COUNT(*) FROM ${SCHEMA}.pipeline_result_event" | tr -d ' ')
  echo "   MySQL: $TOTAL_ROWS, PostgreSQL: $PG_COUNT"
  [[ "$TOTAL_ROWS" == "$PG_COUNT" ]] && echo "✅ 成功" || echo "⚠️ 行数不一致"
  
  local TABLE_END_TIME=$(date +%s)
  echo "   耗时: $((TABLE_END_TIME - TABLE_START_TIME))秒"
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
  echo "❌ PostgreSQL 连接失败" >&2
  exit 1
fi
echo "✅ PostgreSQL 连接成功"

echo "▶ 测试 MySQL 连接..."
if ! mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -e "SELECT 1" > /dev/null 2>&1; then
  echo "❌ MySQL 连接失败" >&2
  exit 1
fi
echo "✅ MySQL 连接成功"

# 记录开始时间
START_TIME=$(date +%s)

# 确定要迁移的表
TARGET="$1"

migrate_single_table() {
  local table="$1"
  case "$table" in
    text_content)
      migrate_text_content "$MYSQL_DB"
      ;;
    pipeline_snapshot)
      migrate_pipeline_snapshot "$MYSQL_DB"
      ;;
    pipeline_result_event)
      migrate_pipeline_result_event "$MYSQL_DB"
      ;;
    *)
      echo "❌ 不支持的表: $table" >&2
      show_usage
      exit 1
      ;;
  esac
}

if [[ "$TARGET" == "all" ]]; then
  echo ""
  echo "▶ 将迁移所有大表: ${SUPPORTED_TABLES[*]}"
  for table in "${SUPPORTED_TABLES[@]}"; do
    migrate_single_table "$table"
  done
else
  migrate_single_table "$TARGET"
fi

# 计算总耗时
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo "============================================"
echo "🎉 大表迁移完成！"
echo "   总耗时: ${MINUTES}分${SECONDS}秒"
echo "   结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
