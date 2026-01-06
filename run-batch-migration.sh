#!/usr/bin/env bash
# ============================================================================
# 分批迁移脚本
# 用法:
#   ./run-batch-migration.sh all     # 执行所有批次 (2-5)
#   ./run-batch-migration.sh 2       # 只执行第2批
#   ./run-batch-migration.sh 3       # 只执行第3批
#   ./run-batch-migration.sh 4       # 只执行第4批
#   ./run-batch-migration.sh 5       # 只执行第5批
# ============================================================================

set -euo pipefail

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 必需的环境变量
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

# 配置文件目录
CONFIG_DIR="/app"
BASE_CONFIG="${CONFIG_DIR}/pgloader-config-base.load"

# ============================================================================
# 分批配置（每个表名只定义一次）
# ============================================================================
# 第1批：text_content 太大，手动迁移，这里不处理
# 始终排除的表
ALWAYS_EXCLUDE=("text_content" "project_data" "token_usage" "pipeline_step" "project_index_v2" "project_deploy_key" "benchmark_case_run" "style_config_learn_task" "predefined_style" "user_style_configs" "gaia_benchmark_case_result" "benchmark_run" "ask_user_response" "gaia_benchmark_round_result" "dbpaas_upsert_record" "env_config" "project_notion_auth" "signup_invite_code")

# 第2批：大表 (455MB)
BATCH_2_TABLES=("pipeline_snapshot")
BATCH_2_DESC="大表 (455MB)"

# 第3批：大表 (397MB)
BATCH_3_TABLES=("pipeline_result_snippet")
BATCH_3_DESC="大表 (397MB)"

# 第4批：中大表 (~180MB)
BATCH_4_TABLES=("pipeline_result_event" "project_index" "pipeline")
BATCH_4_DESC="中大表 (~180MB)"

# 第5批：剩余所有小表（自动计算需要排除的表）
BATCH_5_DESC="剩余所有小表 (~170MB)"

# ============================================================================
# 工具函数
# ============================================================================

# 将表名数组转换为 pgloader 正则表达式格式
# 例如: ("a" "b") -> "~/^a$/, ~/^b$/"
tables_to_regex() {
    local tables=("$@")
    local result=""
    for table in "${tables[@]}"; do
        if [[ -n "$result" ]]; then
            result+=", "
        fi
        result+="~/^${table}\$/"
    done
    echo "$result"
}

# 获取批次的表名（用于显示）
get_batch_tables_display() {
    local batch=$1
    case $batch in
        2) echo "${BATCH_2_TABLES[*]}" ;;
        3) echo "${BATCH_3_TABLES[*]}" ;;
        4) echo "${BATCH_4_TABLES[*]}" ;;
        5) echo "剩余所有小表" ;;
    esac
}

# 校验环境变量
check_env_vars() {
    for var in "${required_vars[@]}"; do
        value="${!var:-}"
        if [[ -z "$value" ]]; then
            echo -e "${RED}❌ Missing required env var: $var${NC}" >&2
            exit 1
        fi
    done
}

# 替换模板中的环境变量（参考 run-pgloader.sh）
substitute_env_vars() {
    local template="$1"
    for var in "${required_vars[@]}"; do
        value="${!var:-}"
        template="${template//\$\{$var\}/$value}"
    done
    echo "$template"
}

# PostgreSQL 连接字符串（用于清理连接池）
get_pg_conn() {
    echo "postgresql://${PG_USER}:endpoint=${PG_ENDPOINT_ID};${PG_PASSWORD}@${PG_ENDPOINT_ID}.${PG_REGION}.aws.neon.tech/${PG_DB}?sslmode=require"
}

# 清理连接池
cleanup_connections() {
    echo -e "${YELLOW}▶ 清理连接池中的残留连接...${NC}"
    local pg_conn=$(get_pg_conn)
    psql "${pg_conn}" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid != pg_backend_pid();" 2>/dev/null || true
    sleep 2
}

# 生成配置文件
generate_config() {
    local batch=$1
    
    # 根据批次生成表过滤条件
    local table_filter=""
    local batch_comment=""
    case $batch in
        2)
            batch_comment="-- 第2批：${BATCH_2_DESC}"
            table_filter="INCLUDING ONLY TABLE NAMES MATCHING $(tables_to_regex "${BATCH_2_TABLES[@]}")"
            ;;
        3)
            batch_comment="-- 第3批：${BATCH_3_DESC}"
            table_filter="INCLUDING ONLY TABLE NAMES MATCHING $(tables_to_regex "${BATCH_3_TABLES[@]}")"
            ;;
        4)
            batch_comment="-- 第4批：${BATCH_4_DESC}"
            table_filter="INCLUDING ONLY TABLE NAMES MATCHING $(tables_to_regex "${BATCH_4_TABLES[@]}")"
            ;;
        5)
            batch_comment="-- 第5批：${BATCH_5_DESC}"
            # 第5批排除：始终排除的表 + 第2-4批的表
            local exclude_tables=("${ALWAYS_EXCLUDE[@]}" "${BATCH_2_TABLES[@]}" "${BATCH_3_TABLES[@]}" "${BATCH_4_TABLES[@]}")
            table_filter="EXCLUDING TABLE NAMES MATCHING $(tables_to_regex "${exclude_tables[@]}")"
            ;;
    esac
    
    # 读取基础配置模板
    local template
    template="$(cat "${BASE_CONFIG}")"
    
    # 替换占位符，插入表过滤条件
    template="${template//-- 表过滤条件（由脚本动态添加）/${batch_comment}
${table_filter}}"
    
    # 添加验证 SQL
    template="${template}

AFTER LOAD DO
    \$\$ SELECT 'Batch ${batch} completed. Tables in public: ' || COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'; \$\$;"
    
    # 替换环境变量
    template=$(substitute_env_vars "$template")
    
    # 写入临时文件
    local config_file
    config_file="$(mktemp)"
    printf '%s\n' "$template" > "$config_file"
    
    echo "$config_file"
}

# 执行单批迁移
run_batch() {
    local batch=$1
    
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}▶ 开始执行第 ${batch} 批迁移${NC}"
    echo -e "${GREEN}============================================${NC}"
    
    # 清理连接池(链接参数里不带pooler, 不需要再清楚链接)
    # cleanup_connections
    
    # 生成配置文件
    local config_file
    config_file=$(generate_config $batch)
    echo -e "${YELLOW}▶ 配置文件: ${config_file}${NC}"
    
    # 执行迁移
    echo -e "${YELLOW}▶ 开始 pgloader 迁移...${NC}"
    if pgloader --load-lisp-file ms-transforms.lisp --no-ssl-cert-verification --dynamic-space-size 16384 "${config_file}"; then
        echo -e "${GREEN}✓ 第 ${batch} 批迁移成功！${NC}"
        rm -f "${config_file}"
        return 0
    else
        echo -e "${RED}✗ 第 ${batch} 批迁移失败！${NC}"
        echo -e "${YELLOW}配置文件保留在: ${config_file}${NC}"
        return 1
    fi
}

# 显示帮助信息
show_help() {
    echo -e "${RED}用法: $0 {all|2|3|4|5}${NC}"
    echo "  all - 执行所有批次"
    echo "  2   - 第2批: ${BATCH_2_TABLES[*]} (${BATCH_2_DESC})"
    echo "  3   - 第3批: ${BATCH_3_TABLES[*]} (${BATCH_3_DESC})"
    echo "  4   - 第4批: ${BATCH_4_TABLES[*]} (${BATCH_4_DESC})"
    echo "  5   - 第5批: ${BATCH_5_DESC}"
}

# 主函数
main() {
    local target=${1:-"all"}
    
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}   MySQL -> PostgreSQL 分批迁移脚本${NC}"
    echo -e "${GREEN}============================================${NC}"
    
    # 校验环境变量
    check_env_vars
    
    # 检查基础配置文件是否存在
    if [[ ! -f "${BASE_CONFIG}" ]]; then
        echo -e "${RED}错误: 基础配置文件 ${BASE_CONFIG} 不存在${NC}"
        exit 1
    fi
    
    case $target in
        all)
            echo -e "${YELLOW}▶ 将执行所有批次 (2, 3, 4, 5)${NC}"
            for batch in 2 3 4 5; do
                run_batch $batch
                if [[ $batch -lt 5 ]]; then
                    echo -e "${YELLOW}▶ 等待 2 秒后执行下一批...${NC}"
                    sleep 2
                fi
            done
            echo -e "${GREEN}============================================${NC}"
            echo -e "${GREEN}✓ 所有批次迁移完成！${NC}"
            echo -e "${GREEN}============================================${NC}"
            ;;
        2|3|4|5)
            run_batch $target
            ;;
        *)
            show_help
            exit 1
            ;;
    esac
}

main "$@"
