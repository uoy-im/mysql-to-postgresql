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

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

required_vars=(MYSQL_DB MYSQL_HOST MYSQL_PASSWORD MYSQL_PORT MYSQL_USER PG_DB PG_ENDPOINT_ID PG_PASSWORD PG_REGION PG_USER)

CONFIG_DIR="/app"
BASE_CONFIG="${CONFIG_DIR}/pgloader-config-base.load"

# ============================================================================
# 分批配置
# ============================================================================
# 始终排除的表（手动迁移或不需要）
ALWAYS_EXCLUDE=("text_content" "project_data" "token_usage" "pipeline_step" "project_index_v2" "project_deploy_key" "benchmark_case_run" "style_config_learn_task" "predefined_style" "user_style_configs" "gaia_benchmark_case_result" "benchmark_run" "ask_user_response" "gaia_benchmark_round_result" "dbpaas_upsert_record" "env_config" "project_notion_auth" "signup_invite_code")
# 始终排除的正则模式
ALWAYS_EXCLUDE_PATTERNS=("_peerdb_.*")

BATCH_2_TABLES=("pipeline_snapshot")
BATCH_3_TABLES=("pipeline_result_snippet")
BATCH_4_TABLES=("pipeline_result_event" "project_index" "pipeline")

# ============================================================================
# 工具函数
# ============================================================================

# 将数组转换为 pgloader 正则表达式格式
to_regex() {
    local result=""
    for item in "$@"; do
        [[ -n "$result" ]] && result+=", "
        result+="~/^${item}\$/"
    done
    echo "$result"
}

check_env_vars() {
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            echo -e "${RED}❌ Missing required env var: $var${NC}" >&2
            exit 1
        fi
    done
}

substitute_env_vars() {
    local template="$1"
    for var in "${required_vars[@]}"; do
        template="${template//\$\{$var\}/${!var:-}}"
    done
    echo "$template"
}

generate_config() {
    local batch=$1
    local table_filter=""
    
    case $batch in
        2) table_filter="INCLUDING ONLY TABLE NAMES MATCHING $(to_regex "${BATCH_2_TABLES[@]}")" ;;
        3) table_filter="INCLUDING ONLY TABLE NAMES MATCHING $(to_regex "${BATCH_3_TABLES[@]}")" ;;
        4) table_filter="INCLUDING ONLY TABLE NAMES MATCHING $(to_regex "${BATCH_4_TABLES[@]}")" ;;
        5)
            local exclude_all=("${ALWAYS_EXCLUDE[@]}" "${BATCH_2_TABLES[@]}" "${BATCH_3_TABLES[@]}" "${BATCH_4_TABLES[@]}")
            local tables_regex=$(to_regex "${exclude_all[@]}")
            local patterns_regex=$(to_regex "${ALWAYS_EXCLUDE_PATTERNS[@]}")
            if [[ -n "$patterns_regex" ]]; then
                table_filter="EXCLUDING TABLE NAMES MATCHING ${tables_regex}, ${patterns_regex}"
            else
                table_filter="EXCLUDING TABLE NAMES MATCHING ${tables_regex}"
            fi
            ;;
    esac
    
    local template
    template="$(cat "${BASE_CONFIG}")"
    template="${template//-- 表过滤条件（由脚本动态添加）/-- 第${batch}批
${table_filter}}"
    
    local after_load_sql
    after_load_sql=$(cat <<'EOF'

AFTER LOAD DO
    $$ SELECT 'Migration completed. Tables in public: ' || COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'; $$;
EOF
)
    template="${template}${after_load_sql}"
    template=$(substitute_env_vars "$template")
    
    local config_file
    config_file="$(mktemp)"
    printf '%s\n' "$template" > "$config_file"
    echo "$config_file"
}

run_batch() {
    local batch=$1
    
    echo -e "${GREEN}▶ 开始执行第 ${batch} 批迁移${NC}"
    
    local config_file
    config_file=$(generate_config $batch)
    echo -e "${YELLOW}▶ 配置文件: ${config_file}${NC}"
    
    if pgloader --load-lisp-file ms-transforms.lisp --no-ssl-cert-verification --dynamic-space-size 16384 "${config_file}"; then
        echo -e "${GREEN}✓ 第 ${batch} 批迁移成功！${NC}"
        rm -f "${config_file}"
        return 0
    else
        echo -e "${RED}✗ 第 ${batch} 批迁移失败！配置文件: ${config_file}${NC}"
        return 1
    fi
}

main() {
    local target=${1:-"all"}
    
    echo -e "${GREEN}MySQL -> PostgreSQL 分批迁移脚本${NC}"
    
    check_env_vars
    
    if [[ ! -f "${BASE_CONFIG}" ]]; then
        echo -e "${RED}错误: ${BASE_CONFIG} 不存在${NC}"
        exit 1
    fi
    
    case $target in
        all)
            for batch in 2 3 4 5; do
                run_batch $batch
                [[ $batch -lt 5 ]] && sleep 2
            done
            echo -e "${GREEN}✓ 所有批次迁移完成！${NC}"
            ;;
        2|3|4|5)
            run_batch $target
            ;;
        *)
            echo "用法: $0 {all|2|3|4|5}"
            exit 1
            ;;
    esac
}

main "$@"
