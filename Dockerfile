# 数据库迁移: MySQL -> PostgreSQL (Neon)
# 
# 使用方式:
#   1. 在 Render 创建 Private Service（Root Directory: scripts/db-migration）
#   2. 部署完成后打开 Shell
#   3. 执行: bash run-pgloader.sh
#   4. 迁移完成后删除服务

# ============================================================================
# 阶段 1: 从 GitHub master 编译 pgloader（包含 MySQL 8.0 排序规则修复）
# ============================================================================
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y \
    sbcl \
    git \
    curl \
    ca-certificates \
    libssl-dev \
    freetds-dev \
    libsqlite3-dev \
    libzip-dev \
    make \
    && rm -rf /var/lib/apt/lists/*

# 克隆最新的 master 分支（包含所有修复）
RUN git clone --depth 1 https://github.com/dimitri/pgloader.git /pgloader

WORKDIR /pgloader
RUN make pgloader

# ============================================================================
# 阶段 2: 运行时镜像
# ============================================================================
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    freetds-dev \
    libsqlite3-0 \
    libzip4 \
    default-mysql-client \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 从构建阶段复制编译好的 pgloader
COPY --from=builder /pgloader/build/bin/pgloader /usr/local/bin/pgloader

WORKDIR /app
COPY . .

CMD ["tail", "-f", "/dev/null"]
