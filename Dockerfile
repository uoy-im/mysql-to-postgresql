# 数据库迁移: MySQL -> PostgreSQL (Neon)
# 
# 使用方式:
#   1. 在 Render 创建 Private Service（Root Directory: scripts/db-migration）
#   2. 部署完成后打开 Shell
#   3. 执行: pgloader --no-ssl-cert-verification pgloader-config.load
#   4. 迁移完成后删除服务

FROM debian:bookworm-slim

# 安装 pgloader 和调试工具
# - default-mysql-client: 用于测试 MySQL 连接 (mysql -h xxx -u xxx -p)
# - postgresql-client: 用于测试 PostgreSQL 连接 (psql postgres://xxx)
RUN apt-get update && \
    apt-get install -y \
      pgloader \
      ca-certificates \
      default-mysql-client \
      postgresql-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 复制迁移配置文件
COPY . .

# 不自动执行迁移！保持容器运行，等待手动执行
# 优点：可以随时重跑、方便调试、查看日志
CMD ["tail", "-f", "/dev/null"]
