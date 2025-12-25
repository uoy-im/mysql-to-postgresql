# 数据库迁移: MySQL -> PostgreSQL (Neon)
# 
# 使用方式:
#   1. 在 Render 创建 Private Service（Root Directory: scripts/db-migration）
#   2. 部署完成后打开 Shell
#   3. 执行: bash run-pgloader.sh
#   4. 迁移完成后删除服务

# 使用官方 pgloader 镜像 (latest = v3.6.9)
FROM dimitri/pgloader:latest

USER root
WORKDIR /app

# 复制迁移配置文件
COPY . .

# 不自动执行迁移！保持容器运行，等待手动执行
# 优点：可以随时重跑、方便调试、查看日志
CMD ["tail", "-f", "/dev/null"]
