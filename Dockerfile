FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y \
      pgloader \
      ca-certificates \
      mysql-client \
      postgresql-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY config.load .

CMD ["pgloader", "config.load"]
