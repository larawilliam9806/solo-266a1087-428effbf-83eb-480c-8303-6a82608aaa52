FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HLL_DB_PATH=/data/hll.sqlite3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY wsgi.py .

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 多 worker 并发写由 SQLite BEGIN IMMEDIATE + WAL 串行化；写锁等待上限 1s，
# 超时返回 503 可重试。线程数保证 16 笔并发写 + 下载请求不被 WSGI 层排队。
CMD ["gunicorn", "--bind=0.0.0.0:8000", "--workers=2", "--threads=16", \
     "--timeout=30", "wsgi:app"]
