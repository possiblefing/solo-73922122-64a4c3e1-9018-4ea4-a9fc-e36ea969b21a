FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8001
# 单进程同时运行：对外 TCP 中继(:8001，含关闭握手不响应场景) + 内部 uvicorn(:8011)
CMD ["python", "app.py"]
