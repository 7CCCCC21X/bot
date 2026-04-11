FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY predict_monitor_bot.py .
RUN python -m py_compile predict_monitor_bot.py
RUN mkdir -p /app/data
VOLUME ["/app/data"]
CMD ["python", "predict_monitor_bot.py"]
