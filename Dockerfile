FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY predict_monitor_bot.py .
RUN python -m py_compile predict_monitor_bot.py
CMD ["python", "predict_monitor_bot.py"]
