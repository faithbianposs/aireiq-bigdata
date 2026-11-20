FROM python:3.10-slim-trixie
RUN pip install --no-cache-dir kafka-python==3.0.13
WORKDIR /app
COPY app.py .
COPY iot.py .
COPY supabase_bridge.py test_supabase_bridge.py ./
COPY verify_supabase_kafka.py ./
COPY minutes.py test_minutes.py ./
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "app.py"]
