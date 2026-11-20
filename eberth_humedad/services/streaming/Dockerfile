FROM aireiq-jupyter:latest
RUN pip install --no-cache-dir kafka-python==3.0.13
WORKDIR /app
COPY streaming.py .
COPY forecast.py .
COPY bi.py .
COPY live_forecast.py live_state.py test_live_state.py ./
ENTRYPOINT ["spark-submit", "--master", "local[2]", "--driver-memory", "1g", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0", "/app/streaming.py"]
