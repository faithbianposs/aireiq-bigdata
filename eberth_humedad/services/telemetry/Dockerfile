FROM python:3.10-slim-trixie
WORKDIR /app
COPY exporter.py verify.py ./
COPY verify_s11.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "exporter.py"]
