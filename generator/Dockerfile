FROM python:3.12-slim
WORKDIR /app
COPY generate.py /app/generate.py
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/generate.py"]
