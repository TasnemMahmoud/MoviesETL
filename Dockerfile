
FROM python:3.9-alpine


WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


COPY . .


ENV GOOGLE_APPLICATION_CREDENTIALS=/app/ServiceKey_GoogleCloud.json
ENV PUBSUB_EMULATOR_HOST=localhost:8085


CMD ["python3", "create_bucket.py"]
