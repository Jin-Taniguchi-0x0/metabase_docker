FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Add curl installation
RUN apt-get update && apt-get install -y curl && apt-get install sudo
RUN apt-get install -y netcat-openbsd

COPY . .

CMD ["python", "app.py"]
# curl -X POST http://metabase:3000/api/session -H "Content-Type: application/json" -d '{"username": "root", "password": "root"}'
