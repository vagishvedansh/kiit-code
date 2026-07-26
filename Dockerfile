FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    tor \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8787

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

CMD ./entrypoint.sh
