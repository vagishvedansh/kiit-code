FROM python:3.11-alpine

RUN apk add --no-cache ca-certificates tor build-base libffi-dev openssl-dev

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY genuine_sessions.txt ./
COPY tor-zen-proxy.py ./

EXPOSE 8787

RUN echo "ControlPort 9051" > /etc/tor/torrc && \
    echo "SOCKSPort 9050" >> /etc/tor/torrc && \
    echo "CookieAuthentication 0" >> /etc/tor/torrc

CMD tor & python tor-zen-proxy.py --port 8787 --socks 127.0.0.1:9050 --control 127.0.0.1:9051
