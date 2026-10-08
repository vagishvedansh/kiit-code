FROM golang:alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /server .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tor netcat-openbsd tini

WORKDIR /app
COPY --from=builder /server /app/server
COPY --from=builder /app/prompts /app/prompts
COPY torrc /etc/tor/torrc
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8787

ENTRYPOINT ["/sbin/tini", "--", "/app/entrypoint.sh"]
