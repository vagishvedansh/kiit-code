FROM golang:1.22-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o /server .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tor sqlite-libs

WORKDIR /app
COPY --from=builder /server /app/server
COPY --from=builder /app/prompts /app/prompts

EXPOSE 8787

RUN echo "ControlPort 9051" > /etc/tor/torrc && \
    echo "SOCKSPort 9050" >> /etc/tor/torrc && \
    echo "CookieAuthentication 0" >> /etc/tor/torrc

CMD tor & /app/server
