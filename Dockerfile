FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /server main.go

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tor

WORKDIR /app
COPY --from=builder /server /app/server
COPY --from=builder /app/prompts /app/prompts

EXPOSE 8787

CMD tor & /app/server
