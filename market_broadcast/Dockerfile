FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

RUN go build -o broadcast ./cmd/broadcast

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/broadcast .

ENV LISTEN_ADDR=:8080

EXPOSE 8080

ENTRYPOINT ["./broadcast"]
