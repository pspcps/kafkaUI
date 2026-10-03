FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o kafbat-ui-server ./cmd/server

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/kafbat-ui-server .

EXPOSE 8080
CMD ["./kafbat-ui-server"]
