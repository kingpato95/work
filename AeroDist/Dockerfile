FROM golang:1.25-alpine AS builder

WORKDIR /app

# 1. Copiar dependencias
COPY go.mod go.sum ./
RUN go mod download

# 2. Copiar TODO el código fuente (incluyendo los .pb.go que ya generaste)
COPY . .

# 3. Compilar el binario específico que le pidamos
# Usamos ARG para reutilizar este mismo archivo para broker, datanode, etc.
ARG SERVICE_NAME
RUN go build -o /bin/app ./cmd/${SERVICE_NAME}

# 4. Imagen final ligera
FROM alpine:latest
COPY --from=builder /bin/app /app
ENTRYPOINT ["/app"]