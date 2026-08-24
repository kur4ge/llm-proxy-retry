# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS builder

ARG GOPROXY=https://goproxy.cn,direct
ARG GOSUMDB=sum.golang.google.cn

ENV GOPROXY=${GOPROXY} \
    GOSUMDB=${GOSUMDB} \
    CGO_ENABLED=0

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build \
    -buildvcs=false \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/llm-proxy \
    ./cmd/llm-proxy \
    && mkdir -p /runtime-tmp \
    && chmod 1777 /runtime-tmp

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /runtime-tmp /tmp
COPY --from=builder /out/llm-proxy /llm-proxy

USER 65532:65532
EXPOSE 8318

ENTRYPOINT ["/llm-proxy"]
CMD ["-config", "/config.yaml"]
