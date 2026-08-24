# llm-proxy-retry

单机 HTTP/SSE 反向代理。下游响应命中指定状态码或字符串时，在保持客户端请求的同时重试。

## 行为

- 使用字面量最长前缀路由，不支持正则。
- 同一路由下的 backend 使用平滑加权轮询。
- 新请求优先选择不在冷却期的 backend；全部冷却时按权重选择一个并等待。
- 请求一旦选定 backend，所有重试都固定在该 backend，不进行故障切换。
- backend 返回可重试结果后进入共享 `retry_delay`，其他新请求会暂时避开它。
- 每个 backend 独立配置重试状态码、关键词、延迟、单次超时和最大重试窗口。
- 达到最大重试窗口后返回最后一次完整的下游 HTTP 响应。
- SSE `200` 响应立即透传并逐块 Flush，不检查关键词，输出开始后不重试。
- 客户端断开后立即取消等待和下游请求。
- 配置只在启动时读取，修改后需要重启。

## 启动

```bash
cp config.example.yaml config.yaml
go run ./cmd/llm-proxy -config config.yaml
```

构建：

```bash
go build -o llm-proxy ./cmd/llm-proxy
./llm-proxy -config config.yaml
```

配置使用严格校验，未知字段、重复路由、无效 URL 或非正数时间会导致进程拒绝启动。

## 路径处理

`prefix` 按路径段边界匹配。例如 `/A` 匹配 `/A` 和 `/A/chat`，但不匹配 `/ABC`。多个路由同时匹配时使用最长前缀。

```yaml
- prefix: /A
  strip_prefix: true
  backends:
    - url: https://example.com/base
```

上例会把 `/A/v1/chat?x=1` 转发到 `https://example.com/base/v1/chat?x=1`。设置 `strip_prefix: false` 后则转发到 `https://example.com/base/A/v1/chat?x=1`。

backend URL 自带的查询参数会放在客户端查询参数之前，两部分均保留。

## 重试语义

`retry_statuses` 和 `retry_keywords` 是“或”关系。关键词使用区分大小写的原始字节子串匹配，不使用正则。

重试始终使用 backend 的 `retry_delay`，不会采用下游的 `Retry-After`。冷却时间从收到可重试结果时开始计算。

`attempt_timeout` 限制一次请求在响应提交给客户端之前所花的时间。成功的 SSE 或已经开始透传的大响应不再受该超时和重试窗口限制。

`max_retry_duration` 从 Proxy 收到客户端请求时开始计算。达到截止时间后：

- 最近一次失败是完整 HTTP 响应时，返回它的状态码、Header 和 body。
- 最近一次失败没有 HTTP 响应时，超时返回 `504`，其他网络错误返回 `502`。

Proxy 会重试包括 `POST` 在内的所有 HTTP 方法。下游可能已经处理了请求，因此重试可能造成重复调用或重复计费。

重试期间 Proxy 不向客户端写入响应数据。部署在其他网关或负载均衡器之后时，其请求超时必须大于 `max_retry_duration`。

## 缓冲限制

为支持重试，请求体会先缓冲。小于 `memory_request_body_bytes` 时保存在内存，超过后写入 `temp_dir`，总大小不能超过 `max_request_body_bytes`。

关键词判断要求在向客户端发送响应前读取响应。Proxy 只检查不超过 `max_inspect_response_body_bytes` 的完整响应：

- 状态码命中时始终重试；错误体超过限制时暂时保留当前响应，冷却结束后关闭并重试，截止时间先到则完整透传该响应。
- 非状态码触发的响应超过限制时直接透传，不再检查关键词。
- 带非 `identity` `Content-Encoding` 的响应只按状态码判断，不检查关键词。
- SSE 只按状态码判断；`200 text/event-stream` 完全不检查关键词。

该限制避免任意大响应占满内存，同时保证被保留的最后错误可以原样返回。应根据下游错误体的最大尺寸设置该值。

## HTTP 透明性

请求方法、请求体、查询参数、认证 Header 和其他端到端 Header 会保留。以下内容按 HTTP 代理规则处理：

- `Connection`、`Transfer-Encoding` 等逐跳 Header 不会跨代理转发。
- 默认将 `Host` 改为 backend 的主机名；可通过 `preserve_host: true` 保留客户端 Host。
- TCP 分包和 HTTP chunk 边界不保证与客户端请求一致，正文内容不被改写。
- WebSocket/HTTP Upgrade 不在支持范围内；SSE 支持 HTTP/1.1 和 HTTP/2。

## Docker

```bash
cp config.example.yaml config.yaml
docker compose up -d --build
docker compose logs -f llm-proxy
```

构建默认使用 `https://goproxy.cn,direct` 和 `sum.golang.google.cn`。需要覆盖时：

```bash
GOPROXY=https://proxy.golang.org,direct \
GOSUMDB=sum.golang.org \
docker compose build
```

宿主机端口默认为 `8080`，可通过 `LLM_PROXY_PORT=18080 docker compose up -d` 修改。
