# Ombre Brain 全环境变量清单

本文是环境变量名称的唯一文档真源。代码、Docker、Zeabur 模板和 README 只能引用这里已经登记的名称；禁止静默改名。确需更名时，旧名必须保留兼容映射、弃用提示和迁移说明。

## 模型与向量

- `OMBRE_COMPRESS_API_KEY`：打标、脱水和合并模型密钥。
- `OMBRE_COMPRESS_BASE_URL`：OpenAI 兼容模型地址。
- `OMBRE_COMPRESS_MODEL`：打标模型名。
- `OMBRE_COMPRESS_FORMAT`：打标 API 格式。
- `OMBRE_COMPRESS_API_FORMAT`：`OMBRE_COMPRESS_FORMAT` 的旧兼容名。
- `OMBRE_COMPRESS_TIMEOUT_SECONDS`：打标请求超时秒数。
- `OMBRE_EMBED_API_KEY`：向量模型密钥。
- `OMBRE_EMBED_BASE_URL`：向量 API 地址。
- `OMBRE_EMBED_MODEL`：向量模型名。
- `OMBRE_EMBED_FORMAT`：向量 API 格式。
- `OMBRE_EMBED_TIMEOUT_SECONDS`：向量请求超时秒数。
- `OMBRE_EMBED_BACKEND`：向量后端，例如 `api` 或 `local`。
- `OMBRE_OLLAMA_URL`：本地 Ollama 地址。

## 存储、媒体与日志

- `OMBRE_VAULT_DIR`：推荐的数据根目录。
- `OMBRE_BUCKETS_DIR`：`OMBRE_VAULT_DIR` 的旧兼容名。
- `OMBRE_MEDIA_DIR`：永久媒体目录；默认 `<数据根目录>/_media`。
- `OMBRE_MEDIA_MAX_BYTES`：单个媒体文件最大字节数，默认 25 MiB。
- `OMBRE_CONFIG_PATH`：持久配置文件路径。
- `OMBRE_CODE_DIR`：容器中持久运行代码目录。
- `OMBRE_LOG_DIR`：日志目录。
- `OMBRE_LOG_FILE`：日志文件路径。
- `OMBRE_EXTERNAL_CHANGE_POLL_SECONDS`：外部 Markdown 变动轮询间隔。

## HTTP、MCP 与鉴权

- `OMBRE_TRANSPORT`：`stdio`、`sse` 或 `streamable-http`。
- `OMBRE_PORT`：容器或裸机监听端口。
- `OMBRE_BIND_HOST`：进程实际监听地址；容器/PaaS 通常需要 `0.0.0.0`，裸机仅限本机访问时应设为 `127.0.0.1`。
- `OMBRE_MCP_REQUIRE_AUTH`：是否要求 MCP 鉴权。
- `OMBRE_MCP_AUTH_MODE`：`oauth`、`token` 或 `hybrid`。`hybrid` 保留 OAuth 动态注册，同时让 `Authorization: Bearer` 也接受预置静态 Token；关闭鉴权仍由 `OMBRE_MCP_REQUIRE_AUTH=false` 控制。
- `OMBRE_MCP_TOKEN`：静态 Token / OAuth + 静态 Token 共存模式的预置密钥。
- `OMBRE_ALLOW_INSECURE_MCP`：Dashboard / 部署向导保存非回环免鉴权组合、以及内置 Tunnel 免鉴权启动时的高风险确认。直接设置 `OMBRE_MCP_REQUIRE_AUTH=false` 会按明确配置生效；该变量不再是启动期暗中改写鉴权开关的条件。
- `OMBRE_DASHBOARD_PASSWORD`：Dashboard 密码。
- `OMBRE_DASHBOARD_SESSION_DAYS`：Dashboard 登录会话天数。
- `OMBRE_TRUSTED_PROXY_CIDRS`：直接连接 OB 的最后一跳可信反向代理 CIDR；不是公网客户端 IP 或域名，禁止使用 `0.0.0.0/0`。官方 Compose 模板会从 `.env` 透传该值，修改后需要重新创建容器。

## Tunnel、Hook 与 GitHub

- `OMBRE_GITHUB_TOKEN`：GitHub 备份或更新访问令牌；自动恢复部署需要仓库 Contents 读写权限。
- `OMBRE_GITHUB_REPO`：环境变量形式的备份仓库（`owner/repo`），优先于临时盘里的面板配置。
- `OMBRE_GITHUB_BRANCH`：备份分支，默认 `main`。
- `OMBRE_GITHUB_PATH_PREFIX`：仓库内的备份前缀，默认 `ombre`。
- `OMBRE_GITHUB_AUTO_INTERVAL_MINUTES`：定时备份间隔；设为 `1` 时启动恢复完成后立即备份，此后每分钟一次。
- `OMBRE_GITHUB_AUTO_RESTORE`：启动时本地记忆为空则先从 GitHub 恢复；恢复失败会阻止自动同步启动，避免空仓库覆盖有效备份。
- `OMBRE_GITHUB_STATE_KEY`：至少 32 字节的稳定高熵密钥，用 AES-GCM 加密备份 OAuth `client_id`/grant 状态。只允许配合私有仓库；更换或丢失后旧密文无法恢复。
- `OMBRE_HOOK_URL`：外部 Hook 地址。
- `OMBRE_HOOK_TOKEN`：Hook 鉴权令牌。
- `OMBRE_HOOK_SKIP`：跳过 Hook。
- `OMBRE_HOOK_ALLOW_PUBLIC`：允许公网 Hook 地址。
- `TUNNEL_EDGE`：cloudflared 边缘节点覆盖值。
- `TUNNEL_TRANSPORT_PROTOCOL`：cloudflared 传输协议。

## 更新与容器维护

- `OMBRE_ALLOW_CUSTOM_UPDATE_REPO`：允许自定义更新仓库。
- `OMBRE_ALLOW_UNTRUSTED_MIRROR`：允许未受信任镜像源。
- `OMBRE_UPDATE_ALLOW_PIP`：允许热更新执行 pip。
- `OMBRE_FORCE_CODE_RESEED`：下次启动强制从镜像重播代码；使用后应移除。
- `OMBRE_IMAGE_ROOT`：镜像内置代码根目录。
- `OMBRE_BOOTSTRAP_ONLY`：仅执行启动引导和诊断。
- `OMBRE_DOCKER_INTEGRATION_URL`：Docker 集成服务地址。
- `OMBRE_DOCKER_WEB_BASE_URL`：Docker Web 基地址。

## 部署编排与多所有者

- `OMBRE_BIND_ADDRESS`：Compose 宿主机对外绑定地址，默认 `127.0.0.1`；官方模板同时把该值传入容器，供 MCP 安全门禁判断真实宿主边界。
- `OMBRE_HOST_PORT`：Compose 宿主机端口。
- `OMBRE_HOST_VAULT_DIR`：Compose 宿主机数据目录。
- `OMBRE_CONTAINER_NAME`：目标容器名。
- `OMBRE_OWNER_NAME`：当前所有者名。
- `OMBRE_OWNER_COUNT`：多所有者实例数。
- `OMBRE_MING_VAULT_DIR`、`OMBRE_HONG_VAULT_DIR`：示例多所有者数据目录。
- `OMBRE_MING_PASSWORD`、`OMBRE_HONG_PASSWORD`：示例多所有者密码。
- `OMBRE_MING_MCP_TOKEN`、`OMBRE_HONG_MCP_TOKEN`：多所有者 Compose 中每个实例各自的静态 MCP Token；不要让多个 owner 共用同一密钥。
- `AI_NAME`：AI 显示名称。

## v1.x 兼容变量

这些旧名仍然生效，但新部署应使用右侧正式名称：

- `OMBRE_API_KEY` → `OMBRE_COMPRESS_API_KEY`
- `OMBRE_BASE_URL` → `OMBRE_COMPRESS_BASE_URL`
- `PASSWORD` → `OMBRE_DASHBOARD_PASSWORD`

正式名称和旧名同时存在时，以正式名称为准。删除兼容名必须经过一次明确的主版本迁移，并在发布说明中写出截止版本，禁止在普通重构中删除。
