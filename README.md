# dsh-llm-first-token-guard

心跳感知的**首内容超时**守卫，解决 DSH（DeepSeek Harness）里「上游持续回 SSE 心跳、但一个 token 都不吐」导致的无限挂起。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

---

## 问题：会话卡住不动，界面却一直显示"求索中"

DSH 内置的 `idleWatchdog`（`@deepseek-ai/dsh-timeout`）只在**一个 chunk 都不产生**时才超时。而网关类上游（自建 newapi、LiteLLM 等）在模型卡住时常会持续下发 SSE 心跳/保活字节，连接看起来一直健康：

- `idleWatchdog` 被心跳无限重置，永远走不到默认的 300s
- 请求永挂：`sessionStats.openStep.firstTokenTime` 永远是 `null`
- 一次 `llm/retry` 都不产生，因为根本没"失败"过，重试逻辑无从触发
- 界面上显示成「深度求索中，用时 12 分钟」，实际模型一个 token 都没吐
- 状态文件里 `llmRetry` 是空的，让人无从判断它到底在干嘛

**根因一句话**：`idleWatchdog` 的"空闲"定义是"没有 chunk"，而心跳也是 chunk。所以心跳越勤，越永远不超时。

## 修复思路

区分**连接活性**与**内容进展**：

- 心跳只能证明连接活着，**不能**证明模型在出内容
- 因此按「真实内容块」计时，心跳只计数、**不重置**任何计时器

| 阶段 | 计时器 | 心跳是否重置 |
|---|---|---|
| 请求发出 → 第一个真实内容块 | `firstTokenMs`（默认 60s） | **否** |
| 上一个真实内容块 → 下一个真实内容块 | `idleAfterFirstMs`（默认 120s） | **否** |

「真实内容块」指 `text-delta` / `reasoning-delta` / `tool-call-delta` / `block-start` / `block-end`。`usage`、空 delta、SSE 保活注释只计数，不重置计时器。

超时**以 `finish` chunk 收尾**，不是抛异常：

```js
{ type: "finish", reason: { kind: "error", failure: { message, code: "TIMEOUT" } } }
```

因为 `dsh-agent-loop` 只有在拿到 `finish` 且 `reason.kind === "error"` 时才走 `agent/request-error` 瀑布去问 retryPolicy；在流迭代中抛出的异常会被直接 rethrow，整轮立刻失败、**一次都不重试**（v0.1.1 的 bug，2026-11-01 实测修复）。所以现在超时会按 provider 的 `retryPolicy` 正常重试，错误码也归一成 `TIMEOUT`。`guardStream` 的 `finally` 会调用 `iterator.return()`，pi-ai 适配器据此执行 `consumer.abort(...)`，上游 HTTP 连接随之关闭。

**不修改任何 DSH 核心包**，纯插件实现。

### 实现位置

DSH 的 `LlmRuntime.stream()` 通过 `ctx.waterfall(this, "llm/stream", options, next)` 暴露给插件。本插件在最外层包一个异步生成器，因此对所有走 `llm/stream` 的 provider 生效。

最容易写错的地方在于：如果每收到一个 chunk 就重建一次"下一次 `next()` 的超时"，持续的心跳会把计时器无限续命，等于把原缺陷原样复刻一遍。所以本插件的**阶段计时器只 arm 一次，跨多次 `next()` 存活**。

## 安装

```bash
dsh plugin --profile web add github:xingzoudefengye/dsh-llm-first-token-guard
```

或手动放进 profile：

```bash
cd ~/.dsh/profiles/web
git clone https://github.com/xingzoudefengye/dsh-llm-first-token-guard.git node_modules/dsh-llm-first-token-guard
```

插件自带 `cordis.patch.yml`，`dsh` 的 bundle 机制会自动挂载。重启 DSH 生效。

## 配置

默认值已经可用。要调整时在 profile 的 `cordis.patch.yml` 里覆盖：

```yaml
- insert:
    - id: llm-first-token-guard
      name: 'dsh-llm-first-token-guard'
      config:
        firstTokenMs: 60000       # 首内容硬期限
        idleAfterFirstMs: 120000  # 内容之间的最长静默
        guardProviders: []        # 只守卫这些 provider；留空 = 全部
        debug: false              # 打印首内容耗时与心跳计数
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `firstTokenMs` | `60000` | 请求发出到第一个真实内容块的硬期限。普通请求 TTFT 通常 1-10s，60s 足以容忍排队抖动 |
| `idleAfterFirstMs` | `120000` | 拿到首个内容后，两次真实内容之间允许的最长静默 |
| `guardProviders` | `[]` | 只守卫这些 provider 名；留空守卫全部 |
| `debug` | `false` | 打印首内容耗时与心跳计数，便于定位上游问题 |

## 测试

```bash
node --test --test-timeout=15000 lib/index.test.js
```

覆盖 5 个场景：

1. 正常流：首内容及时到达，原样透传并在 `finish` 结束
2. **致命场景**：心跳持续但永不吐内容 → 在 `firstTokenMs` 后失败，而不是无限挂起
3. 拿到首内容之后的心跳不能掩盖卡死 → `idleAfterFirstMs` 生效
4. `guardProviders` 白名单：不在名单内的 provider 完全不包裹
5. 调用方 abort 时立即结束，不等到超时

## 业界参考

同类问题与结论：

- anthropic-sdk-typescript [#998 — Ping-aware streaming watchdog](https://github.com/anthropics/anthropic-sdk-typescript/issues/998)
- anthropic-sdk-typescript [#867 — Streaming Idle Timeout to Prevent Indefinite Hangs](https://github.com/anthropics/anthropic-sdk-typescript/issues/867)
- LiteLLM [#34819 — SSE streaming responses emit zero bytes during a long time-to-first-token](https://github.com/BerriAI/litellm/issues/34819)

核心共识：**心跳是唯一的活性证据，但只有心跳不够**。客户端必须能区分「服务器在思考」与「流已静默卡死」。

## 已知边界

- 守卫的是**内容**而非字节。若上游用合法 chunk 反复发空 delta，在第一个内容块之前一直假活，仍会在 `firstTokenMs` 失败，这是期望行为。
- 深度思考阶段如果**完全不产生** `reasoning-delta`（真实静默），会按 `idleAfterFirstMs` 判定超时；必要时调大该值到 300000。
- 这是**客户端侧兜底**。根因（网关/上游不发内容）仍需在上游侧排查；本插件保证的是"最多等 60 秒就明确失败并重试"，而不是"让上游恢复"。

## 协议

MIT
