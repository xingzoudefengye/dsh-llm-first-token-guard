import { test } from "node:test";
import assert from "node:assert/strict";
import { apply } from "./index.js";

// 用一个最小的假 ctx 模拟 cordis：只实现 effect/on/logger。
function fakeCtx() {
  const listeners = new Map();
  const ctx = {
    logger: { warn() {}, info() {}, error() {} },
    effect(fn) {
      const dispose = fn();
      return () => { if (typeof dispose === "function") dispose(); };
    },
    on(event, listener) {
      listeners.set(event, listener);
      return () => listeners.delete(event);
    },
    fire(event, options, next) {
      const listener = listeners.get(event);
      return listener ? listener(options, next) : next();
    },
  };
  return ctx;
}

// 可控的假上游流：先吐 N 个心跳，再吐内容，或者永远只吐心跳。
async function* makeUpstream({ heartbeats = 0, heartbeatIntervalMs = 10, contentAfter = Infinity, signal }) {
  for (let i = 0; i < heartbeats; i++) {
    await new Promise((r) => setTimeout(r, heartbeatIntervalMs));
    if (signal?.aborted) throw signal.reason;
    // 心跳：usage-only / 空 delta，DSH 里这类 chunk 不是真实内容
    yield { type: "usage", usage: { inputTokens: 1 } };
    if (i >= contentAfter) {
      yield { type: "text-delta", index: 0, text: "hello" };
      yield { type: "finish", reason: { kind: "stop" } };
      return;
    }
  }
  if (contentAfter !== Infinity) {
    yield { type: "text-delta", index: 0, text: "hello" };
    yield { type: "finish", reason: { kind: "stop" } };
    return;
  }
  // 永远挂着：只发心跳，不发内容
  while (true) {
    await new Promise((r) => setTimeout(r, heartbeatIntervalMs));
    if (signal?.aborted) throw signal.reason;
    yield { type: "usage", usage: { inputTokens: 1 } };
  }
}

async function drain(stream) {
  const chunks = [];
  for await (const c of stream) chunks.push(c);
  return chunks;
}

test("正常流：首内容及时到达，原样透传并在 finish 结束", async () => {
  const ctx = fakeCtx();
  apply(ctx, { firstTokenMs: 500, idleAfterFirstMs: 500 }, { minTimeoutMs: 1 });
  const stream = ctx.fire(
    "llm/stream",
    { provider: "p", model: "m" },
    () => makeUpstream({ heartbeats: 1, heartbeatIntervalMs: 5, contentAfter: 1 }),
  );
  const chunks = await drain(stream);
  assert.equal(chunks.at(-1).type, "finish");
  assert.ok(chunks.some((c) => c.type === "text-delta"));
});

test("致命场景：心跳持续但永不吐内容 → 在 firstTokenMs 后以 finish(error) 收尾，而不是无限挂起", async () => {
  const ctx = fakeCtx();
  apply(ctx, { firstTokenMs: 120, idleAfterFirstMs: 120 }, { minTimeoutMs: 1 });
  const started = Date.now();
  const stream = ctx.fire(
    "llm/stream",
    { provider: "p", model: "m" },
    () => makeUpstream({ heartbeatIntervalMs: 10, contentAfter: Infinity }),
  );
  const chunks = await drain(stream);
  const last = chunks.at(-1);
  assert.equal(last.type, "finish");
  assert.equal(last.reason.kind, "error");
  assert.equal(last.reason.failure.code, "TIMEOUT");
  assert.match(last.reason.failure.message, /no first content/);
  const elapsed = Date.now() - started;
  assert.ok(elapsed < 1000, `应在约 120ms 收尾，实际 ${elapsed}ms`);
});

// 回归保护：抛异常会被 agent-loop 当成「流中途崩溃」直接 rethrow，绕过
// agent/request-error，provider 的 retryPolicy 一次都不会被问到（2026-11-01 实测）。
test("超时不得抛异常：必须走 finish(error) 才会触发 agent/request-error 重试", async () => {
  const ctx = fakeCtx();
  apply(ctx, { firstTokenMs: 100, idleAfterFirstMs: 100 }, { minTimeoutMs: 1 });
  const stream = ctx.fire(
    "llm/stream",
    { provider: "p", model: "m" },
    () => makeUpstream({ heartbeatIntervalMs: 10, contentAfter: Infinity }),
  );
  await assert.doesNotReject(() => drain(stream));
});

test("拿到首内容之后的心跳不能掩盖卡死 → idleAfterFirstMs 生效", async () => {
  const ctx = fakeCtx();
  apply(ctx, { firstTokenMs: 500, idleAfterFirstMs: 100 }, { minTimeoutMs: 1 });
  // 先给一个真实内容，然后只发心跳、不再有内容
  async function* stream() {
    yield { type: "text-delta", index: 0, text: "first" };
    while (true) {
      await new Promise((r) => setTimeout(r, 10));
      yield { type: "usage", usage: { inputTokens: 1 } };
    }
  }
  const started = Date.now();
  const out = ctx.fire("llm/stream", { provider: "p", model: "m" }, () => stream());
  const it = out[Symbol.asyncIterator]();
  const first = await it.next();
  assert.equal(first.value.type, "text-delta");
  // 必须继续消费心跳，才能走到「内容之后」的超时判定
  let last;
  while (true) {
    const r = await it.next();
    if (r.done) break;
    last = r.value;
  }
  assert.equal(last.type, "finish");
  assert.equal(last.reason.kind, "error");
  assert.match(last.reason.failure.message, /no new content/);
  assert.ok(Date.now() - started < 1000, `应在约 100ms 后收尾，实际 ${Date.now() - started}ms`);
});

test("guardProviders 白名单：不在名单内的 provider 完全不包裹", async () => {
  const ctx = fakeCtx();
  apply(ctx, { firstTokenMs: 50, guardProviders: ["only-me"] }, { minTimeoutMs: 1 });
  const sentinel = { marker: true };
  const returned = ctx.fire("llm/stream", { provider: "other", model: "m" }, () => sentinel);
  assert.equal(returned, sentinel, "未守卫的 provider 应原样返回 next() 结果");
});

test("调用方 abort 时立即结束，不等到超时", async () => {
  const ctx = fakeCtx();
  apply(ctx, { firstTokenMs: 10_000, idleAfterFirstMs: 10_000 }, { minTimeoutMs: 1 });
  const controller = new AbortController();
  const stream = ctx.fire(
    "llm/stream",
    { provider: "p", model: "m", signal: controller.signal },
    () => makeUpstream({ heartbeatIntervalMs: 5, contentAfter: Infinity, signal: controller.signal }),
  );
  const started = Date.now();
  setTimeout(() => controller.abort(new Error("user cancelled")), 50);
  await assert.rejects(() => drain(stream));
  assert.ok(Date.now() - started < 1000, "取消应立即生效");
});

