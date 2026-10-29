/**
 * dsh-llm-first-token-guard
 *
 * ## 为什么需要这个插件
 *
 * DSH 的 `idleWatchdog`（`@deepseek-ai/dsh-timeout`）只在上游「一个 chunk 都不产生」
 * 时才会超时。但自建 newapi 这类网关会在上游模型卡住时持续回 SSE 心跳/保活字节，
 * 连接看起来一直健康，于是 watchdog 永远走不到 300s，请求无限挂起：
 *
 *   - `sessionStats.openStep.firstTokenTime` 永远为 null
 *   - `llm/retry` 事件一次都不产生（因为根本没失败过，重试逻辑无从触发）
 *   - 界面上显示成「深度求索中，用时 12 分钟」，实际模型一个 token 都没吐
 *
 * 业界同类问题与结论一致（anthropic-sdk-typescript #998 / #867、litellm #34819）：
 * 客户端必须区分「服务器还在思考」和「流已经静默卡死」，而心跳是唯一的活性证据。
 * 但只靠心跳不够 —— 心跳证明连接活着，不证明模型在出内容。所以需要一个
 * **首内容超时**：从请求发出到第一个真实内容块的硬期限，心跳不重置它。
 *
 * ## 关键设计：计时器跨 chunk 存活
 *
 * 最容易写错的地方正是这里：如果每收到一个 chunk 就重建一次"下一次 next() 的超时"，
 * 那么持续的心跳会不断重置计时器 —— 那就是把 DSH 原来的缺陷原样复刻了一遍。
 *
 * 本插件的正确做法：**阶段计时器只 arm 一次**，在
 *   - `firstTokenMs`：流开始 → 第一个真实内容块；
 *   - `idleAfterFirstMs`：上一个真实内容块 → 下一个真实内容块；
 * 之间持续计时。心跳到达只增加计数，**不重新 arm 计时器**。
 *
 * ## 实现位置：不碰核心包
 *
 * DSH 的 `LlmRuntime.stream()` 通过 `ctx.waterfall(this, "llm/stream", options, next)`
 * 暴露给插件。本插件在最外层包一个异步生成器。超时以抛错收尾，
 * 因为 `llm/stream` 是 waterfall：插件抛错 = 本次请求失败，DSH 的
 * `agent/request-error` 恢复点会按 provider 的 `retryPolicy` 正常重试。
 */

import z from "@deepseek-ai/schemastery";

export const name = "llm-first-token-guard";
export const inject = ["llm"];

/** 默认首内容超时：60s。普通请求 TTFT 通常 1-10s，60s 足以容忍排队抖动。 */
const DEFAULT_FIRST_TOKEN_MS = 60_000;
/** 默认内容后的最大停顿：120s。深度思考在两次内容之间可能静默，但不应超过 2 分钟。 */
const DEFAULT_IDLE_AFTER_FIRST_MS = 120_000;

/** Runtime schema for the plugin configuration. */
export const Config = z.object({
	firstTokenMs: z.number().min(1000).default(DEFAULT_FIRST_TOKEN_MS)
		.description("毫秒：从请求发出到第一个真实内容块的硬期限，心跳不重置"),
	idleAfterFirstMs: z.number().min(1000).default(DEFAULT_IDLE_AFTER_FIRST_MS)
		.description("毫秒：拿到首个内容后，两次真实内容之间允许的最长静默，心跳不重置"),
	guardProviders: z.array(z.string()).default([])
		.description("只守卫这些 provider；留空表示守卫全部"),
	debug: z.boolean().default(false)
		.description("打印首内容耗时与心跳计数"),
});

/**
 * 第一个「真实内容」chunk 的判定。
 * 心跳/usage/空 delta 都不算：它们只证明连接活着。
 */
function isContentChunk(chunk) {
	switch (chunk.type) {
		case "text-delta":
		case "reasoning-delta":
			return typeof chunk.text === "string" && chunk.text.length > 0;
		case "tool-call-delta":
			return (typeof chunk.argumentsDelta === "string" && chunk.argumentsDelta.length > 0) || chunk.name !== undefined;
		case "block-start":
		case "block-end":
			return true;
		default:
			return false;
	}
}

/** 构造带 `failure` 字段的错误，让 DSH 的失败归一化能识别成 TIMEOUT。 */
function guardError(message, timeoutMs) {
	const error = new Error(message);
	error.name = "LlmFirstTokenGuardError";
	error.failure = Object.freeze({ message, code: "TIMEOUT" });
	error.guardTimeoutMs = timeoutMs;
	return error;
}

/**
 * 一个「阶段计时器」：从创建到 fire()/dispose() 之间持续计时，**不随 chunk 重置**。
 * @returns {{ promise: Promise<never>, dispose: () => void, fired: () => boolean }}
 */
function armPhaseTimeout(timeoutMs, message, externalSignal) {
	let timer;
	let disposed = false;
	let fired = false;
	let onAbort;
	const promise = new Promise((_, reject) => {
		timer = setTimeout(() => {
			if (disposed) return;
			fired = true;
			reject(guardError(message, timeoutMs));
		}, timeoutMs);
		if (externalSignal !== undefined) {
			onAbort = () => {
				if (disposed) return;
				reject(externalSignal.reason ?? new Error("aborted"));
			};
			if (externalSignal.aborted) onAbort();
			else externalSignal.addEventListener("abort", onAbort, { once: true });
		}
	});
	// 避免未处理的 rejection：只在我们主动 race 时才消费它。
	promise.catch(() => {});
	return {
		promise,
		fired: () => fired,
		dispose: () => {
			disposed = true;
			clearTimeout(timer);
			if (externalSignal !== undefined && onAbort !== undefined) externalSignal.removeEventListener("abort", onAbort);
		},
	};
}

/**
 * 包装 chunk 流，加入两级「真实内容」超时。
 */
export async function* guardStream(source, options) {
	const { firstTokenMs, idleAfterFirstMs, signal, logger, provider, model, debug, onTimeout } = options;
	const iterator = source[Symbol.asyncIterator]();
	const startedAt = Date.now();
	let firstContentSeen = false;
	let lastContentAt = startedAt;
	let heartbeats = 0;
	let completed = false;

	// 当前阶段计时器（跨多次 next() 存活，这是修复的核心）。
	let phase = null;
	const startPhase = () => {
		phase?.dispose();
		const isFirst = !firstContentSeen;
		const timeoutMs = isFirst ? firstTokenMs : idleAfterFirstMs;
		const message = isFirst
			? `first-token-guard: no first content from ${provider}/${model} within ${timeoutMs}ms (upstream kept the connection open but produced no content)`
			: `first-token-guard: no new content from ${provider}/${model} for ${timeoutMs}ms (upstream kept the connection open but stalled)`;
		phase = armPhaseTimeout(timeoutMs, message, signal);
	};
	startPhase();

	try {
		while (true) {
			let result;
			try {
				// 与「阶段计时器」竞争，而不是每次都新建一个短超时。
				result = await Promise.race([iterator.next(), phase.promise]);
			} catch (error) {
				if (error && error.name === "LlmFirstTokenGuardError") {
					const elapsed = Date.now() - startedAt;
					const detail = firstContentSeen
						? `stalled after ${elapsed - (lastContentAt - startedAt)}ms of real silence with ${heartbeats} keep-alive chunk(s)`
						: `zero content after ${elapsed}ms with ${heartbeats} keep-alive chunk(s)`;
					logger?.warn?.(`dsh-llm-first-token-guard: ${provider}/${model} ${detail}; failing the attempt so retryPolicy can act`);
					try { onTimeout?.(); } catch { /* 中止上游失败不影响收尾 */ }
				}
				throw error;
			}

			if (result.done) {
				completed = true;
				return;
			}

			const chunk = result.value;
			if (chunk.type === "finish") {
				// 终止 chunk 原样透传；DSH 自己根据 reason 判定 error/aborted/stop。
				completed = true;
				yield chunk;
				return;
			}

			if (isContentChunk(chunk)) {
				if (!firstContentSeen) {
					firstContentSeen = true;
					if (debug) logger?.info?.(`dsh-llm-first-token-guard: ${provider}/${model} first content after ${Date.now() - startedAt}ms (${heartbeats} keep-alive(s))`);
				}
				lastContentAt = Date.now();
				// 有真实内容 → 进入下一阶段：从此刻重新计 idleAfterFirstMs。
				startPhase();
			} else {
				// 心跳/usage/空 delta：只计数。**不重置计时器** —— 这正是本插件与原版 idleWatchdog 的分水岭。
				heartbeats += 1;
			}

			yield chunk;
		}
	} finally {
		phase?.dispose();
		if (!completed) {
			const close = iterator.return?.bind(iterator);
			if (close) {
				try { await close(); } catch { /* 关闭失败不影响收尾 */ }
			}
		}
	}
}

/**
 * @param ctx cordis 上下文
 * @param config 插件配置
 * @param internals 仅测试注入：`minTimeoutMs` 允许单测用毫秒级超时。
 */
export function apply(ctx, config = {}, internals = {}) {
	// 生产下限 1000ms：低于 1 秒的超时对真实模型请求没有意义，只会造成误杀。
	// 单测通过 internals.minTimeoutMs 注入更小的下限，避免为了测试放宽生产约束。
	const minTimeoutMs = internals.minTimeoutMs ?? 1000;
	const firstTokenMs = (() => {
		const v = config.firstTokenMs ?? DEFAULT_FIRST_TOKEN_MS;
		if (!Number.isFinite(v) || v < minTimeoutMs) throw new Error(`llm-first-token-guard: firstTokenMs must be a finite number >= ${minTimeoutMs}`);
		return v;
	})();
	const idleAfterFirstMs = (() => {
		const v = config.idleAfterFirstMs ?? DEFAULT_IDLE_AFTER_FIRST_MS;
		if (!Number.isFinite(v) || v < minTimeoutMs) throw new Error(`llm-first-token-guard: idleAfterFirstMs must be a finite number >= ${minTimeoutMs}`);
		return v;
	})();
	const only = new Set(config.guardProviders ?? []);
	const debug = config.debug === true;

	ctx.effect(() => {
		const dispose = ctx.on("llm/stream", (options, next) => {
			const provider = String(options.provider ?? "");
			if (only.size > 0 && !only.has(provider)) return next();

			const source = next();
			return guardStream(source, {
				firstTokenMs,
				idleAfterFirstMs,
				provider,
				model: String(options.model ?? ""),
				signal: options.signal,
				logger: ctx.logger,
				debug,
				// 中止上游 HTTP 连接：signal 是调用方的，这里通过给 adapter 的机会有限，
				// 但 guardStream 的 finally 会调用 iterator.return()，pi-ai 会据此 abort consumer。
			});
		});
		return () => dispose?.();
	}, "llm-first-token-guard: intercept llm/stream");
}
