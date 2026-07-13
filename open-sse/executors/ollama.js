import { DefaultExecutor } from "./default.js";
import { peekNdjsonError } from "../utils/streamHelpers.js";
import { HTTP_STATUS } from "../config/runtimeConfig.js";

// In-band error wording that maps to a rate-limit / quota exhaustion → account fallback.
const OLLAMA_LIMIT_PATTERN = /usage limit|rate limit|too many requests|quota|limit reached|exceeded/i;

/**
 * Ollama Cloud Executor.
 *
 * Ollama streams NDJSON (`application/x-ndjson`) and — per its API docs — cannot
 * change the HTTP status once the body has started. So usage/rate-limit errors
 * arrive in-band with HTTP 200 as a `{"error":"..."}` line instead of a 4xx.
 *
 * Following the same protocol as the Codex executor, we peek the leading bytes
 * of the 200-OK body: if an error appears before any content, we synthesize a
 * real non-2xx Response. chatCore's generic `!providerResponse.ok` path then
 * runs the uniform account-fallback protocol (parseUpstreamError →
 * markAccountUnavailable → next account) — identical to a native 4xx.
 *
 * Once content has streamed, the status is already committed; we re-assemble the
 * stream and pass it through unchanged (no mid-stream retry).
 */
export class OllamaExecutor extends DefaultExecutor {
  constructor() {
    super("ollama");
  }

  async execute(args) {
    const result = await super.execute(args);
    const response = result?.response;
    if (!response || !response.ok || !response.body) return result;

    const { earlyError, response: rebuilt } = await peekNdjsonError(response);
    if (earlyError) {
      const status = OLLAMA_LIMIT_PATTERN.test(earlyError)
        ? HTTP_STATUS.RATE_LIMITED
        : HTTP_STATUS.BAD_GATEWAY;
      args.log?.warn?.("RETRY", `OLLAMA | in-band error "${earlyError}" → ${status} (account fallback)`);
      result.response = ollamaInbandErrorResponse(status, earlyError);
      return result;
    }

    result.response = rebuilt;
    return result;
  }
}

// Synthesize a non-2xx Response from an in-band Ollama error so the uniform
// upstream-error path handles it (mirrors codex's codexSseErrorResponse).
function ollamaInbandErrorResponse(status, message) {
  return new Response(JSON.stringify({
    error: {
      message,
      type: status === HTTP_STATUS.RATE_LIMITED ? "rate_limit_error" : "server_error",
      code: status === HTTP_STATUS.RATE_LIMITED ? "rate_limit_exceeded" : "upstream_error",
    }
  }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default OllamaExecutor;
