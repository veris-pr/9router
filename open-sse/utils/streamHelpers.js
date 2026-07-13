import { FORMATS } from "../translator/formats.js";

// Parse SSE data line
export function parseSSELine(line, format = null) {
  if (!line) return null;

  // NDJSON format (Ollama): raw JSON lines without "data:" prefix
  if (format === FORMATS.OLLAMA) {
    const trimmed = line.trim();
    if (trimmed.startsWith("{")) {
      try {
        return JSON.parse(trimmed);
      } catch (error) {
        return null;
      }
    }
    return null;
  }

  // Standard SSE format: "data: {...}"
  if (line.charCodeAt(0) !== 100) return null; // 'd' = 100

  const data = line.slice(5).trim();
  if (data === "[DONE]") return { done: true };

  try {
    return JSON.parse(data);
  } catch (error) {
    if (data.length > 0 && data.length < 1000) {
      console.log(`[WARN] Failed to parse SSE line (${data.length} chars): ${data.substring(0, 100)}...`);
    }
    return null;
  }
}

// Check if chunk has valuable content (not empty)
export function hasValuableContent(chunk, format) {
  // OpenAI format
  if (format === FORMATS.OPENAI && chunk.choices?.[0]?.delta) {
    const delta = chunk.choices[0].delta;
    return delta.content && delta.content !== "" ||
           delta.reasoning_content && delta.reasoning_content !== "" ||
           delta.tool_calls && delta.tool_calls.length > 0 ||
           chunk.choices[0].finish_reason ||
           delta.role;
  }

  // Claude format
  if (format === FORMATS.CLAUDE) {
    const isContentBlockDelta = chunk.type === "content_block_delta";
    const hasText = chunk.delta?.text && chunk.delta.text !== "";
    const hasThinking = chunk.delta?.thinking && chunk.delta.thinking !== "";
    const hasInputJson = chunk.delta?.partial_json && chunk.delta.partial_json !== "";
    
    if (isContentBlockDelta && !hasText && !hasThinking && !hasInputJson) {
      return false;
    }
    return true;
  }

  return true; // Other formats: keep all chunks
}

// Fix invalid id (generic or too short)
export function fixInvalidId(parsed) {
  if (parsed.id && (parsed.id === "chat" || parsed.id === "completion" || parsed.id.length < 8)) {
    const fallbackId = parsed.extend_fields?.requestId || 
                      parsed.extend_fields?.traceId || 
                      Date.now().toString(36);
    parsed.id = `chatcmpl-${fallbackId}`;
    return true;
  }
  return false;
}

function cleanUsagePayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return payload;
  }

  let cleaned = payload;

  if ("usage" in cleaned) {
    if (cleaned.usage === null) {
      const { usage, ...payloadWithoutUsage } = cleaned;
      cleaned = payloadWithoutUsage;
    } else if (typeof cleaned.usage === "object" && cleaned.usage.perf_metrics === null) {
      const { perf_metrics, ...usageWithoutPerf } = cleaned.usage;
      cleaned = { ...cleaned, usage: usageWithoutPerf };
    }
  }

  if (cleaned.response && typeof cleaned.response === "object" && !Array.isArray(cleaned.response)) {
    const cleanedResponse = cleanUsagePayload(cleaned.response);
    if (cleanedResponse !== cleaned.response) {
      cleaned = { ...cleaned, response: cleanedResponse };
    }
  }

  return cleaned;
}

/**
 * Peek the leading bytes of an NDJSON (Ollama) upstream stream to detect an
 * in-band error emitted with HTTP 200 before any content is generated.
 *
 * Ollama Cloud reports usage/rate limits mid-response: once the x-ndjson body
 * has started the HTTP status can no longer change, so the error arrives as a
 * `{"error":"..."}` line in the body instead of a 4xx status. Without this
 * peek the error line is silently dropped by the ollama→openai translator
 * (it has no `message`/`done`), the request looks successful, and account
 * fallback never fires.
 *
 * Returns { earlyError, response }:
 *  - earlyError: the error string if one appears before any content line, else null.
 *    When set, the reader is cancelled and `response` is null — the caller should
 *    treat it as a provider error and fall over to the next account.
 *  - response: a reconstructed Response replaying the peeked bytes, so the caller
 *    can stream it normally when no early error is present.
 *
 * @param {Response} providerResponse
 * @param {{ maxBytes?: number }} [opts]
 * @returns {Promise<{ earlyError: string|null, response: Response|null }>}
 */
export async function peekNdjsonError(providerResponse, { maxBytes = 65536 } = {}) {
  if (!providerResponse?.body) return { earlyError: null, response: providerResponse };

  const reader = providerResponse.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: false });
  const consumed = [];
  let textBuf = "";
  let peeked = 0;
  let earlyError = null;

  // Classify one NDJSON line: "error" (in-band error), "content" (generation
  // started or a normal terminal chunk), or null (empty/warmup/unparseable).
  const classify = (line) => {
    const t = line.trim();
    if (!t || t[0] !== "{") return null;
    let obj;
    try {
      obj = JSON.parse(t);
    } catch {
      return null;
    }
    if (obj.error) {
      earlyError = typeof obj.error === "string" ? obj.error : JSON.stringify(obj.error);
      return "error";
    }
    const msg = obj.message;
    const hasContent = msg && (msg.content || msg.thinking || (Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0));
    if (hasContent || obj.done) return "content";
    return null;
  };

  let decided = false;
  while (!decided) {
    let result;
    try {
      result = await reader.read();
    } catch {
      break; // upstream error while peeking — fall through, caller handles empty stream
    }
    const { value, done } = result;
    if (done) {
      // Stream ended: inspect any remaining buffered line (covers a non-streaming
      // single JSON object with no trailing newline).
      textBuf += decoder.decode();
      if (classify(textBuf)) decided = true;
      break;
    }
    consumed.push(value);
    peeked += value.byteLength || value.length || 0;
    textBuf += decoder.decode(value, { stream: true });

    const lines = textBuf.split("\n");
    // Only inspect complete lines (skip the trailing partial)
    for (let i = 0; i < lines.length - 1; i++) {
      if (classify(lines[i])) { decided = true; break; }
    }
    if (peeked >= maxBytes) break;
  }

  if (earlyError) {
    reader.cancel().catch(() => {});
    return { earlyError, response: null };
  }

  // Reconstruct the stream: replay the peeked bytes, then drain the rest of the reader.
  const replay = new ReadableStream({
    start(controller) {
      for (const c of consumed) controller.enqueue(c);
    },
    async pull(controller) {
      try {
        const { value, done } = await reader.read();
        if (done) {
          controller.close();
          return;
        }
        controller.enqueue(value);
      } catch (e) {
        controller.error(e);
      }
    },
    cancel(reason) {
      reader.cancel(reason).catch(() => {});
    }
  });

  // Preserve upstream headers but drop framing headers that no longer match the
  // reconstructed (already-decoded, unbounded) body.
  const headers = new Headers(providerResponse.headers);
  headers.delete("content-length");
  headers.delete("content-encoding");
  const response = new Response(replay, { status: providerResponse.status, headers });
  return { earlyError: null, response };
}

// Format output as SSE
export function formatSSE(data, sourceFormat) {
  if (data === null || data === undefined) return "data: null\n\n";
  if (data && data.done) return "data: [DONE]\n\n";

  // OpenAI Responses API format
  if (data && data.event && data.data) {
    const cleanedEventData = cleanUsagePayload(data.data);
    return `event: ${data.event}\ndata: ${JSON.stringify(cleanedEventData)}\n\n`;
  }

  data = cleanUsagePayload(data);

  // Claude format
  if (sourceFormat === FORMATS.CLAUDE && data && data.type) {
    return `event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n`;
  }

  return `data: ${JSON.stringify(data)}\n\n`;
}
