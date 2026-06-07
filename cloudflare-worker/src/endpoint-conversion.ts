import type { Env } from "./types";

const ENDPOINT_CONVERSION_KV_KEY = "config:endpoint_conversion_enabled";

export interface EndpointConversionConfig {
  enabled: boolean;
}

export async function loadEndpointConversionConfig(env: Env): Promise<EndpointConversionConfig> {
  const raw = (await env.MIMO_KV.get(ENDPOINT_CONVERSION_KV_KEY, "text")) || env.MIMO_ENDPOINT_CONVERSION_ENABLED || "";
  return { enabled: raw === "true" || raw === "1" || raw.toLowerCase() === "yes" };
}

export async function saveEndpointConversionEnabled(env: Env, enabled: boolean): Promise<void> {
  await env.MIMO_KV.put(ENDPOINT_CONVERSION_KV_KEY, enabled ? "true" : "false");
}

function generateId(prefix: string): string {
  const bytes = new Uint8Array(12);
  crypto.getRandomValues(bytes);
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `${prefix}_${hex}`;
}

function stringifyToolPayload(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === null || value === undefined) return "";
  return JSON.stringify(value);
}

function extractMessageContent(content: unknown): string | Array<Record<string, unknown>> {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return String(content ?? "");

  const parts: Array<Record<string, unknown>> = [];
  for (const part of content) {
    if (typeof part === "string") {
      parts.push({ type: "text", text: part });
      continue;
    }
    if (!part || typeof part !== "object") continue;
    const item = part as Record<string, any>;
    const type = String(item.type || "");
    if (type === "input_text" || type === "output_text" || type === "text") {
      parts.push({ type: "text", text: item.text || "" });
    } else if (type === "input_image" && (item.image_url || item.url)) {
      parts.push({ type: "image_url", image_url: { url: item.image_url || item.url } });
    } else if (type === "input_file") {
      parts.push({ type: "text", text: `[Attached File: ${item.filename || "unknown"}]` });
    } else {
      parts.push({ type: "text", text: JSON.stringify(item) });
    }
  }

  if (parts.length === 1 && parts[0].type === "text") return String(parts[0].text || "");
  return parts;
}

function mergeText(current: string | undefined, next: string | undefined): string | undefined {
  if (!next) return current;
  return current ? `${current}${next}` : next;
}

function normalizeRole(role: unknown): string {
  const value = String(role || "user");
  return ["system", "user", "assistant", "developer"].includes(value) ? value : "user";
}

function convertResponsesToolsToChat(tools: unknown): unknown {
  if (!Array.isArray(tools)) return tools;
  return tools
    .map((tool) => {
      if (!tool || typeof tool !== "object") return null;
      const t = tool as Record<string, any>;
      if (t.type !== "function") return null;
      if (t.function && typeof t.function === "object") return { type: "function", function: t.function };
      return {
        type: "function",
        function: {
          name: t.name || "function_call",
          description: t.description || "",
          parameters: t.parameters || {},
        },
      };
    })
    .filter(Boolean);
}

function convertResponsesToolChoiceToChat(toolChoice: unknown): unknown {
  if (!toolChoice || typeof toolChoice !== "object") return toolChoice;
  const tc = toolChoice as Record<string, any>;
  if (tc.type === "function" && tc.name && !tc.function) {
    return { type: "function", function: { name: tc.name } };
  }
  return toolChoice;
}

export function convertResponsesRequestToChat(req: Record<string, any>): Record<string, any> {
  const messages: Array<Record<string, any>> = [];

  if (req.instructions) {
    messages.push({ role: "system", content: String(req.instructions) });
  }

  const input = req.input;
  if (typeof input === "string" && input) {
    messages.push({ role: "user", content: input });
  } else if (Array.isArray(input)) {
    let pendingReasoning: string | undefined;

    for (const raw of input) {
      if (!raw || typeof raw !== "object") {
        messages.push({ role: "user", content: String(raw ?? "") });
        continue;
      }

      const item = raw as Record<string, any>;
      const itemType = item.type;

      if (!itemType && ("role" in item || "content" in item)) {
        messages.push({ role: normalizeRole(item.role), content: extractMessageContent(item.content || []) });
        continue;
      }

      if (itemType === "reasoning") {
        pendingReasoning = mergeText(pendingReasoning, item.reasoning_content || item.encrypted_content || "");
        continue;
      }

      if (itemType === "message") {
        const msg: Record<string, any> = { role: normalizeRole(item.role), content: extractMessageContent(item.content || []) };
        if (msg.role === "assistant" && pendingReasoning) {
          msg.reasoning_content = pendingReasoning;
          pendingReasoning = undefined;
        }
        messages.push(msg);
        continue;
      }

      if (itemType === "function_call" || itemType === "custom_tool_call") {
        const callId = item.call_id || item.id || generateId("call");
        const toolCall = {
          id: callId,
          type: "function",
          function: {
            name: item.name || (itemType === "custom_tool_call" ? "custom_tool_call" : "function_call"),
            arguments: stringifyToolPayload(item.arguments ?? item.input ?? item.content),
          },
        };
        const last = messages[messages.length - 1];
        if (last && last.role === "assistant") {
          if (pendingReasoning) {
            last.reasoning_content = mergeText(last.reasoning_content, pendingReasoning);
            pendingReasoning = undefined;
          }
          last.tool_calls = [...(last.tool_calls || []), toolCall];
        } else {
          const msg: Record<string, any> = { role: "assistant", content: null, tool_calls: [toolCall] };
          if (pendingReasoning) {
            msg.reasoning_content = pendingReasoning;
            pendingReasoning = undefined;
          }
          messages.push(msg);
        }
        continue;
      }

      if (itemType === "function_call_output" || itemType === "custom_tool_call_output") {
        messages.push({ role: "tool", tool_call_id: item.call_id || item.id || generateId("call"), content: stringifyToolPayload(item.output ?? item.content) });
      }
    }
  }

  const out: Record<string, any> = { ...req };
  if ("max_output_tokens" in out) {
    out.max_tokens = out.max_output_tokens;
  }

  for (const key of [
    "instructions",
    "input",
    "store",
    "previous_response_id",
    "max_output_tokens",
    "background",
    "include",
    "prompt",
    "reasoning",
    "text",
    "truncation",
  ]) {
    delete out[key];
  }

  if ("tools" in out) out.tools = convertResponsesToolsToChat(out.tools);
  if ("tool_choice" in out) out.tool_choice = convertResponsesToolChoiceToChat(out.tool_choice);
  out.messages = messages;
  if (!("stream" in out)) out.stream = false;
  return out;
}

function responseUsage(chatUsage: any): Record<string, number> | undefined {
  if (!chatUsage || typeof chatUsage !== "object") return undefined;
  return {
    input_tokens: Number(chatUsage.prompt_tokens || 0),
    output_tokens: Number(chatUsage.completion_tokens || 0),
    total_tokens: Number(chatUsage.total_tokens || 0),
  };
}

function makeReasoningItem(reasoning: string, status = "completed"): Record<string, any> {
  return { type: "reasoning", id: generateId("rs"), summary: [], encrypted_content: reasoning, status };
}

function makeMessageItem(content: Array<Record<string, unknown>>, status = "completed", id = generateId("msg")): Record<string, any> {
  return { type: "message", id, role: "assistant", content, status };
}

function makeFunctionCallItem(callId: string, name: string, args: string, id = generateId("fc")): Record<string, any> {
  return { type: "function_call", id, call_id: callId, name, arguments: args };
}

export function convertChatResponseToResponses(chatResp: Record<string, any>, modelFallback = ""): Record<string, any> {
  const choice = (chatResp.choices || [{}])[0] || {};
  const message = choice.message || {};
  const output: Array<Record<string, any>> = [];

  if (message.reasoning_content) output.push(makeReasoningItem(String(message.reasoning_content)));

  const contentParts: Array<Record<string, unknown>> = [];
  if (message.content) contentParts.push({ type: "output_text", text: String(message.content), annotations: [] });
  if (message.refusal) contentParts.push({ type: "refusal", refusal: String(message.refusal) });
  if (contentParts.length > 0) output.push(makeMessageItem(contentParts));

  for (const tc of message.tool_calls || []) {
    const fn = tc.function || {};
    output.push(makeFunctionCallItem(String(tc.id || generateId("call")), String(fn.name || ""), stringifyToolPayload(fn.arguments || "{}")));
  }

  return {
    id: generateId("resp"),
    object: "response",
    created_at: Math.floor(Date.now() / 1000),
    model: String(chatResp.model || modelFallback || ""),
    output,
    usage: responseUsage(chatResp.usage),
    status: "completed",
  };
}

function sseEvent(eventType: string, data: Record<string, any>): string {
  const payload = { ...data, type: eventType };
  return `event: ${eventType}\ndata: ${JSON.stringify(payload)}\n\n`;
}

class ResponsesStreamConverter {
  private readonly respId = generateId("resp");
  private readonly msgId = generateId("msg");
  private readonly createdAt = Math.floor(Date.now() / 1000);
  private nextOutputIndex = 0;
  private responseCreated = false;
  private contentDone = false;
  private completionEmitted = false;
  private usage: Record<string, number> | undefined;
  private reasoningIndex: number | undefined;
  private reasoningBuf = "";
  private reasoningClosed = false;
  private textIndex: number | undefined;
  private textBuf = "";
  private textClosed = false;
  private toolCalls = new Map<number, { outputIndex: number; item: Record<string, any> }>();

  constructor(private model = "") {}

  processSse(rawEvent: string): string[] {
    const dataLines = rawEvent
      .split(/\r?\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trim());
    if (dataLines.length === 0) return [];
    const data = dataLines.join("\n");
    if (!data) return [];
    if (data === "[DONE]") return this.handleDone();

    let chunk: any;
    try {
      chunk = JSON.parse(data);
    } catch {
      return [];
    }

    const out: string[] = [];
    if (chunk.model && !this.model) this.model = String(chunk.model);
    if (chunk.usage) {
      this.usage = responseUsage(chunk.usage);
      if (this.contentDone && !this.completionEmitted) out.push(...this.emitCompletion());
    }

    for (const choice of chunk.choices || []) {
      out.push(...this.handleDelta(choice.delta || {}));
      if (choice.finish_reason) out.push(...this.handleFinish(String(choice.finish_reason)));
    }
    return out;
  }

  finalize(): string[] {
    return this.handleDone();
  }

  private allocateIndex(): number {
    return this.nextOutputIndex++;
  }

  private baseResponse(status: string): Record<string, any> {
    return { id: this.respId, object: "response", created_at: this.createdAt, model: this.model, output: [], usage: this.usage, status };
  }

  private emitResponseCreated(): string[] {
    if (this.responseCreated) return [];
    this.responseCreated = true;
    return [sseEvent("response.created", { response: this.baseResponse("in_progress") })];
  }

  private ensureReasoningItemStarted(): string[] {
    const out = this.emitResponseCreated();
    if (this.reasoningIndex === undefined) {
      this.reasoningIndex = this.allocateIndex();
      out.push(sseEvent("response.output_item.added", { output_index: this.reasoningIndex, item: { type: "reasoning", id: generateId("rs"), summary: [], status: "in_progress" } }));
    }
    return out;
  }

  private closeReasoningItem(): string[] {
    if (this.reasoningIndex === undefined || this.reasoningClosed) return [];
    this.reasoningClosed = true;
    return [sseEvent("response.output_item.done", { output_index: this.reasoningIndex, item: makeReasoningItem(this.reasoningBuf) })];
  }

  private ensureTextItemStarted(): string[] {
    const out = this.emitResponseCreated();
    if (this.textIndex === undefined) {
      this.textIndex = this.allocateIndex();
      out.push(sseEvent("response.output_item.added", { output_index: this.textIndex, item: { type: "message", id: this.msgId, role: "assistant", status: "in_progress", content: [] } }));
      out.push(sseEvent("response.content_part.added", { output_index: this.textIndex, content_index: 0, part: { type: "output_text", text: "" } }));
    }
    return out;
  }

  private closeTextContent(): string[] {
    if (this.textIndex === undefined || this.textClosed) return [];
    this.textClosed = true;
    const part = { type: "output_text", text: this.textBuf, annotations: [] };
    return [
      sseEvent("response.content_part.done", { output_index: this.textIndex, content_index: 0, part }),
      sseEvent("response.output_item.done", { output_index: this.textIndex, item: makeMessageItem([part], "completed", this.msgId) }),
    ];
  }

  private handleDelta(delta: Record<string, any>): string[] {
    const out: string[] = [];
    if (delta.role) out.push(...this.emitResponseCreated());
    if (delta.reasoning_content) {
      out.push(...this.ensureReasoningItemStarted());
      this.reasoningBuf += String(delta.reasoning_content);
    }
    if (delta.content) {
      out.push(...this.closeReasoningItem());
      out.push(...this.ensureTextItemStarted());
      this.textBuf += String(delta.content);
      out.push(sseEvent("response.output_text.delta", { output_index: this.textIndex, content_index: 0, delta: String(delta.content) }));
    }
    for (const tc of delta.tool_calls || []) {
      out.push(...this.closeReasoningItem());
      out.push(...this.handleToolCallDelta(tc));
    }
    return out;
  }

  private handleToolCallDelta(tc: Record<string, any>): string[] {
    const out: string[] = [];
    const index = Number(tc.index || 0);
    const fn = tc.function || {};
    if (!this.toolCalls.has(index)) {
      out.push(...this.closeTextContent());
      out.push(...this.emitResponseCreated());
      const outputIndex = this.allocateIndex();
      const item = makeFunctionCallItem(String(tc.id || generateId("call")), "", "");
      this.toolCalls.set(index, { outputIndex, item });
      out.push(sseEvent("response.output_item.added", { output_index: outputIndex, item }));
    }
    const state = this.toolCalls.get(index)!;
    if (fn.name) state.item.name = String(fn.name);
    if (fn.arguments) {
      state.item.arguments += String(fn.arguments);
      out.push(sseEvent("response.function_call_arguments.delta", { output_index: state.outputIndex, delta: String(fn.arguments) }));
    }
    return out;
  }

  private handleFinish(reason: string): string[] {
    if (this.contentDone) return [];
    this.contentDone = true;
    const out = [...this.closeReasoningItem(), ...this.closeTextContent()];
    if (reason === "tool_calls") {
      for (const state of [...this.toolCalls.values()].sort((a, b) => a.outputIndex - b.outputIndex)) {
        out.push(sseEvent("response.function_call_arguments.done", { output_index: state.outputIndex, item: state.item }));
        out.push(sseEvent("response.output_item.done", { output_index: state.outputIndex, item: state.item }));
      }
    }
    return out;
  }

  private emitCompletion(): string[] {
    if (this.completionEmitted) return [];
    this.completionEmitted = true;
    const response = this.baseResponse("completed");
    if (this.reasoningIndex !== undefined) response.output.push(makeReasoningItem(this.reasoningBuf));
    if (this.textIndex !== undefined || this.toolCalls.size === 0) {
      response.output.push(makeMessageItem([{ type: "output_text", text: this.textBuf, annotations: [] }], "completed", this.msgId));
    }
    for (const state of [...this.toolCalls.values()].sort((a, b) => a.outputIndex - b.outputIndex)) {
      response.output.push(state.item);
    }
    return [sseEvent("response.completed", { response })];
  }

  private handleDone(): string[] {
    const out: string[] = [];
    if (!this.contentDone) out.push(...this.handleFinish("stop"));
    out.push(...this.emitCompletion());
    return out;
  }
}

function cloneHeadersWithoutLength(headers: Headers): Headers {
  const out = new Headers(headers);
  out.delete("content-length");
  return out;
}

export async function transformChatCompletionResponseToResponses(response: Response, modelFallback = ""): Promise<Response> {
  if (response.status >= 400) return response;
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("text/event-stream")) {
    if (!response.body) return response;
    const converter = new ResponsesStreamConverter(modelFallback);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const encoder = new TextEncoder();
    let buffer = "";

    const stream = new ReadableStream({
      async start(controller) {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            let match: RegExpExecArray | null;
            const sep = /\r?\n\r?\n/;
            while ((match = sep.exec(buffer))) {
              const rawEvent = buffer.slice(0, match.index);
              buffer = buffer.slice(match.index + match[0].length);
              for (const event of converter.processSse(rawEvent)) controller.enqueue(encoder.encode(event));
            }
          }
          buffer += decoder.decode();
          if (buffer.trim()) {
            for (const event of converter.processSse(buffer)) controller.enqueue(encoder.encode(event));
          }
          for (const event of converter.finalize()) controller.enqueue(encoder.encode(event));
        } finally {
          try { controller.close(); } catch {}
        }
      },
      cancel() {
        try { reader.cancel(); } catch {}
      },
    });

    const headers = cloneHeadersWithoutLength(response.headers);
    headers.set("content-type", "text/event-stream; charset=utf-8");
    headers.set("cache-control", "no-cache");
    return new Response(stream, { status: response.status, headers });
  }

  const text = await response.text();
  try {
    const converted = convertChatResponseToResponses(JSON.parse(text), modelFallback);
    const headers = cloneHeadersWithoutLength(response.headers);
    headers.set("content-type", "application/json");
    return new Response(JSON.stringify(converted), { status: response.status, headers });
  } catch {
    return new Response(text, { status: response.status, headers: cloneHeadersWithoutLength(response.headers) });
  }
}
