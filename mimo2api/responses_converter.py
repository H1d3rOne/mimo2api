import json
import secrets
import time
from typing import Any, Iterator, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


def _generate_id(prefix: str = "resp") -> str:
    return f"{prefix}_{secrets.token_hex(12)}"

# Chat Completions 模型
class ChatFunctionDef(BaseModel):
    name: str
    arguments: str


class ChatToolCall(BaseModel):
    id: str
    type: Literal["function"] = "function"
    function: ChatFunctionDef


class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "tool", "developer"]
    content: Union[str, list[dict[str, Any]], None] = None
    name: Optional[str] = None
    tool_calls: Optional[list[ChatToolCall]] = None
    tool_call_id: Optional[str] = None
    reasoning_content: Optional[str] = None


class ChatRequest(BaseModel):
    model: str = ""
    messages: list[ChatMessage]
    stream: bool = True
    max_tokens: Optional[int] = None
    # 允许额外透传的参数 (temperature, top_p, tools 等) 自动保留
    model_config = ConfigDict(extra="allow")


# Responses API 模型
class RespReasoningItem(BaseModel):
    type: Literal["reasoning"] = "reasoning"
    id: str = Field(default_factory=lambda: _generate_id("rs"))
    summary: list[Any] = Field(default_factory=list)
    encrypted_content: Optional[str] = None
    reasoning_content: Optional[str] = None
    status: Optional[str] = None


class RespMessageItem(BaseModel):
    type: Literal["message"] = "message"
    id: str = Field(default_factory=lambda: _generate_id("msg"))
    role: Literal["system", "user", "assistant", "developer"] = "user"
    content: Union[str, list[Any]] = Field(default_factory=list)
    status: Optional[str] = None


class RespFunctionCallItem(BaseModel):
    type: Literal["function_call"] = "function_call"
    id: str = Field(default_factory=lambda: f"fc_{secrets.token_hex(12)}")
    call_id: str
    name: str
    arguments: str = ""


class RespFunctionOutputItem(BaseModel):
    type: Literal["function_call_output"] = "function_call_output"
    call_id: str
    output: str


RespItem = Union[RespReasoningItem, RespMessageItem, RespFunctionCallItem, RespFunctionOutputItem]


# ─── 请求转换 ───────────────────────────────────────────────
def _extract_message_content(content: Any) -> Union[str, list[dict[str, Any]]]:
    """提取 message content (保留复杂的防御逻辑)。"""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return "" if content is None else str(content)

    parts: list[dict[str, Any]] = []
    for part in content:
        if isinstance(part, str):
            parts.append({"type": "text", "text": part})
            continue
        if not isinstance(part, dict):
            continue

        ptype = part.get("type", "")
        if ptype in ("input_text", "output_text", "text"):
            parts.append({"type": "text", "text": part.get("text", "")})
        elif ptype == "input_image" and (url := part.get("image_url", part.get("url", ""))):
            parts.append({"type": "image_url", "image_url": {"url": url}})
        elif ptype == "input_file":
            parts.append(
                {"type": "text", "text": f"[Attached File: {part.get('filename', 'unknown')}]"})
        else:
            parts.append(
                {"type": "text", "text": json.dumps(part, ensure_ascii=False)})

    if len(parts) == 1 and parts[0].get("type") == "text":
        return parts[0]["text"]
    return parts


def _stringify_tool_payload(value: Any) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False)


def _merge_reasoning_content(current: Optional[str], new: Optional[str]) -> Optional[str]:
    if not new:
        return current
    if not current:
        return new
    return f"{current}{new}"


def _normalize_role(role: Any) -> str:
    value = str(role or "user")
    return value if value in {"system", "user", "assistant", "developer"} else "user"


def _extract_reasoning_content(item: RespReasoningItem) -> str:
    return item.reasoning_content or item.encrypted_content or ""


def _parse_response_input_item(raw_item: Any) -> RespItem | None:
    """宽容解析 Responses input 历史项，跳过 Chat Completions 无法表达的内部项。"""
    if not isinstance(raw_item, dict):
        return RespMessageItem(role="user", content=str(raw_item))

    item_type = raw_item.get("type")
    if not item_type and ("role" in raw_item or "content" in raw_item):
        return RespMessageItem(role=_normalize_role(raw_item.get("role")), content=raw_item.get("content", []))

    if item_type == "reasoning":
        item = dict(raw_item)
        if item.get("summary") is None:
            item["summary"] = []
        if item.get("encrypted_content") is None and item.get("reasoning_content") is not None:
            item["encrypted_content"] = item["reasoning_content"]
        return RespReasoningItem.model_validate(item)

    if item_type == "message":
        item = dict(raw_item)
        if item.get("content") is None:
            item["content"] = []
        item["role"] = _normalize_role(item.get("role"))
        return RespMessageItem.model_validate(item)

    if item_type == "function_call":
        item = dict(raw_item)
        item["call_id"] = item.get("call_id") or item.get("id") or _generate_id("call")
        item["name"] = item.get("name") or "function_call"
        item["arguments"] = _stringify_tool_payload(item.get("arguments"))
        return RespFunctionCallItem.model_validate(item)

    if item_type == "function_call_output":
        item = dict(raw_item)
        item["call_id"] = item.get("call_id") or item.get("id") or _generate_id("call")
        item["output"] = _stringify_tool_payload(item.get("output", item.get("content")))
        return RespFunctionOutputItem.model_validate(item)

    if item_type == "custom_tool_call":
        return RespFunctionCallItem(
            call_id=raw_item.get("call_id") or raw_item.get("id") or _generate_id("call"),
            name=raw_item.get("name") or "custom_tool_call",
            arguments=_stringify_tool_payload(
                raw_item.get("arguments", raw_item.get("input", raw_item.get("content")))
            ),
        )

    if item_type == "custom_tool_call_output":
        return RespFunctionOutputItem(
            call_id=raw_item.get("call_id") or raw_item.get("id") or _generate_id("call"),
            output=_stringify_tool_payload(raw_item.get("output", raw_item.get("content"))),
        )

    return None


def _convert_responses_tools_to_chat(tools: Any) -> Any:
    if not isinstance(tools, list):
        return tools

    converted: list[dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            continue
        if isinstance(tool.get("function"), dict):
            converted.append({"type": "function", "function": tool["function"]})
            continue
        converted.append({
            "type": "function",
            "function": {
                "name": tool.get("name") or "function_call",
                "description": tool.get("description") or "",
                "parameters": tool.get("parameters") or {},
            },
        })
    return converted


def _convert_responses_tool_choice_to_chat(tool_choice: Any) -> Any:
    if not isinstance(tool_choice, dict):
        return tool_choice
    if tool_choice.get("type") == "function" and tool_choice.get("name") and "function" not in tool_choice:
        return {"type": "function", "function": {"name": tool_choice["name"]}}
    return tool_choice


def convert_request(req: dict[str, Any]) -> dict[str, Any]:
    """将 Responses API 请求转换为 Chat Completions 请求。"""
    chat_messages: list[ChatMessage] = []

    if instructions := req.get("instructions"):
        chat_messages.append(ChatMessage(role="system", content=instructions))

    input_data = req.get("input", "")
    if isinstance(input_data, str) and input_data:
        chat_messages.append(ChatMessage(role="user", content=input_data))
    elif isinstance(input_data, list):
        items = [
            item for item in (_parse_response_input_item(raw_item) for raw_item in input_data)
            if item is not None
        ]
        pending_reasoning_content: Optional[str] = None

        for item in items:
            if isinstance(item, RespReasoningItem):
                pending_reasoning_content = _merge_reasoning_content(
                    pending_reasoning_content, _extract_reasoning_content(item)
                )
                continue

            if isinstance(item, RespMessageItem):
                chat_message = ChatMessage(
                    role=item.role,
                    content=_extract_message_content(item.content)
                )
                if item.role == "assistant" and pending_reasoning_content:
                    chat_message.reasoning_content = pending_reasoning_content
                    pending_reasoning_content = None
                chat_messages.append(chat_message)

            elif isinstance(item, RespFunctionCallItem):
                tc = ChatToolCall(id=item.call_id, function=ChatFunctionDef(
                    name=item.name, arguments=item.arguments))
                if chat_messages and chat_messages[-1].role == "assistant":
                    if pending_reasoning_content:
                        chat_messages[-1].reasoning_content = _merge_reasoning_content(
                            chat_messages[-1].reasoning_content, pending_reasoning_content
                        )
                        pending_reasoning_content = None
                    if chat_messages[-1].tool_calls is None:
                        chat_messages[-1].tool_calls = []
                    chat_messages[-1].tool_calls.append(tc)
                else:
                    chat_message = ChatMessage(role="assistant", tool_calls=[tc])
                    if pending_reasoning_content:
                        chat_message.reasoning_content = pending_reasoning_content
                        pending_reasoning_content = None
                    chat_messages.append(chat_message)

            elif isinstance(item, RespFunctionOutputItem):
                chat_messages.append(ChatMessage(
                    role="tool", tool_call_id=item.call_id, content=item.output))

    # 在副本上操作，不污染原始 req
    req = dict(req)
    if "max_output_tokens" in req:
        req["max_tokens"] = req.pop("max_output_tokens")

    # 移除 Responses API 专有字段，避免污染 Chat Completions 请求
    for key in (
        "instructions",
        "input",
        "store",
        "previous_response_id",
        "background",
        "include",
        "prompt",
        "reasoning",
        "text",
        "truncation",
    ):
        req.pop(key, None)

    if "tools" in req:
        req["tools"] = _convert_responses_tools_to_chat(req["tools"])

    if "tool_choice" in req:
        req["tool_choice"] = _convert_responses_tool_choice_to_chat(req["tool_choice"])

    if "stream" not in req:
        req["stream"] = False

    req_kwargs = {**req, "messages": chat_messages}
    chat_req = ChatRequest(**req_kwargs)

    return chat_req.model_dump(exclude_none=True)


# ─── 非流式响应转换 ──────────────────────────────────────────

class ResponseUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


class ResponsesAPIResponse(BaseModel):
    id: str = Field(default_factory=lambda: _generate_id("resp"))
    object: str = "response"
    created_at: int = Field(default_factory=lambda: int(time.time()))
    model: str
    output: list[RespItem] = Field(default_factory=list)
    usage: Optional[ResponseUsage] = None
    status: str = "completed"


def convert_response(chat_resp: dict[str, Any], model_fallback: str = "") -> dict[str, Any]:
    """将 Chat Completions 响应转换为 Responses API 响应。"""
    choice = (chat_resp.get("choices") or [{}])[0]
    message = choice.get("message", {})
    output_items: list[RespItem] = []

    if reasoning_content := message.get("reasoning_content"):
        output_items.append(RespReasoningItem(
            status="completed",
            summary=[],
            encrypted_content=reasoning_content
        ))

    content_parts = []
    if content := message.get("content"):
        content_parts.append(
            {"type": "output_text", "text": content, "annotations": []})
    if refusal := message.get("refusal"):
        content_parts.append({"type": "refusal", "refusal": refusal})

    if content_parts:
        output_items.append(RespMessageItem(
            role="assistant",
            status="completed",
            content=content_parts
        ))

    for tc in (message.get("tool_calls") or []):
        func = tc.get("function", {})
        output_items.append(RespFunctionCallItem(
            call_id=tc.get("id", ""),
            name=func.get("name", ""),
            arguments=func.get("arguments", "{}")
        ))

    chat_usage = chat_resp.get("usage", {})
    usage = ResponseUsage(
        input_tokens=chat_usage.get("prompt_tokens", 0),
        output_tokens=chat_usage.get("completion_tokens", 0),
        total_tokens=chat_usage.get("total_tokens", 0)
    ) if chat_usage else None

    resp = ResponsesAPIResponse(
        model=chat_resp.get("model") or model_fallback or "",
        output=output_items,
        usage=usage
    )
    return resp.model_dump(exclude_none=True)


# ─── 流式 SSE 转换 (生成器模式) ──────────────────────────

def _sse_event(event_type: str, data: Union[dict, BaseModel]) -> str:
    """格式化单条 SSE 事件，支持直接传入 Pydantic 模型。"""
    payload = data.model_dump(exclude_none=True) if isinstance(
        data, BaseModel) else data
    payload["type"] = event_type

    def _default(obj):
        if isinstance(obj, BaseModel):
            return obj.model_dump(exclude_none=True)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False, default=_default)}\n\n"


class ResponsesStreamConverter:
    """Chat Completions SSE -> Responses API SSE。"""

    def __init__(self, model: str = ""):
        self._resp_id = _generate_id("resp")
        self._msg_id = _generate_id("msg")
        self._model = model
        self._created_at = int(time.time())

        self._next_out_idx = 0
        self._reasoning_out_idx: Optional[int] = None
        self._reasoning_buf = ""
        self._reasoning_closed = False
        self._text_out_idx: Optional[int] = None
        self._text_buf = ""
        self._text_closed = False

        self._tool_calls: dict[int, dict[str, Any]] = {}

        self._response_created_emitted = False
        self._content_done = False
        self._completion_emitted = False
        self._usage: Optional[ResponseUsage] = None

    def _allocate_index(self) -> int:
        idx = self._next_out_idx
        self._next_out_idx += 1
        return idx

    def _base_response(self, status: str) -> ResponsesAPIResponse:
        return ResponsesAPIResponse(
            id=self._resp_id, model=self._model, created_at=self._created_at,
            status=status, usage=self._usage, output=[]
        )

    def process_chunk(self, chunk_text: str) -> list[str]:
        """兼容旧调用名：入参应是一段完整 SSE event 文本。"""
        return self.process_sse(chunk_text)

    def process_sse(self, raw_event: str) -> list[str]:
        return list(self._process_sse_iter(raw_event))

    def _process_sse_iter(self, raw_event: str) -> Iterator[str]:
        data_lines = [
            line[5:].strip()
            for line in raw_event.splitlines()
            if line.startswith("data:")
        ]
        if not data_lines:
            return

        data_str = "\n".join(data_lines).strip()
        if not data_str:
            return
        if data_str == "[DONE]":
            yield from self._handle_done()
            return

        try:
            chunk = json.loads(data_str)
        except json.JSONDecodeError:
            return

        if chunk.get("model") and not self._model:
            self._model = str(chunk["model"])

        if usage_data := chunk.get("usage"):
            self._usage = ResponseUsage(
                input_tokens=usage_data.get("prompt_tokens", 0),
                output_tokens=usage_data.get("completion_tokens", 0),
                total_tokens=usage_data.get("total_tokens", 0)
            )
            if self._content_done and not self._completion_emitted:
                yield from self._emit_completion()

        for choice in chunk.get("choices", []):
            yield from self._handle_delta(choice.get("delta", {}))
            if finish_reason := choice.get("finish_reason"):
                yield from self._handle_finish(finish_reason)

    def _handle_delta(self, delta: dict[str, Any]) -> Iterator[str]:
        if delta.get("role"):
            yield from self._emit_response_created()

        if reasoning_content := delta.get("reasoning_content"):
            yield from self._ensure_reasoning_item_started()
            self._reasoning_buf += str(reasoning_content)

        if content := delta.get("content"):
            yield from self._close_reasoning_item()
            yield from self._ensure_text_item_started()
            self._text_buf += str(content)
            yield _sse_event("response.output_text.delta", {"output_index": self._text_out_idx, "content_index": 0, "delta": str(content)})

        for tc in (delta.get("tool_calls") or []):
            yield from self._close_reasoning_item()
            yield from self._handle_tool_call_delta(tc)

    def _handle_tool_call_delta(self, tc: dict[str, Any]) -> Iterator[str]:
        tc_index = tc.get("index", 0)
        func = tc.get("function", {})

        if tc_index not in self._tool_calls:
            yield from self._close_text_content()
            yield from self._emit_response_created()

            out_idx = self._allocate_index()
            call_id = tc.get("id") or _generate_id("call")

            # 使用模型来生成结构，确保规范
            item_model = RespFunctionCallItem(call_id=call_id, name="")
            self._tool_calls[tc_index] = {
                "model": item_model, "output_index": out_idx}

            yield _sse_event("response.output_item.added", {"output_index": out_idx, "item": item_model})

        tc_data = self._tool_calls[tc_index]
        model: RespFunctionCallItem = tc_data["model"]

        if name := func.get("name"):
            model.name = str(name)
        if args := func.get("arguments"):
            model.arguments += str(args)
            yield _sse_event("response.function_call_arguments.delta", {"output_index": tc_data["output_index"], "delta": str(args)})

    def _emit_response_created(self) -> Iterator[str]:
        if not self._response_created_emitted:
            self._response_created_emitted = True
            yield _sse_event("response.created", {"response": self._base_response("in_progress")})

    def _ensure_text_item_started(self) -> Iterator[str]:
        yield from self._emit_response_created()
        if self._text_out_idx is None:
            self._text_out_idx = self._allocate_index()
            msg_item = RespMessageItem(
                id=self._msg_id, role="assistant", status="in_progress")
            yield _sse_event("response.output_item.added", {"output_index": self._text_out_idx, "item": msg_item})
            yield _sse_event("response.content_part.added", {"output_index": self._text_out_idx, "content_index": 0, "part": {"type": "output_text", "text": ""}})

    def _ensure_reasoning_item_started(self) -> Iterator[str]:
        yield from self._emit_response_created()
        if self._reasoning_out_idx is None:
            self._reasoning_out_idx = self._allocate_index()
            reasoning_item = RespReasoningItem(status="in_progress", summary=[])
            yield _sse_event("response.output_item.added", {"output_index": self._reasoning_out_idx, "item": reasoning_item})

    def _close_reasoning_item(self) -> Iterator[str]:
        if self._reasoning_out_idx is not None and not self._reasoning_closed:
            self._reasoning_closed = True
            reasoning_item = RespReasoningItem(
                status="completed",
                summary=[],
                encrypted_content=self._reasoning_buf,
            )
            yield _sse_event("response.output_item.done", {"output_index": self._reasoning_out_idx, "item": reasoning_item})

    def _close_text_content(self) -> Iterator[str]:
        if self._text_out_idx is not None and not self._text_closed:
            self._text_closed = True
            text_part = {"type": "output_text",
                         "text": self._text_buf, "annotations": []}
            yield _sse_event("response.content_part.done", {"output_index": self._text_out_idx, "content_index": 0, "part": text_part})
            msg_item = RespMessageItem(
                id=self._msg_id, role="assistant", status="completed", content=[text_part])
            yield _sse_event("response.output_item.done", {"output_index": self._text_out_idx, "item": msg_item})

    def _handle_finish(self, finish_reason: str) -> Iterator[str]:
        if self._content_done:
            return
        self._content_done = True
        yield from self._close_reasoning_item()
        yield from self._close_text_content()

        if finish_reason == "tool_calls":
            for idx in sorted(self._tool_calls.keys()):
                tc = self._tool_calls[idx]
                yield _sse_event("response.function_call_arguments.done", {"output_index": tc["output_index"], "item": tc["model"]})
                yield _sse_event("response.output_item.done", {"output_index": tc["output_index"], "item": tc["model"]})

    def _emit_completion(self) -> Iterator[str]:
        if self._completion_emitted:
            return
        self._completion_emitted = True

        resp = self._base_response("completed")
        if self._reasoning_out_idx is not None:
            resp.output.append(RespReasoningItem(
                status="completed",
                summary=[],
                encrypted_content=self._reasoning_buf,
            ))
        if self._text_out_idx is not None or not self._tool_calls:
            resp.output.append(RespMessageItem(id=self._msg_id, role="assistant", status="completed", content=[
                               {"type": "output_text", "text": self._text_buf, "annotations": []}]))

        for idx in sorted(self._tool_calls.keys()):
            resp.output.append(self._tool_calls[idx]["model"])

        yield _sse_event("response.completed", {"response": resp})

    def _handle_done(self) -> Iterator[str]:
        if not self._content_done:
            yield from self._handle_finish("stop")
        yield from self._emit_completion()

    def finalize(self) -> list[str]:
        return list(self._handle_done())
