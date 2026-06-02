"""Yellowpages platform adapter for Hermes.

Receives human messages from Supabase Realtime private Broadcast events on
``agent:<agentId>``. A one-time ``GET /inbox`` drain runs after each
connect/reconnect to catch messages sent while the adapter was offline; there
is no periodic polling loop.

Each human message is dispatched as one ``MessageEvent``. When the agent
produces a reply, ``send()`` posts to ``{API_BASE_URL}/message`` with
``{humanId, body, replyId}``. Agent-side REST endpoints are bearer
authenticated and scoped to the agent identified by ``YELLOWPAGES_TOKEN``.
"""

import asyncio
import datetime
import json
import logging
import os
import random
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set
from urllib.parse import quote, urlparse, urlunparse

import aiohttp

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "https://kpjowqfgbvpmjzgvylbo.supabase.co/functions/v1/api"
SUPABASE_PUBLISHABLE_KEY = "sb_publishable_lvc5hVzQmNAIcA0OyojwpA_YiMhYrbw"
REQUEST_TIMEOUT_SECONDS = 30
REALTIME_HEARTBEAT_INTERVAL_SECONDS = 20.0
REALTIME_JOIN_TIMEOUT_SECONDS = 10.0
REALTIME_RECONNECT_INITIAL_SECONDS = 1.0
REALTIME_RECONNECT_MAX_SECONDS = 30.0
REALTIME_TOKEN_REFRESH_SKEW_SECONDS = 60.0
REALTIME_DEDUPE_MAX_MESSAGE_IDS = 1000
INBOX_BATCH_CONTINUE_THRESHOLD = 50
TYPING_REFRESH_INTERVAL_SECONDS = 2.0
TYPING_UNSUPPORTED_STATUSES = frozenset({404, 405, 501})
HUMAN_MESSAGE_EVENT = "human_message"

# ---------------------------------------------------------------------------
# Backend control-plane message filtering
#
# Yellowpages agents are exposed to *paying end users*. The only thing a
# consumer should ever see is the agent's in-persona reply. Hermes, however,
# emits a number of operator-facing "control-plane" messages over the same
# send() path — progress tickers, dangerous-command approval prompts, DM
# pairing codes, the "no home channel" notice. The adapter is the last choke
# point before the consumer, so we drop these here. Because this lives in the
# plugin, the guarantee holds for *every* Yellowpages agent regardless of how
# its deployment configures display/approvals.
#
# This is the enforced floor, NOT a substitute for configuring the source:
#   - display.platforms.yellowpages.tool_progress: off  (stop progress at source)
#   - approvals.mode: off / HERMES_YOLO_MODE / a sandboxed terminal backend
#     (so the agent never *blocks* on an approval the consumer can't answer)
# Without the latter, a suppressed approval prompt still leaves the agent
# thread waiting until the approval timeout — invisible to the consumer, but
# slow. Suppression keeps it out of the chat; the deploy config keeps it fast.
#
# Markers are intentionally structural (leading status glyph, /approve+/deny
# co-occurrence, distinctive phrases) rather than exact strings, so they
# survive hermes wording changes across versions.

# Leading glyphs hermes uses to prefix progress / inactivity status tickers.
_STATUS_PREFIX_GLYPHS = ("⏳", "⏱")  # ⏳ hourglass, ⏱ stopwatch

# Case-insensitive substrings that mark an operator-facing control message.
_BACKEND_MARKERS = (
    "requires approval",
    "potentially dangerous",
    "pairing code",
    "no home channel is set",
)

CONVERSATION_RESET_MESSAGE = "Conversation reset!"


def is_backend_chatter(content: str) -> bool:
    """Return True if *content* is a hermes control-plane message.

    Such messages (progress tickers, approval prompts, pairing codes, home
    channel notices) are operator-facing and must never reach a Yellowpages
    consumer — only the agent's in-persona reply should. Returns False for
    normal agent replies, which are forwarded unchanged.
    """
    if not content or not content.strip():
        return False
    if content.lstrip().startswith(_STATUS_PREFIX_GLYPHS):
        return True
    lowered = content.lower()
    if any(marker in lowered for marker in _BACKEND_MARKERS):
        return True
    # Dangerous-command approval prompt: both /approve and /deny instructions
    # appear together. An in-persona reply would never emit that pair.
    if "/approve" in lowered and "/deny" in lowered:
        return True
    return False


def is_session_reset_notice(content: str) -> bool:
    """Return True for hermes session-reset banners with backend metadata."""
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if not lines:
        return False

    first_line = lines[0].lower()
    if "session reset" not in first_line and "conversation reset" not in first_line:
        return False
    if "starting fresh" in first_line:
        return True

    lowered_lines = [line.lower() for line in lines[1:]]
    metadata_markers = ("model:", "provider:", "context:", "tip:")
    marker_hits = sum(
        1
        for line in lowered_lines
        if any(marker in line for marker in metadata_markers)
    )
    return marker_hits >= 2


def normalize_outgoing_content(content: str) -> Optional[str]:
    """Normalize or suppress backend-generated messages before delivery."""
    if is_session_reset_notice(content):
        return CONVERSATION_RESET_MESSAGE
    if is_backend_chatter(content):
        return None
    return content


class YellowPagesAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig, **_: Any) -> None:
        super().__init__(config=config, platform=Platform("yellowpages"))
        self._token = os.getenv("YELLOWPAGES_TOKEN", "").strip()
        if not self._token:
            raise RuntimeError("YELLOWPAGES_TOKEN env var is required")

        self._api_base_url = (
            os.getenv("YELLOWPAGES_API_URL", DEFAULT_API_BASE_URL).strip().rstrip("/")
        )
        self._supabase_url = _normalize_supabase_url(
            os.getenv("YELLOWPAGES_SUPABASE_URL", "").strip() or self._api_base_url
        )
        self._supabase_key = SUPABASE_PUBLISHABLE_KEY

        self._session: Optional[aiohttp.ClientSession] = None
        self._realtime_session: Optional[aiohttp.ClientSession] = None
        self._realtime_task: Optional[asyncio.Task] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._realtime_ref = 0
        self._realtime_join_ref: Optional[str] = None
        self._realtime_topic: Optional[str] = None
        # conversationId (str) -> humanId (int). Populated from Realtime and
        # catch-up messages so send() can resolve the humanId required by
        # POST /message without an extra round-trip.
        self._human_by_conversation: Dict[str, int] = {}
        # conversationId (str) -> id of the most recent human message seen.
        # Used as the replyId fallback when the gateway invokes send() without
        # an explicit reply_to. Agent messages REQUIRE a replyId.
        self._last_human_msg_by_conversation: Dict[str, int] = {}
        self._seen_message_ids: Deque[int] = deque()
        self._seen_message_id_set: Set[int] = set()
        # None = unprobed, True = supported, False = explicitly unsupported.
        self._typing_supported: Optional[bool] = None
        self._last_typing_request_at: Dict[str, float] = {}

    @property
    def name(self) -> str:
        return "Yellowpages"

    async def connect(self) -> bool:
        # Bypass hermes DM pairing - YP's server is the identity provider,
        # so every humanId from Yellowpages is trusted. A `=false` in .env
        # still wins via setdefault.
        os.environ.setdefault("YELLOWPAGES_ALLOW_ALL_USERS", "true")
        # Suppress hermes' "No home channel is set" notice, which fires on
        # every new session. YP conversations are 1:1 per humanId, so there is
        # no shared home channel to set. A real value in .env still wins.
        os.environ.setdefault("YELLOWPAGES_HOME_CHANNEL", "disabled")

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
        )
        self._realtime_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=None,
                sock_connect=REQUEST_TIMEOUT_SECONDS,
            ),
        )
        self._realtime_task = asyncio.create_task(self._realtime_loop())
        self._mark_connected()
        logger.info(
            "Yellowpages: connected to %s via Supabase Realtime",
            self._api_base_url,
        )
        return True

    async def disconnect(self) -> None:
        if self._realtime_task and not self._realtime_task.done():
            self._realtime_task.cancel()
            try:
                await self._realtime_task
            except asyncio.CancelledError:
                pass
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        if self._realtime_session:
            await self._realtime_session.close()
            self._realtime_session = None
        if self._session:
            await self._session.close()
            self._session = None
        self._mark_disconnected()

    async def _realtime_loop(self) -> None:
        backoff = REALTIME_RECONNECT_INITIAL_SECONDS
        while True:
            try:
                await self._run_realtime_once()
                backoff = REALTIME_RECONNECT_INITIAL_SECONDS
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Yellowpages: realtime connection failed: %s", e)

            sleep_for = backoff + random.uniform(0, min(backoff, 1.0))
            await asyncio.sleep(sleep_for)
            backoff = min(backoff * 2, REALTIME_RECONNECT_MAX_SECONDS)

    async def _run_realtime_once(self) -> None:
        if self._realtime_session is None:
            raise RuntimeError("Realtime session is not connected")

        token_info = await self._fetch_realtime_token()
        topic = str(token_info["topic"])
        access_token = str(token_info["token"])
        expires_at = int(token_info["expiresAt"])
        ws_url = self._realtime_ws_url()

        async with self._realtime_session.ws_connect(
            ws_url,
            heartbeat=None,
            autoping=True,
        ) as ws:
            self._ws = ws
            channel_topic = await self._join_realtime_channel(
                ws,
                topic,
                access_token,
            )
            self._realtime_topic = topic
            logger.info("Yellowpages: subscribed to realtime topic %s", topic)

            await self._drain_inbox_once()

            heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
            refresh_task = asyncio.create_task(
                self._refresh_realtime_auth_loop(ws, topic, expires_at)
            )
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_realtime_text(msg.data, channel_topic)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await self._handle_realtime_binary(msg.data, channel_topic)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError(f"WebSocket error: {ws.exception()}")
                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSING,
                    ):
                        break
            finally:
                for task in (heartbeat_task, refresh_task):
                    task.cancel()
                await asyncio.gather(heartbeat_task, refresh_task, return_exceptions=True)
                self._ws = None
                self._realtime_join_ref = None
                self._realtime_topic = None

        raise RuntimeError("Realtime WebSocket closed")

    async def _fetch_realtime_token(self) -> Dict[str, Any]:
        if self._session is None:
            raise RuntimeError("API session is not connected")

        async with self._session.get(
            f"{self._api_base_url}/agent/realtime-token",
            headers=self._auth_headers(),
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"realtime token request failed with status {resp.status}: {await resp.text()}"
                )
            data = await resp.json()

        if not isinstance(data, dict):
            raise RuntimeError("Realtime token response was not an object")
        if not data.get("token") or not data.get("topic") or not data.get("expiresAt"):
            raise RuntimeError("Realtime token response is missing required fields")
        return data

    async def _join_realtime_channel(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        topic: str,
        access_token: str,
    ) -> str:
        channel_topic = f"realtime:{topic}"
        join_ref = self._next_ref()
        self._realtime_join_ref = join_ref
        await ws.send_json(
            {
                "topic": channel_topic,
                "event": "phx_join",
                "payload": {
                    "config": {
                        "broadcast": {"ack": False, "self": False},
                        "presence": {"enabled": False},
                        "postgres_changes": [],
                        "private": True,
                    },
                    "access_token": access_token,
                },
                "ref": join_ref,
                "join_ref": join_ref,
            }
        )

        while True:
            msg = await ws.receive(timeout=REALTIME_JOIN_TIMEOUT_SECONDS)
            if msg.type != aiohttp.WSMsgType.TEXT:
                raise RuntimeError(f"Unexpected realtime join frame type {msg.type}")

            frame = self._decode_realtime_frame(msg.data)
            if frame.get("event") != "phx_reply" or frame.get("ref") != join_ref:
                continue

            payload = frame.get("payload")
            status = payload.get("status") if isinstance(payload, dict) else None
            if status == "ok":
                return channel_topic
            raise RuntimeError(f"Realtime join failed: {payload!r}")

    async def _heartbeat_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            await asyncio.sleep(REALTIME_HEARTBEAT_INTERVAL_SECONDS)
            await ws.send_json(
                {
                    "topic": "phoenix",
                    "event": "heartbeat",
                    "payload": {},
                    "ref": self._next_ref(),
                }
            )

    async def _refresh_realtime_auth_loop(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        topic: str,
        expires_at: int,
    ) -> None:
        channel_topic = f"realtime:{topic}"
        current_expires_at = expires_at

        while True:
            delay = max(
                30.0,
                (current_expires_at / 1000.0)
                - time.time()
                - REALTIME_TOKEN_REFRESH_SKEW_SECONDS,
            )
            await asyncio.sleep(delay)
            token_info = await self._fetch_realtime_token()
            if token_info.get("topic") != topic:
                raise RuntimeError("Realtime token topic changed during refresh")

            await ws.send_json(
                {
                    "topic": channel_topic,
                    "event": "access_token",
                    "payload": {"access_token": token_info["token"]},
                    "ref": self._next_ref(),
                    "join_ref": self._realtime_join_ref,
                }
            )
            current_expires_at = int(token_info["expiresAt"])

    async def _handle_realtime_text(self, data: str, channel_topic: str) -> None:
        frame = self._decode_realtime_frame(data)
        await self._handle_realtime_frame(frame, channel_topic)

    async def _handle_realtime_binary(self, data: bytes, channel_topic: str) -> None:
        frame = self._decode_realtime_binary_frame(data)
        if frame is None:
            logger.debug("Yellowpages: ignoring unsupported binary realtime frame")
            return
        await self._handle_realtime_frame(frame, channel_topic)

    async def _handle_realtime_frame(
        self,
        frame: Dict[str, Any],
        channel_topic: str,
    ) -> None:
        event = frame.get("event")
        topic = frame.get("topic")

        if topic != channel_topic and topic != "phoenix":
            logger.debug("Yellowpages: ignoring realtime frame for topic %r", topic)
            return

        if event == "broadcast":
            wrapper = frame.get("payload")
            if not isinstance(wrapper, dict):
                logger.warning("Yellowpages: malformed realtime broadcast: %r", wrapper)
                return
            if wrapper.get("event") != HUMAN_MESSAGE_EVENT:
                return
            await self._handle_human_message_broadcast(wrapper.get("payload"))
            return

        if event in {"phx_error", "phx_close"}:
            raise RuntimeError(f"Realtime channel closed with event {event}")

        if event == "phx_reply":
            payload = frame.get("payload")
            status = payload.get("status") if isinstance(payload, dict) else None
            if status == "error":
                raise RuntimeError(f"Realtime command failed: {payload!r}")

    async def _handle_human_message_broadcast(self, payload: Any) -> None:
        message = self._coerce_human_message_payload(payload)
        if message is None:
            return

        message_id = int(message["id"])
        if not self._remember_message_id(message_id):
            return

        self._cache_message_context(message)
        asyncio.create_task(self._safe_dispatch(message, acknowledge=True))

    def _coerce_human_message_payload(self, payload: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            logger.warning("Yellowpages: malformed human_message payload: %r", payload)
            return None
        message = payload.get("message")
        if not isinstance(message, dict):
            logger.warning("Yellowpages: malformed human_message payload: %r", payload)
            return None
        if message.get("authorType") not in (None, "human"):
            logger.debug("Yellowpages: ignoring non-human message payload: %r", message)
            return None

        try:
            return {
                "id": int(message["id"]),
                "humanId": int(message["humanId"]),
                "conversationId": int(message["conversationId"]),
                "body": str(message["body"]),
                "createdAt": int(message["createdAt"]),
                "seenByAgent": bool(message.get("seenByAgent", False)),
            }
        except (KeyError, TypeError, ValueError):
            logger.warning("Yellowpages: malformed human_message payload: %r", payload)
            return None

    async def _drain_inbox_once(self) -> None:
        while True:
            messages = await self._fetch_inbox_batch()
            if not messages:
                return

            for raw_message in messages:
                message = self._coerce_inbox_message(raw_message)
                if message is None:
                    continue
                if not self._remember_message_id(int(message["id"])):
                    continue
                self._cache_message_context(message)
                asyncio.create_task(self._safe_dispatch(message, acknowledge=False))

            if len(messages) < INBOX_BATCH_CONTINUE_THRESHOLD:
                return

    async def _fetch_inbox_batch(self) -> List[Any]:
        if self._session is None:
            raise RuntimeError("API session is not connected")

        async with self._session.get(
            f"{self._api_base_url}/inbox",
            headers=self._auth_headers(),
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"inbox catch-up failed with status {resp.status}: {await resp.text()}"
                )
            messages = await resp.json()

        if not isinstance(messages, list):
            raise RuntimeError("Inbox response was not an array")
        return messages

    def _coerce_inbox_message(self, message: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(message, dict):
            logger.warning("Yellowpages: malformed inbox item: %r", message)
            return None

        try:
            return {
                "id": int(message["id"]),
                "humanId": int(message["humanId"]),
                "conversationId": int(message["conversationId"]),
                "body": str(message["body"]),
                "createdAt": int(message["createdAt"]),
                "seenByAgent": bool(message.get("seenByAgent", False)),
            }
        except (KeyError, TypeError, ValueError):
            logger.warning("Yellowpages: malformed inbox item: %r", message)
            return None

    async def _safe_dispatch(self, m: dict, *, acknowledge: bool) -> None:
        try:
            await self._dispatch_message(m)
        except asyncio.CancelledError:
            raise
        except Exception:
            if acknowledge:
                self._forget_message_id(int(m["id"]))
            logger.exception("Yellowpages: dispatch failed for message %r", m.get("id"))
            return

        if acknowledge:
            try:
                await self._acknowledge_messages([int(m["id"])])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "Yellowpages: failed to acknowledge message %s: %s",
                    m.get("id"),
                    e,
                )

    async def _acknowledge_messages(self, message_ids: List[int]) -> None:
        if self._session is None:
            raise RuntimeError("API session is not connected")

        async with self._session.post(
            f"{self._api_base_url}/inbox/seen",
            headers=self._auth_headers(),
            json={"messageIds": message_ids},
        ) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"inbox acknowledgement failed with status {resp.status}: {await resp.text()}"
                )

    async def _dispatch_message(self, m: dict) -> None:
        if not self._message_handler:
            logger.debug(
                "Yellowpages: handler not registered yet; dropping message %s",
                m.get("id"),
            )
            return
        source = self.build_source(
            chat_id=str(m["conversationId"]),
            chat_name=str(m["conversationId"]),
            chat_type="dm",
            user_id=str(m["humanId"]),
            user_name=str(m["humanId"]),
        )
        event = MessageEvent(
            text=m["body"],
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(m["id"]),
            timestamp=datetime.datetime.fromtimestamp(m["createdAt"] / 1000),
        )
        await self.handle_message(event)

    def _cache_message_context(self, m: Dict[str, Any]) -> None:
        conv_id = str(m["conversationId"])
        self._human_by_conversation[conv_id] = int(m["humanId"])
        self._last_human_msg_by_conversation[conv_id] = int(m["id"])

    def _remember_message_id(self, message_id: int) -> bool:
        if message_id in self._seen_message_id_set:
            return False
        self._seen_message_id_set.add(message_id)
        self._seen_message_ids.append(message_id)
        while len(self._seen_message_ids) > REALTIME_DEDUPE_MAX_MESSAGE_IDS:
            expired = self._seen_message_ids.popleft()
            self._seen_message_id_set.discard(expired)
        return True

    def _forget_message_id(self, message_id: int) -> None:
        self._seen_message_id_set.discard(message_id)

    def _decode_realtime_frame(self, data: str) -> Dict[str, Any]:
        raw = json.loads(data)
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list) and len(raw) >= 5:
            return {
                "join_ref": raw[0],
                "ref": raw[1],
                "topic": raw[2],
                "event": raw[3],
                "payload": raw[4],
            }
        raise RuntimeError(f"Unexpected realtime frame: {raw!r}")

    def _decode_realtime_binary_frame(self, data: bytes) -> Optional[Dict[str, Any]]:
        # Server USER_BROADCAST frame:
        # type, topic_size, event_size, metadata_size, payload_encoding, ...
        if len(data) < 5 or data[0] != 0x04:
            return None

        topic_size = data[1]
        event_size = data[2]
        metadata_size = data[3]
        payload_encoding = data[4]
        offset = 5
        try:
            topic = data[offset : offset + topic_size].decode("utf-8")
            offset += topic_size
            event = data[offset : offset + event_size].decode("utf-8")
            offset += event_size
            offset += metadata_size
            payload_bytes = data[offset:]
            payload: Any
            if payload_encoding == 1:
                payload = json.loads(payload_bytes.decode("utf-8"))
            else:
                payload = payload_bytes
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            logger.warning("Yellowpages: malformed binary realtime broadcast")
            return None

        return {
            "topic": topic,
            "event": "broadcast",
            "payload": {
                "event": event,
                "payload": payload,
            },
        }

    def _next_ref(self) -> str:
        self._realtime_ref += 1
        return str(self._realtime_ref)

    def _auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    def _realtime_ws_url(self) -> str:
        parsed = urlparse(self._supabase_url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        base_path = parsed.path.rstrip("/")
        path = f"{base_path}/realtime/v1/websocket"
        query = f"apikey={quote(self._supabase_key)}&vsn=1.0.0"
        return urlunparse((scheme, parsed.netloc, path, "", query, ""))

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        # Normalize/drop hermes control-plane messages before they reach the
        # consumer. Report success on suppression so the gateway treats it as
        # delivered (no retry / no plain-text fallback) - we intentionally
        # consume it.
        normalized_content = normalize_outgoing_content(content)
        if normalized_content is None:
            logger.debug(
                "Yellowpages: suppressed backend control-plane message: %.120r",
                content,
            )
            return SendResult(success=True, message_id="")
        if normalized_content != content:
            logger.debug(
                "Yellowpages: normalized backend-generated message: %.120r",
                content,
            )
            content = normalized_content
        if self._session is None:
            return SendResult(success=False, error="Not connected", retryable=True)
        conv_id = str(chat_id)
        human_id = self._human_by_conversation.get(conv_id)
        if human_id is None:
            return SendResult(
                success=False,
                error=f"Unknown conversationId {chat_id!r} - no humanId cached from Realtime or catch-up",
                retryable=False,
            )
        # Agent messages require a replyId pointing at a human message.
        # Prefer the gateway-supplied reply_to; fall back to the most recent
        # human message in the conversation for autonomous sends.
        reply_id_raw = reply_to if reply_to is not None else self._last_human_msg_by_conversation.get(conv_id)
        if reply_id_raw is None:
            return SendResult(
                success=False,
                error=f"No replyId available for conversationId {chat_id!r}; agent messages must reference a human message",
                retryable=False,
            )
        try:
            reply_id = int(reply_id_raw)
        except (TypeError, ValueError):
            return SendResult(success=False, error=f"Invalid reply_to {reply_to!r}", retryable=False)
        payload = {"humanId": human_id, "body": content, "replyId": reply_id}
        try:
            async with self._session.post(
                f"{self._api_base_url}/message",
                headers=self._auth_headers(),
                json=payload,
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
            message = data.get("message") if isinstance(data, dict) else None
            message_id = ""
            if isinstance(message, dict) and message.get("id") is not None:
                message_id = str(message["id"])
            # Sending a message settles the front-end typing indicator, so the
            # next message in a multi-message turn needs a fresh "started" ping.
            # Clear the throttle timestamp so the throttle only coalesces pings
            # *within* a single message rather than swallowing the first ping of
            # the following message.
            self._last_typing_request_at.pop(conv_id, None)
            return SendResult(success=True, message_id=message_id)
        except Exception as e:
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        if self._session is None or self._typing_supported is False:
            return None

        try:
            conversation_id = int(chat_id)
        except (TypeError, ValueError):
            logger.debug("Yellowpages: skipping typing for invalid conversationId %r", chat_id)
            return None

        now = time.monotonic()
        last_request_at = self._last_typing_request_at.get(chat_id)
        if (
            last_request_at is not None and
            now - last_request_at < TYPING_REFRESH_INTERVAL_SECONDS
        ):
            return None
        self._last_typing_request_at[chat_id] = now

        try:
            async with self._session.post(
                f"{self._api_base_url}/typing",
                headers=self._auth_headers(),
                json={"conversationId": conversation_id},
            ) as resp:
                if 200 <= resp.status < 300:
                    self._typing_supported = True
                    return None
                if resp.status in TYPING_UNSUPPORTED_STATUSES:
                    self._typing_supported = False
                    logger.info(
                        "Yellowpages: typing endpoint unavailable (status %s); disabling typing indicators",
                        resp.status,
                    )
                    return None
                logger.debug(
                    "Yellowpages: typing request failed with status %s for conversation %s",
                    resp.status,
                    conversation_id,
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(
                "Yellowpages: typing request failed for conversation %s: %s",
                conversation_id,
                e,
            )
        return None

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return SendResult(success=False, error="image send not supported")

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": str(chat_id), "type": "dm", "chat_id": str(chat_id)}


def _normalize_supabase_url(value: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return trimmed
    if "://" not in trimmed:
        trimmed = f"https://{trimmed}"

    parsed = urlparse(trimmed)
    path = parsed.path.rstrip("/")
    suffix = "/functions/v1/api"
    if path.endswith(suffix):
        path = path[: -len(suffix)]
    return urlunparse((parsed.scheme, parsed.netloc, path.rstrip("/"), "", "", ""))
