"""Yellowpages platform adapter for Hermes.

Polls ``GET {API_BASE_URL}/inbox`` every 5s. Each response item is a
``HumanMessage`` dict::

    {id, humanId, conversationId, body, createdAt, seenByAgent}

For each message we dispatch one ``MessageEvent`` (in parallel — one task
per message). When the agent produces a reply, ``send()`` posts to
``{API_BASE_URL}/message`` with ``{humanId, body, replyId}``. The API
"currently open" — no JWT enforcement — so sender identity is not derived
from a bearer token. ``replyId`` references the human message being
replied to; the API requires every agent message to carry one.

Deduplication is handled server-side via the ``seenByAgent`` flag — we
trust ``/inbox`` to only return unseen messages.
"""

import asyncio
import datetime
import logging
import os
from typing import Any, Dict, Optional

import aiohttp

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

# Hardcoded for now; promote to an env var if multiple environments
# (staging / prod) need to coexist.
API_BASE_URL = "https://kpjowqfgbvpmjzgvylbo.supabase.co/functions/v1/api"
POLL_INTERVAL_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 30


class YellowPagesAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig, **_: Any) -> None:
        super().__init__(config=config, platform=Platform("yellowpages"))
        self._token = os.getenv("YELLOWPAGES_TOKEN", "").strip()
        if not self._token:
            raise RuntimeError("YELLOWPAGES_TOKEN env var is required")
        self._session: Optional[aiohttp.ClientSession] = None
        self._poll_task: Optional[asyncio.Task] = None
        # conversationId (str) -> humanId (int). Populated from each
        # inbox poll so send() can resolve the humanId required by
        # POST /message without an extra round-trip.
        self._human_by_conversation: Dict[str, int] = {}
        # conversationId (str) -> id of the most recent human message
        # seen on /inbox. Used as the replyId fallback when the gateway
        # invokes send() without an explicit reply_to (e.g. autonomous
        # / cron pushes). Agent messages REQUIRE a replyId.
        self._last_human_msg_by_conversation: Dict[str, int] = {}

    @property
    def name(self) -> str:
        return "Yellowpages"

    async def connect(self) -> bool:
        # Bypass hermes DM pairing — YP's server is the identity provider,
        # so every humanId on /inbox is trusted. Without this, each new
        # humanId triggers a pairing-code chat the dev must approve via
        # `hermes pairing approve`. A `=false` in .env still wins via
        # setdefault.
        os.environ.setdefault("YELLOWPAGES_ALLOW_ALL_USERS", "true")
        # Suppress hermes' "📬 No home channel is set" notice, which fires on
        # every new session (run.py:7423) and would otherwise leak into each
        # human's chat with the agent — YP conversations are 1:1 per humanId,
        # so there is no shared home channel to set. A real value in .env
        # still wins via setdefault.
        os.environ.setdefault("YELLOWPAGES_HOME_CHANNEL", "disabled")
        self._session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
        )
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        logger.info(
            "Yellowpages: connected, polling %s every %ss",
            API_BASE_URL,
            POLL_INTERVAL_SECONDS,
        )
        return True

    async def disconnect(self) -> None:
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
            self._session = None
        self._mark_disconnected()

    async def _poll_loop(self) -> None:
        while True:
            # TODO: Remove hardcoded agentId
            try:
                async with self._session.get(f"{API_BASE_URL}/inbox?agentId=1") as resp:
                    resp.raise_for_status()
                    messages = await resp.json()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Yellowpages: poll failed: %s", e)
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            for m in messages:
                # Cache the conversation → (humanId, last message id)
                # mapping before dispatching, so a concurrent send()
                # can resolve both fields it needs.
                try:
                    conv_id = str(m["conversationId"])
                    self._human_by_conversation[conv_id] = int(m["humanId"])
                    self._last_human_msg_by_conversation[conv_id] = int(m["id"])
                except (KeyError, TypeError, ValueError):
                    logger.warning("Yellowpages: malformed inbox item: %r", m)
                    continue
                asyncio.create_task(self._safe_dispatch(m))

            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _safe_dispatch(self, m: dict) -> None:
        try:
            await self._dispatch_message(m)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Yellowpages: dispatch failed for message %r", m.get("id"))

    async def _dispatch_message(self, m: dict) -> None:
        if not self._message_handler:
            logger.debug("Yellowpages: handler not registered yet; dropping message %s", m.get("id"))
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

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if self._session is None:
            return SendResult(success=False, error="Not connected", retryable=True)
        conv_id = str(chat_id)
        human_id = self._human_by_conversation.get(conv_id)
        if human_id is None:
            return SendResult(
                success=False,
                error=f"Unknown conversationId {chat_id!r} — no humanId cached from /inbox",
                retryable=False,
            )
        # Agent messages require a replyId pointing at a human message.
        # Prefer the gateway-supplied reply_to; fall back to the most
        # recent human message in the conversation for autonomous sends.
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
            async with self._session.post(f"{API_BASE_URL}/message", json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json()
            return SendResult(success=True, message_id=str(data.get("id", "")))
        except Exception as e:
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
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
