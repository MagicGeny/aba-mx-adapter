import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env", override=False)

try:
    from zoneinfo import ZoneInfo
    try:
        MSK_TZ = ZoneInfo("Europe/Moscow")
    except Exception:
        MSK_TZ = timezone(timedelta(hours=3), name="Europe/Moscow")
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore
    try:
        MSK_TZ = ZoneInfo("Europe/Moscow")
    except Exception:
        MSK_TZ = timezone(timedelta(hours=3), name="Europe/Moscow")


def to_moscow_time(dt: datetime) -> datetime:
    """Convert datetime to Europe/Moscow timezone.

    If dt is naive (no tzinfo), assume it's UTC (as our services always store/transmit UTC).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MSK_TZ)

import aio_pika
import httpx
from pydantic import BaseModel, Field
from max_playwright_sender import (
    MaxBrowserManager,
    _task_correlation,
    logger,
    normalize_phone_for_max,
)
from media_cache_manager import MediaCacheManager

# --- Configuration ---
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
# Authoritative sender-account identity (UUID from tenant_accounts.id).
TENANT_ACCOUNT_ID = (os.getenv("TENANT_ACCOUNT_ID") or os.getenv("ACCOUNT_ID") or "").strip()
RABBITMQ_SEND_EXCHANGE = (os.getenv("RABBITMQ_SEND_EXCHANGE") or "tasks.messages.direct").strip()
QUEUE_SEND = "tasks.messages.send"
QUEUE_SEND_EXISTING = "tasks.messages.send_existing_chat"
QUEUE_POLL = "tasks.messages.poll_replies"
QUEUE_RESULTS = "tasks.messages.results_replies_queue"
# Legacy shared admin-notification queue. Kept only for documentation: it is no
# longer declared or consumed, because one shared queue lets an unrelated MAX
# account consume (and send) another account's admin notification.
QUEUE_NOTIFY_LEGACY = "tasks.messages.tenant_admin_notify"
_NOTIFICATION_PREFIX = "🔔 Получены новые сообщения:"

ERROR_INVALID_PHONE = "INVALID_PHONE"
ERROR_USER_NOT_FOUND = "USER_NOT_FOUND_BY_PHONE"
ERROR_SESSION_EXPIRED = "SESSION_EXPIRED"
ERROR_WORKER_UNAVAILABLE = "WORKER_UNAVAILABLE"
ERROR_MAX_UI = "MAX_UI_ERROR"
ERROR_BROWSER = "BROWSER_ERROR"
ERROR_DELIVERY_UNKNOWN = "DELIVERY_UNKNOWN"
ERROR_ACCOUNT_MISMATCH = "ACCOUNT_MISMATCH"


def account_send_queue(tenant_account_id: str) -> str:
    return f"tasks.messages.send.account.{tenant_account_id}"


def account_routing_key(tenant_account_id: str) -> str:
    return f"account.{tenant_account_id}"


def account_notify_queue(tenant_account_id: str) -> str:
    """Per-account tenant-admin notification queue.

    Admin notifications are published over the default exchange with this queue
    name as the routing key, so only the owning account's worker can consume them.
    """
    return f"tasks.messages.tenant_admin_notify.account.{tenant_account_id}"


def notify_task_targets_account(task_account_id: Optional[str], worker_account_id: str) -> bool:
    """True only when a notification explicitly targets this worker's account.

    Admin notifications must be sent by the MAX account that observed the
    replies, so a missing or foreign ``tenant_account_id`` is rejected instead of
    being processed by whichever worker happens to consume the message.
    """
    task_account = (task_account_id or "").strip()
    worker_account = (worker_account_id or "").strip()
    if not task_account or not worker_account:
        return False
    return task_account == worker_account


def classify_send_error(status_note: str, error_message: str) -> str:
    note = (status_note or "").strip().lower()
    msg = (error_message or "").strip().lower()
    if note == "user_not_found_by_phone":
        return ERROR_USER_NOT_FOUND
    if "session" in msg or "auth" in msg or "login" in msg or "не авториз" in msg:
        return ERROR_SESSION_EXPIRED
    if "browser" in msg or "playwright" in msg or "target closed" in msg or "page closed" in msg:
        return ERROR_BROWSER
    if note == "failed" or note:
        return ERROR_MAX_UI
    return ERROR_MAX_UI


def resolve_user_data_dir(tenant_account_id: str) -> str:
    """Per-account Playwright profile. Avoid accidental sharing via a global MAX_USER_DATA_DIR."""
    explicit = (os.getenv("MAX_USER_DATA_DIR") or "").strip()
    if tenant_account_id:
        if explicit and tenant_account_id in explicit:
            return explicit
        return str(_PROJECT_ROOT / "user_data" / f"account_{tenant_account_id}")
    if explicit:
        return explicit
    return str(_PROJECT_ROOT / "user_data")


# --- Models ---
class SendTask(BaseModel):
    task_id: str
    campaign_id: str
    messenger: str
    phone: str
    message_text: str
    attachment_url: Optional[str] = None
    attachment_name: Optional[str] = None
    tenant_id: Optional[str] = None
    tenant_account_id: Optional[str] = None
    account_id: Optional[str] = None
    messenger_type: Optional[str] = None
    use_chat_id: bool = False
    chat_id: Optional[str] = None
    contact_type: Optional[str] = None

class CallbackPayload(BaseModel):
    task_id: str
    status: str
    error_code: str = ""
    error_message: str = ""
    account_id: str = ""
    sent_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")

class PollTarget(BaseModel):
    target_id: str
    phone_normalized: str

class PollTask(BaseModel):
    campaign_id: str
    targets: List[PollTarget]

class TargetResult(BaseModel):
    target_id: Optional[str] = None
    campaign_id: Optional[str] = None
    tenant_id: Optional[str] = None
    tenant_account_id: Optional[str] = None
    account_id: Optional[str] = None
    phone_number: str
    status: str
    reply_text: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    timestamp: str
    chat_id: Optional[str] = None
    messenger_type: Optional[str] = None

class ClientReplyInfo(BaseModel):
    user_phone: str
    user_name: str
    message: str
    time: datetime

class TenantAdminNotificationTask(BaseModel):
    tenant_phone: str
    tenant_id: Optional[str] = None
    # Sender account (tenant_accounts.id) that must send this notification.
    # Required for account isolation: the task must identify its worker.
    tenant_account_id: Optional[str] = None
    chat_id: Optional[str] = None
    use_chat_id: bool = False
    replies: List[ClientReplyInfo]

# --- Worker Logic ---

class MaxWorkerDaemon:
    def __init__(self):
        self.tenant_account_id = TENANT_ACCOUNT_ID
        if not self.tenant_account_id:
            raise RuntimeError(
                "TENANT_ACCOUNT_ID is required (UUID of tenant_accounts.id). "
                "Each worker process must bind to exactly one sender account."
            )
        user_data_dir = resolve_user_data_dir(self.tenant_account_id)
        # Ensure MaxBrowserManager (which prefers MAX_USER_DATA_DIR) uses this process's profile.
        os.environ["MAX_USER_DATA_DIR"] = user_data_dir
        logger.info(
            f"Worker TENANT_ACCOUNT_ID={self.tenant_account_id!r} "
            f"user_data_dir={user_data_dir!r} exchange={RABBITMQ_SEND_EXCHANGE!r}"
        )
        self.browser_manager = MaxBrowserManager(headless=False, user_data_dir=user_data_dir)
        self.http_client = httpx.AsyncClient(base_url=ORCHESTRATOR_URL, timeout=30.0)
        self.media_http_client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
        self.media_cache = MediaCacheManager(cache_dir="media_cache", http_client=self.media_http_client)
        self.publish_channel: Optional[aio_pika.Channel] = None
        self.publish_connection: Optional[aio_pika.Connection] = None
        self._ws_forwarder_task: Optional[asyncio.Task] = None

    async def send_callback(self, payload: CallbackPayload):
        try:
            logger.info(f"Sending callback for task {payload.task_id}: {payload.status}")
            resp = await self.http_client.post("/api/v1/workers/callback", json=payload.model_dump())
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send callback to orchestrator: {e}")

    async def publish_result(self, result: TargetResult):
        try:
            if not self.publish_channel:
                return
            
            body = result.model_dump_json(exclude_none=True).encode()
            await self.publish_channel.default_exchange.publish(
                aio_pika.Message(body=body, content_type="application/json"),
                routing_key=QUEUE_RESULTS
            )
            logger.info(f"Published result for target {result.target_id}")
        except Exception as e:
            logger.error(f"Failed to publish result: {e}")

    async def publish_payload(self, payload: dict):
        try:
            if not self.publish_channel:
                return
            body = json.dumps(payload, ensure_ascii=False).encode()
            await self.publish_channel.default_exchange.publish(
                aio_pika.Message(body=body, content_type="application/json"),
                routing_key=QUEUE_RESULTS
            )
        except Exception as e:
            logger.error(f"Failed to publish payload: {e}")

    async def forward_ws_events(self):
        while True:
            event = await self.browser_manager.next_ws_action()
            status = str(event.get("status") or "")
            chat_id = str(event.get("chat_id") or "")
            if not status or not chat_id:
                continue
            payload = {
                "status": status,
                "chat_id": chat_id,
                "timestamp": str(event.get("timestamp") or datetime.utcnow().isoformat() + "Z"),
                "tenant_account_id": self.tenant_account_id,
                "account_id": self.tenant_account_id,
            }
            reply_text = event.get("reply_text")
            if isinstance(reply_text, str) and reply_text.strip():
                if reply_text.startswith(_NOTIFICATION_PREFIX):
                    continue
                payload["reply_text"] = reply_text
            await self.publish_payload(payload)
            logger.info(f"Forwarded WS event: status={status} chat_id={chat_id} tenant_account_id={self.tenant_account_id}")

    def format_notification_message(self, task: TenantAdminNotificationTask) -> str:
        lines = ["🔔 Получены новые сообщения:"]
        for i, reply in enumerate(task.replies, start=1):
            moscow_time = to_moscow_time(reply.time)
            time_str = moscow_time.strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"\n{i}. 📱 {reply.user_phone} ({reply.user_name})")
            lines.append(f"   💬 Ответ: {reply.message}")
            lines.append(f"   🕒 Время: {time_str}")
        return "\n".join(lines)

    async def process_notify_task(self, message: aio_pika.IncomingMessage):
        try:
            async with message.process():
                body = json.loads(message.body.decode())
                task = TenantAdminNotificationTask(**body)
                logger.info(
                    f"Processing notify task for tenant: {task.tenant_phone} "
                    f"(tenant_id={task.tenant_id!r}, tenant_account_id={task.tenant_account_id!r}, "
                    f"use_chat_id={task.use_chat_id}, chat_id={task.chat_id!r}) with {len(task.replies)} replies"
                )

                # Account isolation: only the account that observed the replies may
                # send this notification. A missing or foreign tenant_account_id is
                # rejected instead of being sent by whichever worker is free.
                if not notify_task_targets_account(task.tenant_account_id, self.tenant_account_id):
                    logger.error(
                        f"Rejecting admin notify task for tenant_account_id={task.tenant_account_id!r}: "
                        f"worker TENANT_ACCOUNT_ID={self.tenant_account_id!r} (account isolation)"
                    )
                    return

                phone = normalize_phone_for_max(task.tenant_phone)
                if not phone:
                    logger.error(f"Invalid tenant phone format: {task.tenant_phone}")
                    return

                # Build message
                notification_text = self.format_notification_message(task)
                logger.info(f"Notification text:\n{notification_text}")

                # Send message (use_chat_id=True if the orchestrator already knows the admin's chat_id)
                use_chat_id = bool(task.use_chat_id and task.chat_id)
                await asyncio.sleep(random.uniform(2.0, 6.0))
                result = await self.browser_manager.send_message(
                    phone,
                    notification_text,
                    humanize=True,
                    chat_id=task.chat_id,
                    use_chat_id=use_chat_id,
                )
                if result.sent_ok:
                    logger.info(f"Notification sent successfully to {task.tenant_phone}")
                    # Notify the orchestrator about the chat_id so that it is saved upon the first success
                    # and subsequent notifications are sent by chat_id.
                    try:
                        await self.publish_result(TargetResult(
                            target_id=None,
                            campaign_id=None,
                            tenant_id=task.tenant_id or None,
                            # The sender account is required so the orchestrator can
                            # persist admin_chat_phone_mappings for THIS account.
                            tenant_account_id=self.tenant_account_id,
                            account_id=self.tenant_account_id,
                            phone_number=phone,
                            status="sent",
                            timestamp=datetime.utcnow().isoformat() + "Z",
                            chat_id=result.chat_id or task.chat_id or None,
                            messenger_type="MAX",
                        ))
                    except Exception as pub_err:
                        logger.error(f"Failed to publish admin notify result: {pub_err}")
                else:
                    logger.error(f"Failed to send notification: {result.error_message}")
        except Exception as e:
            logger.exception("Error in process_notify_task")
            # Don't requeue, just log for now (but in real life, maybe requeue with backoff)

    async def process_send_task(self, message: aio_pika.IncomingMessage):
        raw_body = message.body.decode("utf-8", errors="replace")
        logger.info(f"Received payload: {raw_body}")
        async with message.process(requeue=False):
            try:
                body = json.loads(raw_body)
                task = SendTask(**body)
                task_account = (task.tenant_account_id or task.account_id or "").strip()
                logger.info(
                    f"Processing send task: {task.task_id} for {task.phone} "
                    f"tenant_account_id={task_account!r} worker={self.tenant_account_id!r} "
                    f"(attachment_url={task.attachment_url!r}, attachment_name={task.attachment_name!r})"
                )

                # Preserve existing diagnostic TASK_RECEIVED / TASK_FINISHED correlation.
                with _task_correlation(task.task_id, self.tenant_account_id):
                    if task_account and task_account != self.tenant_account_id:
                        err_msg = (
                            f"account mismatch: task.tenant_account_id={task_account} "
                            f"worker={self.tenant_account_id}"
                        )
                        logger.error(err_msg)
                        await self.publish_result(TargetResult(
                            target_id=task.task_id,
                            campaign_id=task.campaign_id,
                            tenant_id=task.tenant_id,
                            tenant_account_id=self.tenant_account_id,
                            account_id=self.tenant_account_id,
                            phone_number=task.phone,
                            status="failed",
                            error_code=ERROR_ACCOUNT_MISMATCH,
                            error_message=err_msg,
                            timestamp=datetime.utcnow().isoformat() + "Z",
                        ))
                        return

                    phone = normalize_phone_for_max(task.phone)
                    if not phone:
                        await self.publish_result(TargetResult(
                            target_id=task.task_id,
                            campaign_id=task.campaign_id,
                            tenant_id=task.tenant_id,
                            tenant_account_id=self.tenant_account_id,
                            account_id=self.tenant_account_id,
                            phone_number=task.phone,
                            status="failed",
                            error_code=ERROR_INVALID_PHONE,
                            error_message="Invalid phone format",
                            timestamp=datetime.utcnow().isoformat() + "Z",
                        ))
                        # Legacy callback for non-retry final failures (no error_code race).
                        await self.send_callback(CallbackPayload(
                            task_id=task.task_id,
                            status="failed",
                            error_code=ERROR_INVALID_PHONE,
                            error_message="Invalid phone format",
                            account_id=self.tenant_account_id,
                        ))
                        return

                    attachment_path: Optional[str] = None
                    try:
                        attachment_path = await self.media_cache.ensure_campaign_media(
                            campaign_id=task.campaign_id,
                            attachment_url=task.attachment_url,
                            attachment_name=task.attachment_name,
                        )
                    except Exception as e:
                        err_msg = f"Attachment download failed: {e}"
                        await self.publish_result(TargetResult(
                            target_id=task.task_id,
                            campaign_id=task.campaign_id,
                            tenant_id=task.tenant_id,
                            tenant_account_id=self.tenant_account_id,
                            account_id=self.tenant_account_id,
                            phone_number=phone,
                            status="failed",
                            error_code=ERROR_WORKER_UNAVAILABLE,
                            error_message=err_msg,
                            timestamp=datetime.utcnow().isoformat() + "Z",
                        ))
                        return

                    result = await self.browser_manager.send_message(
                        phone,
                        task.message_text,
                        attachment_path=attachment_path,
                        chat_id=task.chat_id,
                        use_chat_id=bool(task.use_chat_id and task.chat_id),
                    )

                    if result.status_note == "user_not_found_by_phone":
                        await self.publish_result(TargetResult(
                            target_id=task.task_id,
                            campaign_id=task.campaign_id,
                            tenant_id=task.tenant_id,
                            tenant_account_id=self.tenant_account_id,
                            account_id=self.tenant_account_id,
                            phone_number=phone,
                            status="user_not_found_by_phone",
                            error_code=ERROR_USER_NOT_FOUND,
                            error_message=result.error_message or None,
                            timestamp=datetime.utcnow().isoformat() + "Z",
                        ))
                        await self.send_callback(CallbackPayload(
                            task_id=task.task_id,
                            status="user_not_found_by_phone",
                            error_code=ERROR_USER_NOT_FOUND,
                            error_message=result.error_message,
                            account_id=self.tenant_account_id,
                        ))
                        logger.info(f"Soft-fail USER_NOT_FOUND_BY_PHONE for target {task.task_id} phone={phone}")
                        return

                    if result.sent_ok:
                        if not result.chat_id:
                            logger.error(
                                f"Send succeeded but chat_id not captured for target {task.task_id} phone={phone}"
                            )
                        await self.publish_result(TargetResult(
                            target_id=task.task_id,
                            campaign_id=task.campaign_id,
                            tenant_id=task.tenant_id,
                            tenant_account_id=self.tenant_account_id,
                            account_id=self.tenant_account_id,
                            phone_number=phone,
                            status="sent",
                            timestamp=datetime.utcnow().isoformat() + "Z",
                            chat_id=result.chat_id,
                        ))
                        await self.send_callback(CallbackPayload(
                            task_id=task.task_id,
                            status=result.status_note,
                            account_id=self.tenant_account_id,
                            error_message=result.error_message,
                        ))
                        return

                    # Technical failure: ResultConsumer owns retry/cooldown (publish only).
                    err_code = classify_send_error(result.status_note, result.error_message)
                    await self.publish_result(TargetResult(
                        target_id=task.task_id,
                        campaign_id=task.campaign_id,
                        tenant_id=task.tenant_id,
                        tenant_account_id=self.tenant_account_id,
                        account_id=self.tenant_account_id,
                        phone_number=phone,
                        status="failed",
                        error_code=err_code,
                        error_message=result.error_message or None,
                        timestamp=datetime.utcnow().isoformat() + "Z",
                    ))

            except Exception as e:
                logger.exception("Error in process_send_task")
                # Ambiguous outcome — do not blind-retry across accounts.
                try:
                    body = json.loads(raw_body)
                    task_id = body.get("task_id")
                    campaign_id = body.get("campaign_id")
                    tenant_id = body.get("tenant_id")
                    phone = body.get("phone") or ""
                except Exception:
                    task_id = None
                    campaign_id = None
                    tenant_id = None
                    phone = ""
                if task_id:
                    err_msg = f"worker exception: {e}"
                    try:
                        await self.publish_result(TargetResult(
                            target_id=task_id,
                            campaign_id=campaign_id,
                            tenant_id=tenant_id,
                            tenant_account_id=self.tenant_account_id,
                            account_id=self.tenant_account_id,
                            phone_number=phone,
                            status="delivery_unknown",
                            error_code=ERROR_DELIVERY_UNKNOWN,
                            error_message=err_msg,
                            timestamp=datetime.utcnow().isoformat() + "Z",
                        ))
                    except Exception as pub_err:
                        logger.error(f"Failed to publish delivery_unknown result: {pub_err}")

    async def process_poll_task(self, message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                body = json.loads(message.body.decode())
                task = PollTask(**body)
                logger.info(f"Processing poll task for campaign: {task.campaign_id} with {len(task.targets)} targets")

                for i, target in enumerate(task.targets):
                    # Add jitter delay (1.1 - 3.2 seconds)
                    delay = random.uniform(1.1, 3.2)
                    if i > 0:  # Don't delay first target
                        logger.info(f"Waiting {delay:.2f}s before checking next target")
                        await asyncio.sleep(delay)

                    phone = normalize_phone_for_max(target.phone_normalized)
                    if not phone:
                        logger.warning(f"Invalid phone for target {target.target_id}, skipping")
                        continue

                    result = await self.browser_manager.check_reply(phone)

                    if result.check_ok and result.reply_value:
                        # Has a reply
                        if result.from_ws_cache:
                            # This reply was already detected and forwarded to the orchestrator
                            # via the WebSocket forwarder (forward_ws_events). Publishing a
                            # duplicate TargetResult here would cause the orchestrator to
                            # generate a second tenant_admin_notify task, resulting in
                            # duplicate notifications to the administrator.
                            logger.info(
                                f"Skipping duplicate TargetResult for {target.phone_normalized}: "
                                f"reply already reported via WebSocket"
                            )
                            continue

                        target_result = TargetResult(
                            target_id=target.target_id,
                            campaign_id=task.campaign_id,
                            phone_number=target.phone_normalized,
                            status="replied",
                            reply_text=result.reply_value,
                            timestamp=result.replied_at or datetime.utcnow().isoformat() + "Z"
                        )
                    elif result.is_viewed:
                        # Message is viewed
                        target_result = TargetResult(
                            target_id=target.target_id,
                            campaign_id=task.campaign_id,
                            phone_number=target.phone_normalized,
                            status="viewed",
                            timestamp=datetime.utcnow().isoformat() + "Z"
                        )
                    else:
                        # No reply yet, just delivered
                        target_result = TargetResult(
                            target_id=target.target_id,
                            campaign_id=task.campaign_id,
                            phone_number=target.phone_normalized,
                            status="delivered",
                            timestamp=datetime.utcnow().isoformat() + "Z"
                        )

                    await self.publish_result(target_result)

            except Exception as e:
                logger.exception("Error in process_poll_task")

    async def run(self):
        logger.info(
            f"Starting Max Worker Daemon TENANT_ACCOUNT_ID={self.tenant_account_id!r} "
            f"exchange={RABBITMQ_SEND_EXCHANGE!r}..."
        )
        await self.browser_manager.start()
        
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            self.publish_connection = connection
            async with connection:
                channel = await connection.channel()
                self.publish_channel = channel
                await channel.set_qos(prefetch_count=1)

                exchange = await channel.declare_exchange(
                    RABBITMQ_SEND_EXCHANGE,
                    aio_pika.ExchangeType.DIRECT,
                    durable=True,
                )
                send_queue_name = account_send_queue(self.tenant_account_id)
                routing_key = account_routing_key(self.tenant_account_id)
                send_queue = await channel.declare_queue(send_queue_name, durable=True)
                await send_queue.bind(exchange, routing_key=routing_key)

                poll_queue = await channel.declare_queue(QUEUE_POLL, durable=True)
                results_queue = await channel.declare_queue(QUEUE_RESULTS, durable=True)
                # Account-isolated admin notifications: this worker consumes ONLY its
                # own queue, so it can never send another account's notification.
                notify_queue_name = account_notify_queue(self.tenant_account_id)
                notify_queue = await channel.declare_queue(notify_queue_name, durable=True)

                logger.info(
                    f"Waiting for messages on {send_queue_name} "
                    f"(exchange={RABBITMQ_SEND_EXCHANGE} rk={routing_key}), "
                    f"{QUEUE_POLL}, {notify_queue_name}... "
                    f"(note: SEND and NOTIFY are account-isolated; poll remains shared)"
                )

                await send_queue.consume(self.process_send_task)
                await poll_queue.consume(self.process_poll_task)
                await notify_queue.consume(self.process_notify_task)

                self._ws_forwarder_task = asyncio.create_task(self.forward_ws_events())

                # Keep running
                await asyncio.Future()
        finally:
            if self._ws_forwarder_task:
                self._ws_forwarder_task.cancel()
                try:
                    await self._ws_forwarder_task
                except asyncio.CancelledError:
                    pass
                self._ws_forwarder_task = None
            await self.browser_manager.stop()
            await self.http_client.aclose()
            await self.media_http_client.aclose()
#just main
if __name__ == "__main__":
    daemon = MaxWorkerDaemon()
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, exiting...")
    except Exception as e:
        logger.exception("Unhandled exception in main")
