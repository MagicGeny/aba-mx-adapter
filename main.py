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
    logger,
    normalize_phone_for_max,
)
from media_cache_manager import MediaCacheManager

# --- Configuration ---
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
QUEUE_SEND = "tasks.messages.send"
QUEUE_SEND_EXISTING = "tasks.messages.send_existing_chat"
QUEUE_POLL = "tasks.messages.poll_replies"
QUEUE_RESULTS = "tasks.messages.results_replies_queue"
QUEUE_NOTIFY = "tasks.messages.tenant_admin_notify"
_NOTIFICATION_PREFIX = "🔔 Получены новые сообщения:"

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
    messenger_type: Optional[str] = None
    use_chat_id: bool = False
    chat_id: Optional[str] = None
    contact_type: Optional[str] = None

class CallbackPayload(BaseModel):
    task_id: str
    status: str
    error_message: str = ""
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
    phone_number: str
    status: str
    reply_text: Optional[str] = None
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
    chat_id: Optional[str] = None
    use_chat_id: bool = False
    replies: List[ClientReplyInfo]

# --- Worker Logic ---

class MaxWorkerDaemon:
    def __init__(self):
        self.browser_manager = MaxBrowserManager(headless=False)
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
            }
            reply_text = event.get("reply_text")
            if isinstance(reply_text, str) and reply_text.strip():
                if reply_text.startswith(_NOTIFICATION_PREFIX):
                    continue
                payload["reply_text"] = reply_text
            await self.publish_payload(payload)
            logger.info(f"Forwarded WS event: status={status} chat_id={chat_id}")

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
                logger.info(f"Processing notify task for tenant: {task.tenant_phone} (tenant_id={task.tenant_id!r}, use_chat_id={task.use_chat_id}, chat_id={task.chat_id!r}) with {len(task.replies)} replies")

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
                logger.info(f"Processing send task: {task.task_id} for {task.phone} (attachment_url={task.attachment_url!r}, attachment_name={task.attachment_name!r})")

                phone = normalize_phone_for_max(task.phone)
                if not phone:
                    await self.send_callback(CallbackPayload(
                        task_id=task.task_id,
                        status="failed",
                        error_message="Invalid phone format"
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
                    await self.send_callback(CallbackPayload(
                        task_id=task.task_id,
                        status="failed",
                        error_message=f"Attachment download failed: {e}"
                    ))
                    return

                result = await self.browser_manager.send_message(
                    phone,
                    task.message_text,
                    attachment_path=attachment_path,
                    chat_id=task.chat_id,
                    use_chat_id=bool(task.use_chat_id and task.chat_id),
                )
                await self.send_callback(CallbackPayload(
                    task_id=task.task_id,
                    status=result.status_note,
                    error_message=result.error_message
                ))

                if result.status_note == "user_not_found_by_phone":
                    await self.publish_result(TargetResult(
                        target_id=task.task_id,
                        campaign_id=task.campaign_id,
                        phone_number=phone,
                        status="user_not_found_by_phone",
                        timestamp=datetime.utcnow().isoformat() + "Z",
                    ))
                    logger.info(f"Soft-fail USER_NOT_FOUND_BY_PHONE for target {task.task_id} phone={phone}")
                    return

                # Now, also publish to RESULTS_QUEUE with "sent" status!
                if result.sent_ok:
                    if not result.chat_id:
                        logger.error(f"Send succeeded but chat_id not captured for target {task.task_id} phone={phone}")
                    await self.publish_result(TargetResult(
                        target_id=task.task_id,
                        campaign_id=task.campaign_id,
                        phone_number=phone,
                        status="sent",
                        timestamp=datetime.utcnow().isoformat() + "Z",
                        chat_id=result.chat_id
                    ))

            except Exception as e:
                logger.exception("Error in process_send_task")
                # Try to extract a task_id so we can report a failure callback.
                try:
                    body = json.loads(raw_body)
                    task_id = body.get("task_id")
                except Exception:
                    task_id = None
                if task_id:
                    try:
                        await self.send_callback(CallbackPayload(
                            task_id=task_id,
                            status="failed",
                            error_message=f"worker exception: {e}",
                        ))
                    except Exception as cb_err:
                        logger.error(f"Failed to send failure callback: {cb_err}")

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
        logger.info("Starting Max Worker Daemon...")
        await self.browser_manager.start()
        
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            self.publish_connection = connection
            async with connection:
                channel = await connection.channel()
                self.publish_channel = channel
                await channel.set_qos(prefetch_count=1)

                send_queue = await channel.declare_queue(QUEUE_SEND, durable=True)
                existing_queue = await channel.declare_queue(QUEUE_SEND_EXISTING, durable=True)
                poll_queue = await channel.declare_queue(QUEUE_POLL, durable=True)
                results_queue = await channel.declare_queue(QUEUE_RESULTS, durable=True)
                notify_queue = await channel.declare_queue(QUEUE_NOTIFY, durable=True)

                logger.info(f"Waiting for messages on {QUEUE_SEND}, {QUEUE_SEND_EXISTING}, {QUEUE_POLL}, {QUEUE_NOTIFY}...")
                
                # Consume from all queues
                await send_queue.consume(self.process_send_task)
                await existing_queue.consume(self.process_send_task)
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
#just main
if __name__ == "__main__":
    daemon = MaxWorkerDaemon()
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, exiting...")
    except Exception as e:
        logger.exception("Unhandled exception in main")
