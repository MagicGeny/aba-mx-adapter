import asyncio
import base64
from collections import OrderedDict
import importlib.metadata
import inspect
import json
import logging
import os
import random
import re
import struct
import sys
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from playwright.async_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

#version 1
# Structured JSON Logger setup
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)

logger = logging.getLogger("max_worker")
logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

logger.info(
    f"max_playwright_sender loaded pid={os.getpid()} ppid={os.getppid()} file={__file__} cwd={os.getcwd()} python={sys.executable} argv={sys.argv}"
)

DEFAULT_USER_DATA_DIR = "./user_data"
DEFAULT_BASE_URL = "https://web.max.ru"
DEFAULT_RESTART_INTERVAL_SECONDS = 60 * 60
ENV_MAX_USER_DATA_DIR = "MAX_USER_DATA_DIR"
ENV_MAX_RESTART_INTERVAL_SECONDS = "MAX_BROWSER_RESTART_INTERVAL_SECONDS"
_NOTIFICATION_PREFIX = "🔔 Получены новые сообщения:"
_PHONE_CHAT_CACHE_MAX_SIZE = 5000
_DIAG_DUMP_MAX_FILES = 20
_DIAG_DUMP_MAX_AGE = timedelta(hours=24)

class MaxMessengerError(RuntimeError):
    """Base error for Max messenger automation."""

class ContactNotFoundError(MaxMessengerError):
    """Raised when the phone number cannot be found."""

@dataclass(frozen=True)
class SendMaxMessageResult:
    sent_ok: bool
    status_note: str
    error_message: str = ""
    chat_id: Optional[str] = None

@dataclass(frozen=True)
class CheckMaxMessageResult:
    reply_value: str
    check_ok: bool = True
    replied_at: Optional[str] = None
    is_viewed: bool = False
    from_ws_cache: bool = False

# --- Selectors ---
_SEARCH_PLUS_BUTTON_SELECTORS = ["button:has(use[href='#icon_plus'])"]
_FIND_BY_NUMBER_ITEM_MENU_SELECTORS = ["[role='menuitem']:has-text('Найти по номеру')", "[role='menuitem']:has-text('Search by number')"]
_CONTACT_NUMBER_INPUT_SELECTORS = ["form[id='findContact'] input"]
_FIND_CONTACT_SUBMIT_SELECTORS = [
    "button[type='submit'][form='findContact']",
    "form#findContact ~ * button[type='submit']",
    "form#findContact button[type='submit']",
]
_MESSAGE_INPUT_SELECTORS = [
    "div[contenteditable='true'][role='textbox']",
    "div[contenteditable=''][role='textbox']",
    "div[contenteditable][role='textbox']",
    "div[contenteditable='true'][data-testid*='composer']",
    "div[contenteditable][data-testid*='composer']",
    "textarea[placeholder*='Сообщение']",
    "textarea[placeholder*='Message']",
    "span[placeholder*='Message']",
    "span[placeholder*='Сообщение']",
    "div[placeholder*='Сообщение']",
    "div[placeholder*='Message']",
    "div[aria-placeholder*='Сообщение']",
    "div[aria-placeholder*='Message']",
    "div[data-lexical-editor='true']",
]
_ATTACH_BUTTON_SELECTORS = [
    "button:has(use[href*='paperclip'])",
    "button:has(use[href*='attach'])",
    "button[aria-label*='Прикреп']",
    "button[aria-label*='Attach']",
]
_ATTACH_MENU_MEDIA_SELECTORS = [
    "[role='menuitem']:has-text('Фото или видео')",
    "[role='menuitem']:has-text('Фото и видео')",
    "[role='menuitem']:has-text('Фото/Видео')",
    "[role='menuitem']:has-text('Фото')",
    "[role='menuitem']:has-text('Photo or video')",
    "[role='menuitem']:has-text('Photo & video')",
    "[role='menuitem']:has-text('Photo/Video')",
    "[role='menuitem']:has-text('Photo')",
]
_ATTACH_MENU_FILE_SELECTORS = [
    "[role='menuitem']:has-text('Файл')",
    "[role='menuitem']:has-text('Документ')",
    "[role='menuitem']:has-text('File')",
    "[role='menuitem']:has-text('Document')",
]
_ATTACHMENT_PREVIEW_SELECTORS = [
    "[class*='attachment']",
    "[data-testid*='attachment']",
    "a[href][download]",
    "img[src^='blob:']",
    "img[src^='data:']",
]

_ATTACHMENT_PREVIEW_READY_JS = """
() => {
  const root =
    document.querySelector('.openedChat') ||
    document.querySelector('[class*="openedChat"]') ||
    document.body;
  if (!root) return false;
  const imgs = [...root.querySelectorAll('img')];
  for (const img of imgs) {
    const src = img.getAttribute('src') || '';
    const ready = img.complete && img.naturalWidth > 0;
    if (!ready) continue;
    if (src.startsWith('blob:') || src.startsWith('data:')) return true;
    const inPreview = img.closest('[class*="attach"], [class*="Attach"], [class*="preview"], [class*="composer"], [class*="messageInput"]');
    if (inPreview) return true;
  }
  return !!(
    root.querySelector('[class*="attachment"]') ||
    root.querySelector('[data-testid*="attachment"]') ||
    root.querySelector('a[href][download]')
  );
}
"""

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg", ".heic", ".heif", ".tiff", ".tif"}
_SAFE_INVISIBLE_CHARS = [
    "\u200B",  # Zero-Width Space
    "\u200C",  # Zero-Width Non-Joiner
    "\u200E",  # Left-to-Right Mark
    "\u200F",  # Right-to-Left Mark
]


def _is_image_file(path: Path) -> bool:
    ext = path.suffix.lower()
    return ext in _IMAGE_EXTENSIONS


def _randomize_text_with_invisible_chars(text: str) -> str:
    """Добавляет к тексту суффикс из 3–21 рандомных безопасных невидимых символов."""
    if not text:
        return text

    suffix_len = random.randint(3, 21)
    suffix = "".join(
        random.choice(_SAFE_INVISIBLE_CHARS) for _ in range(suffix_len)
    )

    return text + suffix

#"div[placeholder*='Messagne'
_OPENED_CHAT_SELECTORS = [".openedChat", "[class*='openedChat']"]
_CHAT_HISTORY_SELECTORS = [".openedChat .history", "[class*='openedChat'] [class*='history']"]
_BACK_BUTTON_SELECTORS = ["button.backBtn", "button:has(use[href='#icon_arrow_left'])"]
# Real Max UI (EN example): <dialog data-testid="modal" open> with
# header "Number +7920… not found" and action "Find another number".
# RU copy uses "Не нашли номер" / "Найти другой номер". Do not wait on
# outdated "Пользователь не найден" strings — they never appear and used
# to burn 2.5s × N selectors before the chat-input wait loop.
_USER_NOT_FOUND_SELECTORS = [
    'dialog[data-testid="modal"][open]',
    "dialog[open] #modalHeaderTitle",
    "text=Не нашли номер",
    "text=Найти другой номер",
    "text=Find another number",
    "text=Пользователь не найден",
    "text=User not found",
    "text=No users found",
]

_USER_NOT_FOUND_MODAL_JS = """
() => {
  const dialog = document.querySelector('dialog[data-testid="modal"][open], dialog.container[open], dialog[open]');
  if (!dialog) return false;
  const text = (dialog.innerText || '').replace(/\\s+/g, ' ');
  return /not found|не нашли номер|найти другой номер|find another number|пользователь не найден|отправьте ссылку|invite link/i.test(text);
}
"""

_PARSE_LAST_MESSAGE_JS = """
() => {
  const chat = document.querySelector('.openedChat') || document.querySelector('[class*="openedChat"]');
  if (!chat) return { error: 'no_chat' };
  const history = chat.querySelector('.history') || chat.querySelector('[class*="history"]');
  if (!history) return { error: 'no_history' };
  const wrappers = [...history.querySelectorAll('div[class*="messageWrapper"]')];
  if (!wrappers.length) return { error: 'no_messages' };
  const last = wrappers[wrappers.length - 1];
  const isOut = (last.className || '').includes('messageWrapper--isOut');
  const bubbleRoot = last.querySelector('[data-bubbles-variant]');
  const variant = bubbleRoot ? bubbleRoot.getAttribute('data-bubbles-variant') : null;
  const statusUse = last.querySelector('.indicators use');
  const statusIcon = statusUse ? (statusUse.getAttribute('href') || '') : '';
  const textEl = last.querySelector('.bubble span.text') || last.querySelector('.bubble .text');
  let text = '';
  if (textEl) {
    text = (textEl.textContent || '').replace(/\\s+/g, ' ').trim();
  }
  // Try to find time
  const timeEl = last.querySelector('.time') || last.querySelector('[class*="time"]');
  const timeText = timeEl ? timeEl.textContent : null;
  
  return { isOut, variant, statusIcon, text, timeText };
}
"""

class MaxBrowserManager:
    def __init__(
        self,
        headless: bool = True,
        max_tasks: int = 50,
        restart_interval_seconds: int = DEFAULT_RESTART_INTERVAL_SECONDS,
        base_url: str = DEFAULT_BASE_URL,
        user_data_dir: str = DEFAULT_USER_DATA_DIR,
    ):
        self.headless = headless
        self.max_tasks = max_tasks
        self.restart_interval_seconds = restart_interval_seconds
        self.base_url = base_url
        self.user_data_dir = (os.getenv(ENV_MAX_USER_DATA_DIR) or user_data_dir).strip() or DEFAULT_USER_DATA_DIR
        self.browser_channel = (os.getenv("BROWSER_CHANNEL") or "chrome").strip()
        restart_from_env = (os.getenv(ENV_MAX_RESTART_INTERVAL_SECONDS) or "").strip()
        if restart_from_env.isdigit():
            self.restart_interval_seconds = int(restart_from_env)
        self.tasks_count = 0
        self.playwright = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self._op_lock = asyncio.Lock()
        self._startup_lock = asyncio.Lock()
        self._startup_count = 0
        self._restart_task: Optional[asyncio.Task] = None
        self._ws_actions: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._viewer_id: Optional[int] = None
        self._current_phone_for_mapping: Optional[str] = None
        self._chat_by_phone: "OrderedDict[str, str]" = OrderedDict()
        self._phone_by_chat_id: "OrderedDict[str, str]" = OrderedDict()
        self._chat_status: Dict[str, Dict[str, Any]] = {}

    def _startup_caller_tag(self) -> str:
        try:
            frame = inspect.stack()[2]
            return f"{os.path.basename(frame.filename)}:{frame.lineno}:{frame.function}"
        except Exception:
            return "unknown"

    def _env_diag(self) -> Dict[str, Optional[str]]:
        keys = [
            "DISPLAY",
            "PWDEBUG",
            "DEBUG",
            "PLAYWRIGHT_BROWSERS_PATH",
            "CHROME_USER_DATA_DIR",
            "CHROME_CONFIG_HOME",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_CACHE_HOME",
            "HOME",
            "TMPDIR",
            "TMP",
            "TEMP",
        ]
        return {k: os.getenv(k) for k in keys}

    def _playwright_diag(self) -> Dict[str, str]:
        try:
            return {"playwright": importlib.metadata.version("playwright")}
        except Exception:
            return {"playwright": "unknown"}

    async def start(self):
        caller = self._startup_caller_tag()
        pid = os.getpid()
        async with self._startup_lock:
            self._startup_count += 1
            startup_n = self._startup_count
            logger.info(
                f"startup_enter n={startup_n} pid={pid} caller={caller} has_context={bool(self.context)} has_page={bool(self.page)} channel={self.browser_channel!r} headless={self.headless} user_data_dir={self.user_data_dir!r} env={self._env_diag()} pw={self._playwright_diag()}"
            )
            if not self.context:
                logger.info(f"startup_context_begin n={startup_n} pid={pid} caller={caller}")
                await self._start_persistent_context(startup_n=startup_n, caller=caller)
                self.tasks_count = 0
                logger.info(f"startup_context_ok n={startup_n} pid={pid} caller={caller}")
            if not self.page:
                logger.info(f"startup_page_begin n={startup_n} pid={pid} caller={caller}")
                await self._bootstrap_page(startup_n=startup_n, caller=caller)
                logger.info(f"startup_page_ok n={startup_n} pid={pid} caller={caller}")
            if not self._restart_task:
                self._restart_task = asyncio.create_task(self._scheduled_restart_loop())
                logger.info(f"startup_restart_loop_started n={startup_n} pid={pid} caller={caller}")
            logger.info(
                f"startup_exit n={startup_n} pid={pid} caller={caller} has_context={bool(self.context)} has_page={bool(self.page)}"
            )

    async def stop(self):
        if self._restart_task:
            self._restart_task.cancel()
            try:
                await self._restart_task
            except asyncio.CancelledError:
                pass
            self._restart_task = None
        await self._close_page()
        await self._close_context()

    async def _start_persistent_context(self, startup_n: int, caller: str):
        try:
            user_data_path = Path(self.user_data_dir)
            user_data_path.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"context_begin n={startup_n} pid={os.getpid()} caller={caller} mode=launch_persistent_context channel={self.browser_channel!r} headless={self.headless} user_data_dir={str(user_data_path)!r} has_playwright={bool(self.playwright)} has_context={bool(self.context)}"
            )
            self.playwright = await async_playwright().start()
            chromium = self.playwright.chromium
            try:
                executable_path = getattr(chromium, "executable_path", None)
            except Exception:
                executable_path = None
            logger.info(
                f"context_playwright_started n={startup_n} pid={os.getpid()} caller={caller} chromium_executable_path={executable_path!r}"
            )
            self.context = await chromium.launch_persistent_context(
                user_data_dir=str(user_data_path),
                headless=self.headless,
                channel=self.browser_channel,
                ignore_default_args=["--enable-automation"],
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--test-type",  # Подавляет системные предупреждения Chrome (включая --no-sandbox)
                    "--no-sandbox",
                    "--disable-infobars",
                    "--silent-debugger-extension-api",
                ],
            )
            # Hide Playwright / automation fingerprint before any page scripts run.
            await self.context.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', {
                  get: () => undefined,
                });
                // Also neutralize other common automation markers if present.
                try {
                  window.chrome = window.chrome || { runtime: {} };
                } catch (_) {}
                """
            )
            self.context.set_default_timeout(30000)
            self.context.set_default_navigation_timeout(45000)
            await self._block_heavy_resources(self.context)
            logger.info(
                f"context_ok n={startup_n} pid={os.getpid()} caller={caller}"
            )
        except Exception as e:
            logger.error(
                f"context_failed n={startup_n} pid={os.getpid()} caller={caller} err={type(e).__name__}: {e} stack={''.join(traceback.format_exception(type(e), e, e.__traceback__))}"
            )
            await self._close_context()
            raise

    async def _close_context(self):
        if self.context:
            logger.info("Closing Chromium persistent context...")
            try:
                await self.context.close()
            except Exception:
                pass
            self.context = None
        if self.playwright:
            try:
                await self.playwright.stop()
            except Exception:
                pass
            self.playwright = None

    async def _scheduled_restart_loop(self):
        try:
            while True:
                await asyncio.sleep(self.restart_interval_seconds)
                async with self._op_lock:
                    logger.info("Scheduled browser restart triggered")
                    await self._restart_browser("scheduled")
        except asyncio.CancelledError:
            return

    async def _refresh_viewer_id(self, page: Page) -> None:
        try:
            viewer_id = await page.evaluate(
                """() => {
                  try {
                    const raw = localStorage.getItem('__oneme_auth');
                    if (!raw) return null;
                    const obj = JSON.parse(raw);
                    const v = obj && (obj.viewerId ?? obj.viewer_id);
                    if (v === null || v === undefined) return null;
                    const n = Number(v);
                    return Number.isFinite(n) ? n : null;
                  } catch (_) {
                    return null;
                  }
                }"""
            )
            if viewer_id is not None:
                self._viewer_id = int(viewer_id)
        except Exception:
            return

    async def _restart_browser(self, reason: str):
        caller = f"restart:{reason}"
        pid = os.getpid()
        async with self._startup_lock:
            self._startup_count += 1
            startup_n = self._startup_count
            logger.info(
                f"restart_enter n={startup_n} pid={pid} reason={reason} has_context={bool(self.context)} has_page={bool(self.page)}"
            )
            self._chat_status.clear()
            await self._close_page()
            await self._close_context()
            await self._start_persistent_context(startup_n=startup_n, caller=caller)
            self.tasks_count = 0
            await self._bootstrap_page(startup_n=startup_n, caller=caller)
            logger.info(
                f"restart_exit n={startup_n} pid={pid} reason={reason} has_context={bool(self.context)} has_page={bool(self.page)}"
            )

    async def _close_page(self):
        if self.page:
            try:
                await self.page.close()
            except Exception:
                pass
            self.page = None

    async def _ensure_page(self) -> Page:
        if self.page and getattr(self.page, "is_closed", None) and self.page.is_closed():
            self.page = None
        if not self.context or not self.page:
            await self.start()
        if not self.page:
            raise MaxMessengerError("Page is not available")
        return self.page

    async def _bootstrap_page(self, startup_n: int, caller: str):
        if not self.context:
            raise MaxMessengerError("Page bootstrap requested but context is not running")
        logger.info(
            f"page_begin n={startup_n} pid={os.getpid()} caller={caller} base_url={self.base_url!r}"
        )
        sniffer_path = Path(__file__).with_name("max_ws_sniffer.js")
        if sniffer_path.exists():
            await self.context.add_init_script(path=str(sniffer_path))
        pages = self.context.pages
        page = pages[0] if pages else await self.context.new_page()
        try:
            await page.expose_function("__max_ws_sniffer_emit", self._on_ws_sniffer_emit)
        except Exception:
            pass
        if not page.url or page.url == "about:blank":
            await page.goto(self.base_url, wait_until="domcontentloaded", timeout=45000)
        self.page = page
        await self._refresh_viewer_id(page)
        logger.info(
            f"page_ok n={startup_n} pid={os.getpid()} caller={caller}"
        )

    async def _on_ws_sniffer_emit(self, data: Dict[str, Any]):
        try:
            if not isinstance(data, dict):
                return
            if data.get("type") != "frame":
                return
            opcode = int(data.get("opcode") or 0)
            direction = str(data.get("dir") or "")
            if opcode == 1:
                return
            if opcode != 2:
                return
            payload_b64 = data.get("payload")
            if not isinstance(payload_b64, str):
                return
            raw = base64.b64decode(payload_b64)
            decoded = _decode_max_binary_frame(raw)
            if not decoded:
                return
            payload = decoded.get("payload")
            max_opcode = decoded.get("opcode")
            if max_opcode in (128, 130, 50):
                await self._handle_max_protocol_event(direction, decoded)
        except Exception as e:
            logger.error(f"WS sniffer emit handler failed: {e}")

    async def _push_ws_action(self, event: Dict[str, Any]):
        try:
            self._ws_actions.put_nowait(event)
        except asyncio.QueueFull:
            try:
                _ = self._ws_actions.get_nowait()
            except Exception:
                return
            try:
                self._ws_actions.put_nowait(event)
            except Exception:
                return

    async def _handle_max_protocol_event(self, direction: str, decoded: Dict[str, Any]):
        opcode = decoded.get("opcode")
        payload = decoded.get("payload")
        if not isinstance(payload, dict):
            return

        if opcode == 128:
            chat_id = payload.get("chatId")
            message = payload.get("message")
            if chat_id is None or not isinstance(message, dict):
                return
            chat_id_str = str(chat_id)

            msg_text = _extract_message_text(message)
            msg_time_iso = _extract_message_time_iso(message)
            is_out = _extract_message_is_out(message, viewer_id=self._viewer_id)
            reply_to = message.get("replyTo")

            st = self._chat_status.setdefault(chat_id_str, {})
            if not is_out:
                if isinstance(msg_text, str) and msg_text.startswith(_NOTIFICATION_PREFIX):
                    return
                prev_text = st.get("last_incoming_text")
                prev_time = st.get("last_incoming_time")
                if prev_text == msg_text and prev_time == msg_time_iso:
                    return
                st["last_incoming_text"] = msg_text
                st["last_incoming_time"] = msg_time_iso
                st["last_incoming_is_reply"] = reply_to is not None
                phone = self._phone_by_chat_id.get(chat_id_str)
                logger.info(
                    json.dumps(
                        {
                            "event": "max_ws_incoming_message",
                            "phone": phone,
                            "chatId": chat_id_str,
                            "text": msg_text,
                            "isReply": reply_to is not None,
                            "time": msg_time_iso,
                        },
                        ensure_ascii=False,
                    )
                )
                await self._push_ws_action(
                    {
                        "status": "replied",
                        "chat_id": chat_id_str,
                        "reply_text": msg_text,
                        "timestamp": msg_time_iso or datetime.utcnow().isoformat() + "Z",
                    }
                )
            else:
                st["last_outgoing_text"] = msg_text
                st["last_outgoing_time"] = msg_time_iso
                phone_for_mapping = self._current_phone_for_mapping
                if phone_for_mapping:
                    self._cache_phone_chat(phone=phone_for_mapping, chat_id=chat_id_str)
        elif opcode == 130:
            chat_id = payload.get("chatId")
            user_id = payload.get("userId")
            if chat_id is None or user_id is None:
                return
            chat_id_str = str(chat_id)
            try:
                user_id_int = int(user_id)
            except Exception:
                user_id_int = None
            if self._viewer_id is not None and user_id_int == self._viewer_id:
                return
            st = self._chat_status.setdefault(chat_id_str, {})
            mark = payload.get("mark")
            try:
                mark_int = int(mark) if mark is not None else None
            except Exception:
                mark_int = None
            prev_mark = st.get("last_view_mark")
            try:
                prev_mark_int = int(prev_mark) if prev_mark is not None else None
            except Exception:
                prev_mark_int = None
            if mark_int is not None and prev_mark_int is not None and mark_int <= prev_mark_int:
                return
            if mark_int is not None:
                st["last_view_mark"] = mark_int
            st["is_viewed"] = True
            st["viewed_at"] = datetime.utcnow().isoformat() + "Z"
            phone = self._phone_by_chat_id.get(chat_id_str)
            logger.info(
                json.dumps(
                    {
                        "event": "max_ws_read_receipt",
                        "phone": phone,
                        "chatId": chat_id_str,
                        "userId": user_id,
                        "mark": payload.get("mark"),
                        "unread": payload.get("unread"),
                        "ts": st["viewed_at"],
                    },
                    ensure_ascii=False,
                )
            )
            await self._push_ws_action(
                {
                    "status": "viewed",
                    "chat_id": chat_id_str,
                    "timestamp": st["viewed_at"],
                }
            )

    async def _block_heavy_resources(self, context: BrowserContext) -> None:
        blocked_resource_types = {"media", "font"}
        blocked_extensions = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".webm"}
        allowed_image_hosts = {"web.max.ru", "st.max.ru"}

        async def route_handler(route):
            request = route.request
            url = request.url.lower()
            # Allow inline assets (emojis, sprites) served from MAX CDN
            try:
                host = request.url.split("//", 1)[1].split("/", 1)[0].split(":", 1)[0]
                if request.resource_type == "image" and host in allowed_image_hosts:
                    await route.continue_()
                    return
            except Exception:
                pass
            if request.resource_type in blocked_resource_types:
                await route.abort()
                return
            if request.resource_type == "image":
                # Still block third-party images (ads, tracking, heavy avatars from outside MAX)
                await route.abort()
                return
            if any(ext in url for ext in blocked_extensions):
                await route.abort()
                return
            await route.continue_()

        await context.route("**/*", route_handler)

    def _pw_selector(self, selector: str) -> str:
        if selector.startswith("//") or selector.startswith(".//"):
            return "xpath=" + selector
        return selector

    async def _wait_and_get_first(self, page: Page, selectors: Iterable[str], timeout_ms: int = 7000) -> Optional[str]:
        """Wait until ANY selector matches, using a single shared timeout.

        Trae's sequential remaining-ms rewrite was wrong: the first *non-matching*
        selector consumed the entire budget, so later real selectors (e.g.
        contenteditable='') never ran and healthy chats failed. Race waits instead.
        """
        sels = [s for s in selectors if s]
        if not sels:
            return None
        for sel in sels:
            try:
                handle = await page.query_selector(self._pw_selector(sel))
                if handle:
                    return sel
            except Exception:
                continue

        found: asyncio.Future = asyncio.get_running_loop().create_future()

        async def wait_one(sel: str) -> None:
            try:
                await page.wait_for_selector(self._pw_selector(sel), timeout=timeout_ms)
                if not found.done():
                    found.set_result(sel)
            except Exception:
                return

        tasks = [asyncio.create_task(wait_one(sel)) for sel in sels]
        try:
            return await asyncio.wait_for(asyncio.shield(found), timeout=timeout_ms / 1000.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return None
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _human_pause(self, min_ms: int = 200, max_ms: int = 500) -> None:
        await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000.0)

    async def _is_user_not_found_modal(self, page: Page) -> bool:
        try:
            return bool(await page.evaluate(_USER_NOT_FOUND_MODAL_JS))
        except Exception:
            return False

    async def _dismiss_user_not_found_overlay(self, page: Page) -> None:
        """Leave the not-found dialog without clicking its action buttons.

        The open <dialog> intercepts pointer events, so one Plus click is absorbed.
        A second Plus click (same control used to search a new cold number) unblocks
        the UI for the next queued recipient.
        """
        plus = page.locator(_SEARCH_PLUS_BUTTON_SELECTORS[0]).first
        try:
            await plus.click(timeout=250)
        except Exception:
            pass
        await asyncio.sleep(0.12)
        try:
            await plus.click(timeout=500, force=True)
        except Exception:
            pass
        await asyncio.sleep(0.12)
        try:
            still_open = await self._is_user_not_found_modal(page)
        except Exception:
            still_open = False
        if still_open:
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass

    async def _wait_for_chat_or_not_found(self, page: Page, timeout_ms: int = 4000) -> str:
        """Return 'chat', 'not_found', or 'timeout' within timeout_ms total."""
        deadline = asyncio.get_event_loop().time() + timeout_ms / 1000.0
        chat_loc = page.locator(".openedChat, [class*='openedChat']")
        while asyncio.get_event_loop().time() < deadline:
            if await self._is_user_not_found_modal(page):
                return "not_found"
            try:
                if await chat_loc.count() > 0:
                    return "chat"
            except Exception:
                pass
            for sel in _MESSAGE_INPUT_SELECTORS[:4]:
                try:
                    handle = await page.query_selector(self._pw_selector(sel))
                    if handle:
                        return "chat"
                except Exception:
                    continue
            await asyncio.sleep(0.1)
        if await self._is_user_not_found_modal(page):
            return "not_found"
        return "timeout"

    async def _diag_snapshot(self, page: Page, prefix: str) -> None:
        """Best-effort diagnostic snapshot for debugging selector wait failures."""
        try:
            url = page.url
            title = await page.title()
            buttons = await page.locator("button").count()
            uses = await page.locator("use").count()
            hrefs = await page.evaluate(
                "() => [...new Set([...document.querySelectorAll('use')].map(u => u.getAttribute('href') || u.getAttribute('xlink:href') || ''))].slice(0, 20)"
            )
            ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            screenshot_path = f"/tmp/diag_{prefix}_{ts}.png"
            html_path = f"/tmp/diag_{prefix}_{ts}.html"
            try:
                await page.screenshot(path=screenshot_path, full_page=True)
            except Exception as se:
                screenshot_path = f"<screenshot-failed:{se}>"
            try:
                Path(html_path).write_text(await page.content(), encoding="utf-8")
            except Exception as he:
                html_path = f"<html-failed:{he}>"
            logger.error(
                f"[DIAG:{prefix}] url={url} title={title!r} buttons={buttons} uses={uses} hrefs={hrefs} "
                f"screenshot={screenshot_path} html={html_path}"
            )
            self._cleanup_diag_dumps(prefix=prefix)
        except Exception as e:
            logger.exception(f"[DIAG:{prefix}] snapshot collection failed: {e}")

    def _cleanup_diag_dumps(self, prefix: str) -> None:
        try:
            now = datetime.utcnow()
            candidates = list(Path("/tmp").glob("diag_*.*"))
            keep: List[Path] = []
            for p in candidates:
                try:
                    mtime = datetime.utcfromtimestamp(p.stat().st_mtime)
                except Exception:
                    continue
                if now - mtime <= _DIAG_DUMP_MAX_AGE:
                    keep.append(p)
            keep.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            to_delete = keep[_DIAG_DUMP_MAX_FILES:]
            old_to_delete = [p for p in candidates if p not in keep]
            for p in to_delete + old_to_delete:
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception:
            return

    async def _open_chat_by_phone(self, page: Page, phone: str) -> bool:
        logger.info(f"Opening chat for phone: {phone}")
        if await self._is_user_not_found_modal(page):
            await self._dismiss_user_not_found_overlay(page)

        # Return to chat list if needed
        if await page.locator(".openedChat, [class*='openedChat']").count() > 0:
            back_selector = await self._wait_and_get_first(page, _BACK_BUTTON_SELECTORS, timeout_ms=2000)
            if back_selector:
                await page.click(back_selector, timeout=3000)
                await self._wait_and_get_first(page, _SEARCH_PLUS_BUTTON_SELECTORS, timeout_ms=2000)

        open_selector = await self._wait_and_get_first(page, _SEARCH_PLUS_BUTTON_SELECTORS, timeout_ms=10000)
        if not open_selector:
            await self._diag_snapshot(page, "plus_button_missing")
            raise MaxMessengerError("Button 'Начать общение' not found.")
        await self._human_pause(1000, 4000)
        await page.click(open_selector, timeout=5000)
        await self._human_pause(2000, 5000)

        find_by_number_selector = await self._wait_and_get_first(page, _FIND_BY_NUMBER_ITEM_MENU_SELECTORS, timeout_ms=5000)
        if not find_by_number_selector:
            raise MaxMessengerError("Menu item 'Найти по номеру' not found.")
        await page.click(find_by_number_selector, timeout=4000)

        phone_input_selector = await self._wait_and_get_first(page, _CONTACT_NUMBER_INPUT_SELECTORS, timeout_ms=8000)
        if not phone_input_selector:
            raise MaxMessengerError("Phone input not found.")

        await page.fill(phone_input_selector, phone)
        await self._human_pause(2000, 5000)

        submit_selector = await self._wait_and_get_first(page, _FIND_CONTACT_SUBMIT_SELECTORS, timeout_ms=8000)
        if not submit_selector:
            raise MaxMessengerError("Submit button 'Найти в MAX' not found.")
        await page.click(submit_selector, timeout=4000)
        await self._human_pause(1200, 28000)

        outcome = await self._wait_for_chat_or_not_found(page, timeout_ms=4000)
        if outcome == "not_found":
            logger.info(f"User-not-found modal detected for phone={phone}")
            await self._dismiss_user_not_found_overlay(page)
            raise ContactNotFoundError(f"User not found by phone: {phone}")
        if outcome == "chat":
            return True

        # Fallback: modal copy may differ; still do not burn 17×10s selector waits.
        if await self._wait_and_get_first(page, _USER_NOT_FOUND_SELECTORS, timeout_ms=400):
            await self._dismiss_user_not_found_overlay(page)
            raise ContactNotFoundError(f"User not found by phone: {phone}")
        chat_found = await self._wait_and_get_first(page, _OPENED_CHAT_SELECTORS + _MESSAGE_INPUT_SELECTORS, timeout_ms=1500)
        if not chat_found:
            await self._dismiss_user_not_found_overlay(page)
            raise ContactNotFoundError(f"User not found by phone: {phone}")
        return True

    async def _open_chat_by_chat_id(self, page: Page, chat_id: str, base_url: str) -> bool:
        logger.info(f"Opening existing chat by chat_id: {chat_id}")
        candidates = [
            f"{base_url}/{chat_id}",
        ]
        #f"{base_url}?chatId={chat_id}",
        #f"{base_url}/?chatId={chat_id}",
        #f"{base_url}/#/chats/{chat_id}",

        for url in candidates:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                opened = await self._wait_and_get_first(page, _OPENED_CHAT_SELECTORS + _MESSAGE_INPUT_SELECTORS, timeout_ms=4000)
                if opened:
                    return True
            except Exception:
                continue

        await page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
        loc = page.locator(
            f"[data-chat-id='{chat_id}'], [data-chatid='{chat_id}'], a[href*='{chat_id}']"
        )
        if await loc.count() > 0:
            await loc.first.click()
            opened = await self._wait_and_get_first(page, _OPENED_CHAT_SELECTORS + _MESSAGE_INPUT_SELECTORS, timeout_ms=8000)
            return bool(opened)
        return False

    async def _type_like_human(self, page: Page, selector: str, text: str) -> None:
        await page.click(selector, timeout=3000)
        if not text:
            return
        await self._human_pause(80, 220)
        # insert_text fires InputEvents like a paste; avoids 1–4ms/char keyboard.type.
        await page.keyboard.insert_text(text)

    async def _attachment_preview_ready(self, page: Page) -> bool:
        try:
            if await page.evaluate(_ATTACHMENT_PREVIEW_READY_JS):
                return True
        except Exception:
            pass
        for sel in _ATTACHMENT_PREVIEW_SELECTORS:
            try:
                handle = await page.query_selector(self._pw_selector(sel))
                if handle:
                    return True
            except Exception:
                continue
        return False

    async def _wait_for_attachment_preview(self, page: Page, timeout_ms: int = 15000) -> bool:
        deadline = asyncio.get_event_loop().time() + timeout_ms / 1000.0
        while asyncio.get_event_loop().time() < deadline:
            if await self._attachment_preview_ready(page):
                return True
            await asyncio.sleep(0.15)
        return False

    async def _attach_file_to_chat(self, page: Page, attachment_path: Union[str, Path]) -> None:
        path = Path(attachment_path)
        if not path.exists():
            raise MaxMessengerError("Attachment file not found")

        attach_selector = await self._wait_and_get_first(page, _ATTACH_BUTTON_SELECTORS, timeout_ms=8000)
        if not attach_selector:
            await self._diag_snapshot(page, "attach_button_missing")
            raise MaxMessengerError("Attach button not found")

        is_image = _is_image_file(path)
        menu_selectors = _ATTACH_MENU_MEDIA_SELECTORS if is_image else _ATTACH_MENU_FILE_SELECTORS

        async with page.expect_file_chooser(timeout=15000) as fc_info:
            await page.click(attach_selector)
            menu_selector = await self._wait_and_get_first(page, menu_selectors, timeout_ms=1500)
            if menu_selector:
                await page.click(menu_selector)
            else:
                fallback_selector = await self._wait_and_get_first(page, _ATTACH_MENU_FILE_SELECTORS, timeout_ms=800)
                if fallback_selector:
                    await page.click(fallback_selector)

        chooser = await fc_info.value
        await chooser.set_files(str(path))

        preview_ok = await self._wait_for_attachment_preview(page, timeout_ms=15000)
        if not preview_ok:
            logger.warning(
                f"Attachment preview did not appear within 15s path={path} — not sending until preview is in the composer"
            )
            await self._diag_snapshot(page, "attachment_preview_missing")
            raise MaxMessengerError("Attachment preview did not appear in the composer")
        await self._human_pause(200, 500)

    async def send_message(
        self,
        phone: str,
        text: str,
        attachment_path: Optional[Union[str, Path]] = None,
        base_url: str = DEFAULT_BASE_URL,
        chat_id: Optional[str] = None,
        use_chat_id: bool = False,
        humanize: bool = True,
    ) -> SendMaxMessageResult:
        last_err: Optional[BaseException] = None
        for attempt in range(2):
            async with self._op_lock:
                self._current_phone_for_mapping = phone
                try:
                    page = await self._ensure_page()
                    self.tasks_count += 1
                    if self.tasks_count > self.max_tasks:
                        logger.info(f"Task limit ({self.max_tasks}) reached. Restarting browser...")
                        await self._restart_browser("task_limit")
                        page = await self._ensure_page()

                    try:
                        await page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
                    except PlaywrightTimeoutError:
                        pass

                    if use_chat_id and chat_id:
                        self._cache_phone_chat(phone=phone, chat_id=str(chat_id))
                        if not await self._open_chat_by_chat_id(page, chat_id, base_url):
                            return SendMaxMessageResult(
                                sent_ok=False,
                                status_note="failed",
                                error_message=f"Existing chat not opened for chat_id={chat_id}",
                            )
                    else:
                        try:
                            if not await self._open_chat_by_phone(page, phone):
                                raise ContactNotFoundError(f"User not found by phone: {phone}")
                        except ContactNotFoundError as e:
                            return SendMaxMessageResult(
                                sent_ok=False,
                                status_note="user_not_found_by_phone",
                                error_message=str(e),
                            )

                    await self._maybe_capture_chat_id(page, phone)
                    if phone not in self._chat_by_phone:
                        await self._wait_for_chat_id(phone, timeout_seconds=1.5)

                    message_selector = await self._wait_and_get_first(page, _MESSAGE_INPUT_SELECTORS, timeout_ms=2500)
                    if not message_selector:
                        if await self._is_user_not_found_modal(page):
                            await self._dismiss_user_not_found_overlay(page)
                            return SendMaxMessageResult(
                                sent_ok=False,
                                status_note="user_not_found_by_phone",
                                error_message=f"User not found by phone: {phone}",
                            )
                        return SendMaxMessageResult(sent_ok=False, status_note="failed", error_message="Input field not found")

                    if humanize:
                        await self._human_pause(200, 500)

                    randomized_text = _randomize_text_with_invisible_chars(text)

                    await page.click(message_selector, timeout=3000)
                    if attachment_path:
                        try:
                            await self._attach_file_to_chat(page, attachment_path)
                        except Exception as e:
                            return SendMaxMessageResult(sent_ok=False, status_note="failed", error_message=str(e))

                    await self._type_like_human(page, message_selector, randomized_text)
                    await self._human_pause(120, 320)
                    await page.keyboard.press("Enter")
                    try:
                        await page.wait_for_function(
                            """() => {
                                const wrappers = document.querySelectorAll('div[class*="messageWrapper"]');
                                if (!wrappers.length) return false;
                                const last = wrappers[wrappers.length - 1];
                                return (last.className || '').includes('messageWrapper--isOut');
                            }""",
                            timeout=2500,
                        )
                    except PlaywrightTimeoutError:
                        pass

                    await self._maybe_capture_chat_id(page, phone)
                    if phone not in self._chat_by_phone:
                        await self._wait_for_chat_id(phone, timeout_seconds=2.0)

                    captured_chat_id = self._chat_by_phone.get(phone) or chat_id
                    if not captured_chat_id:
                        logger.error(
                            f"chat_id not captured after send phone={phone} url={getattr(page, 'url', '')}"
                        )
                    return SendMaxMessageResult(
                        sent_ok=True,
                        status_note="delivered",
                        chat_id=captured_chat_id,
                    )
                except ContactNotFoundError as e:
                    return SendMaxMessageResult(
                        sent_ok=False,
                        status_note="user_not_found_by_phone",
                        error_message=str(e),
                    )
                except (PlaywrightError, PlaywrightTimeoutError) as e:
                    last_err = e
                    logger.error(f"Playwright error on send_message attempt={attempt + 1}: {type(e).__name__}: {e}")
                    if attempt == 0:
                        await self._restart_browser("playwright_error")
                        continue
                    return SendMaxMessageResult(sent_ok=False, status_note="failed", error_message=str(e))
                except Exception as e:
                    last_err = e
                    logger.exception(f"Error sending message to {phone}")
                    if attempt == 0:
                        await self._restart_browser("exception")
                        continue
                    return SendMaxMessageResult(sent_ok=False, status_note="failed", error_message=str(e))
                finally:
                    self._current_phone_for_mapping = None
        return SendMaxMessageResult(sent_ok=False, status_note="failed", error_message=str(last_err) if last_err else "unknown")

    async def check_reply(self, phone: str, base_url: str = DEFAULT_BASE_URL) -> CheckMaxMessageResult:
        cached = self._get_cached_check_reply(phone)
        if cached:
            return cached
        last_err: Optional[BaseException] = None
        for attempt in range(2):
            async with self._op_lock:
                try:
                    page = await self._ensure_page()
                    self.tasks_count += 1
                    if self.tasks_count > self.max_tasks:
                        logger.info(f"Task limit ({self.max_tasks}) reached. Restarting browser...")
                        await self._restart_browser("task_limit")
                        page = await self._ensure_page()

                    try:
                        await page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
                    except PlaywrightTimeoutError:
                        pass

                    try:
                        opened = await self._open_chat_by_phone(page, phone)
                    except ContactNotFoundError:
                        return CheckMaxMessageResult(reply_value="", check_ok=False)
                    if not opened:
                        return CheckMaxMessageResult(reply_value="", check_ok=False)

                    await self._maybe_capture_chat_id(page, phone)

                    await self._wait_and_get_first(page, _CHAT_HISTORY_SELECTORS, timeout_ms=8000)
                    payload = await page.evaluate(_PARSE_LAST_MESSAGE_JS)

                    if payload.get("error"):
                        return CheckMaxMessageResult(reply_value="", check_ok=False)

                    variant = (payload.get("variant") or "").strip().casefold()
                    is_out = bool(payload.get("isOut"))
                    text = str(payload.get("text") or "").strip()
                    status_icon = str(payload.get("statusIcon") or "")

                    logger.info(f"Debug: is_out={is_out}, variant={variant}, statusIcon={status_icon}")

                    is_viewed = False
                    status_icon_lower = status_icon.lower()
                    if is_out and (
                        "check-double" in status_icon_lower or
                        "read" in status_icon_lower or
                        "viewed" in status_icon_lower or
                        "seen" in status_icon_lower
                    ):
                        is_viewed = True

                    if variant == "incoming" or not is_out:
                        return CheckMaxMessageResult(
                            reply_value=text,
                            check_ok=True,
                            replied_at=datetime.utcnow().isoformat() + "Z",
                        )

                    return CheckMaxMessageResult(reply_value="", check_ok=True, is_viewed=is_viewed)
                except (PlaywrightError, PlaywrightTimeoutError) as e:
                    last_err = e
                    logger.error(f"Playwright error on check_reply attempt={attempt + 1}: {type(e).__name__}: {e}")
                    if attempt == 0:
                        await self._restart_browser("playwright_error")
                        continue
                    return CheckMaxMessageResult(reply_value="", check_ok=False)
                except Exception as e:
                    last_err = e
                    logger.exception(f"Error checking reply for {phone}")
                    if attempt == 0:
                        await self._restart_browser("exception")
                        continue
                    return CheckMaxMessageResult(reply_value="", check_ok=False)
        return CheckMaxMessageResult(reply_value="", check_ok=False)

    async def _maybe_capture_chat_id(self, page: Page, phone: str) -> None:
        chat_id = _extract_chat_id_from_url(page.url)
        if not chat_id:
            try:
                chat_id = await page.evaluate(
                    """() => {
                      const fromUrl = () => {
                        const href = String(location.href || '');
                        const m1 = href.match(/(?:chatId=|chat_id=|chat\\/|chats\\/)(\\d+)/);
                        if (m1) return m1[1];
                        const m2 = href.match(/[#&?]c(?:hat)?=(\\d+)/);
                        if (m2) return m2[1];
                        return null;
                      };

                      const fromDom = () => {
                        const el =
                          document.querySelector('[data-chat-id]') ||
                          document.querySelector('[data-chatid]') ||
                          document.querySelector('.openedChat') ||
                          document.querySelector('[class*="openedChat"]');
                        if (!el) return null;
                        return (
                          el.getAttribute('data-chat-id') ||
                          el.getAttribute('data-chatid') ||
                          (el.dataset ? (el.dataset.chatId || el.dataset.chatid) : null) ||
                          null
                        );
                      };

                      return fromUrl() || fromDom();
                    }"""
                )
            except Exception:
                chat_id = None
        if not chat_id:
            return
        chat_id_str = str(chat_id)
        self._cache_phone_chat(phone=phone, chat_id=chat_id_str)

    def _cache_phone_chat(self, phone: str, chat_id: str) -> None:
        try:
            self._chat_by_phone[phone] = chat_id
            self._chat_by_phone.move_to_end(phone)
            self._phone_by_chat_id[chat_id] = phone
            self._phone_by_chat_id.move_to_end(chat_id)

            while len(self._chat_by_phone) > _PHONE_CHAT_CACHE_MAX_SIZE:
                old_phone, old_chat = self._chat_by_phone.popitem(last=False)
                if self._phone_by_chat_id.get(old_chat) == old_phone:
                    self._phone_by_chat_id.pop(old_chat, None)

            while len(self._phone_by_chat_id) > _PHONE_CHAT_CACHE_MAX_SIZE:
                old_chat, old_phone = self._phone_by_chat_id.popitem(last=False)
                if self._chat_by_phone.get(old_phone) == old_chat:
                    self._chat_by_phone.pop(old_phone, None)
        except Exception:
            return

    async def _wait_for_chat_id(self, phone: str, timeout_seconds: float) -> Optional[str]:
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while asyncio.get_event_loop().time() < deadline:
            chat_id = self._chat_by_phone.get(phone)
            if chat_id:
                return chat_id
            await asyncio.sleep(0.05)
        return None

    def _get_cached_check_reply(self, phone: str) -> Optional[CheckMaxMessageResult]:
        chat_id = self._chat_by_phone.get(phone)
        if not chat_id:
            return None
        st = self._chat_status.get(chat_id) or {}
        incoming_text = st.get("last_incoming_text")
        incoming_time = st.get("last_incoming_time")
        if isinstance(incoming_text, str) and incoming_text.strip():
            return CheckMaxMessageResult(
                reply_value=incoming_text,
                check_ok=True,
                replied_at=incoming_time or datetime.utcnow().isoformat() + "Z",
                from_ws_cache=True,
            )
        if st.get("is_viewed"):
            return CheckMaxMessageResult(reply_value="", check_ok=True, is_viewed=True)
        return CheckMaxMessageResult(reply_value="", check_ok=True)

    async def next_ws_action(self) -> Dict[str, Any]:
        return await self._ws_actions.get()

def normalize_phone_for_max(raw: Union[str, int, float, None]) -> Optional[str]:
    if raw is None: return None
    s = "".join(c for c in str(raw) if c.isdigit())
    if len(s) == 11:
        if s.startswith("8") or s.startswith("7"):
            s = s[1:]
    if len(s) == 10 and s.startswith("9"):
        return s
    return None

def _extract_chat_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"(?:chatId=|chat_id=|chat/|chats/)(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"[#&?]c(?:hat)?=(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"https?://[^/]+/(\d+)(?:[/?#]|$)", url)
    if m:
        return m.group(1)
    return None

def _lz4_decompress(src: bytes, max_output_size: int) -> bytes:
    out = bytearray(max_output_size)
    si = 0
    oi = 0
    slen = len(src)
    while si < slen:
        token = src[si]
        si += 1
        lit_len = token >> 4
        if lit_len == 15:
            while True:
                if si >= slen:
                    raise ValueError("lz4: corrupt block")
                b = src[si]
                si += 1
                lit_len += b
                if b != 255:
                    break
        if si + lit_len > slen or oi + lit_len > max_output_size:
            raise ValueError("lz4: corrupt block")
        out[oi : oi + lit_len] = src[si : si + lit_len]
        si += lit_len
        oi += lit_len
        if si == slen:
            break
        if si + 2 > slen:
            raise ValueError("lz4: corrupt block")
        offset = src[si] | (src[si + 1] << 8)
        si += 2
        if offset == 0 or offset > oi:
            raise ValueError("lz4: corrupt block")
        match_len = (token & 0x0F) + 4
        if (token & 0x0F) == 15:
            while True:
                if si >= slen:
                    raise ValueError("lz4: corrupt block")
                b = src[si]
                si += 1
                match_len += b
                if b != 255:
                    break
        if oi + match_len > max_output_size:
            raise ValueError("lz4: corrupt block")
        ref = oi - offset
        if offset >= match_len:
            out[oi : oi + match_len] = out[ref : ref + match_len]
            oi += match_len
        else:
            end = oi + match_len
            while oi < end:
                out[oi] = out[ref]
                oi += 1
                ref += 1
    return bytes(out[:oi])

def _msgpack_decode(data: bytes) -> Any:
    def read(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(data):
            raise ValueError("msgpack: truncated")
        b = data[pos : pos + n]
        pos += n
        return b

    def decode_one() -> Any:
        nonlocal pos
        b0 = data[pos]
        pos += 1

        if b0 <= 0x7F:
            return b0
        if b0 >= 0xE0:
            return b0 - 256

        if 0xA0 <= b0 <= 0xBF:
            ln = b0 & 0x1F
            return read(ln).decode("utf-8", errors="replace")

        if 0x90 <= b0 <= 0x9F:
            ln = b0 & 0x0F
            return [decode_one() for _ in range(ln)]

        if 0x80 <= b0 <= 0x8F:
            ln = b0 & 0x0F
            m: Dict[Any, Any] = {}
            for _ in range(ln):
                k = decode_one()
                v = decode_one()
                m[k] = v
            return m

        if b0 == 0xC0:
            return None
        if b0 == 0xC2:
            return False
        if b0 == 0xC3:
            return True

        if b0 == 0xCC:
            return struct.unpack(">B", read(1))[0]
        if b0 == 0xCD:
            return struct.unpack(">H", read(2))[0]
        if b0 == 0xCE:
            return struct.unpack(">I", read(4))[0]
        if b0 == 0xCF:
            return struct.unpack(">Q", read(8))[0]

        if b0 == 0xD0:
            return struct.unpack(">b", read(1))[0]
        if b0 == 0xD1:
            return struct.unpack(">h", read(2))[0]
        if b0 == 0xD2:
            return struct.unpack(">i", read(4))[0]
        if b0 == 0xD3:
            return struct.unpack(">q", read(8))[0]

        if b0 == 0xCA:
            return struct.unpack(">f", read(4))[0]
        if b0 == 0xCB:
            return struct.unpack(">d", read(8))[0]

        if b0 == 0xC4:
            ln = struct.unpack(">B", read(1))[0]
            return read(ln)
        if b0 == 0xC5:
            ln = struct.unpack(">H", read(2))[0]
            return read(ln)
        if b0 == 0xC6:
            ln = struct.unpack(">I", read(4))[0]
            return read(ln)

        if b0 == 0xD9:
            ln = struct.unpack(">B", read(1))[0]
            return read(ln).decode("utf-8", errors="replace")
        if b0 == 0xDA:
            ln = struct.unpack(">H", read(2))[0]
            return read(ln).decode("utf-8", errors="replace")
        if b0 == 0xDB:
            ln = struct.unpack(">I", read(4))[0]
            return read(ln).decode("utf-8", errors="replace")

        if b0 == 0xDC:
            ln = struct.unpack(">H", read(2))[0]
            return [decode_one() for _ in range(ln)]
        if b0 == 0xDD:
            ln = struct.unpack(">I", read(4))[0]
            return [decode_one() for _ in range(ln)]

        if b0 == 0xDE:
            ln = struct.unpack(">H", read(2))[0]
            m = {}
            for _ in range(ln):
                k = decode_one()
                v = decode_one()
                m[k] = v
            return m
        if b0 == 0xDF:
            ln = struct.unpack(">I", read(4))[0]
            m = {}
            for _ in range(ln):
                k = decode_one()
                v = decode_one()
                m[k] = v
            return m

        if b0 in (0xC7, 0xC8, 0xC9):
            if b0 == 0xC7:
                ln = struct.unpack(">B", read(1))[0]
            elif b0 == 0xC8:
                ln = struct.unpack(">H", read(2))[0]
            else:
                ln = struct.unpack(">I", read(4))[0]
            ext_type = struct.unpack(">b", read(1))[0]
            payload = read(ln)
            if ext_type == 1:
                try:
                    return int(_msgpack_decode(payload))
                except Exception:
                    return payload
            return payload

        raise ValueError(f"msgpack: unsupported type 0x{b0:02x}")

    pos = 0
    val = decode_one()
    return val

def _decode_max_binary_frame(raw: bytes) -> Optional[Dict[str, Any]]:
    if not raw or len(raw) < 10:
        return None
    proto = raw[0]
    cmd = raw[1]
    seq = struct.unpack(">h", raw[2:4])[0]
    opcode = struct.unpack(">h", raw[4:6])[0]
    factor = raw[6]
    payload_len = (raw[7] << 16) | (raw[8] << 8) | raw[9]
    if payload_len <= 0:
        return {"proto": proto, "cmd": cmd, "seq": seq, "opcode": opcode, "payload": None}
    payload_comp = raw[10 : 10 + payload_len]
    if len(payload_comp) != payload_len:
        return None
    try:
        payload_bytes = payload_comp
        if factor and factor > 0:
            payload_bytes = _lz4_decompress(payload_comp, payload_len * factor)
        payload = _msgpack_decode(payload_bytes)
    except Exception:
        payload = None
    return {"proto": proto, "cmd": cmd, "seq": seq, "opcode": opcode, "payload": payload}

def _extract_message_is_out(message: Dict[str, Any], viewer_id: Optional[int]) -> bool:
    if "isOut" in message:
        return bool(message.get("isOut"))
    if viewer_id is not None:
        for key in ("senderId", "sender", "authorId", "author", "fromUserId", "from"):
            if key not in message:
                continue
            sender = message.get(key)
            try:
                if int(sender) == int(viewer_id):
                    return True
            except Exception:
                continue
    return False

def _extract_message_text(message: Dict[str, Any]) -> str:
    v = message.get("text")
    if isinstance(v, str):
        return v.strip()
    content = message.get("content")
    if isinstance(content, dict):
        t = content.get("text")
        if isinstance(t, str):
            return t.strip()
    return ""

def _extract_message_time_iso(message: Dict[str, Any]) -> Optional[str]:
    t = message.get("time")
    if t is None:
        return None
    try:
        ts = float(t)
    except Exception:
        return None
    if ts > 1e12:
        ts = ts / 1000.0
    try:
        return datetime.utcfromtimestamp(ts).isoformat() + "Z"
    except Exception:
        return None
