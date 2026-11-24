"""FUNPAY CONTROL CENTER — async Telegram management bot.

Supported integration: fakelag28/funpay-api-python (unofficial FunPay client).
Only methods documented by that client are called. Reviews, claims, fake-online
heartbeats and undocumented lot-field mutations are deliberately not fabricated.

Run: python main.py
"""
from __future__ import annotations

import asyncio
import html
import hashlib
import inspect
import json
import logging
import os
import random
import re
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Awaitable, Callable

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv
from loguru import logger

from storage import Storage

load_dotenv()

# === СЕКЦИЯ: КОНФИГУРАЦИЯ И ЛОГИРОВАНИЕ ===

def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on", "да"}


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


@dataclass(frozen=True)
class Config:
    bot_token: str = os.getenv("BOT_TOKEN", "").strip()
    admin_id: int = env_int("ADMIN_ID", 0, 0, 2**63 - 1)
    trusted_admins: tuple[int, ...] = tuple(
        int(x.strip()) for x in os.getenv("TRUSTED_ADMINS", "").split(",")
        if x.strip().isdigit()
    )
    golden_key: str = os.getenv("FUNPAY_GOLDEN_KEY", "").strip()
    user_agent: str = os.getenv(
        "FUNPAY_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    ).strip()
    proxy: str = os.getenv("FUNPAY_PROXY", "").strip()
    database: str = os.getenv("DATABASE_PATH", "funpay.db")
    products_dir: str = os.getenv("PRODUCTS_DIR", "products")
    backups_dir: str = os.getenv("BACKUPS_DIR", "backups")
    logs_dir: str = os.getenv("LOGS_DIR", "logs")
    request_timeout: int = env_int("REQUEST_TIMEOUT", 20, 5, 120)
    order_poll: int = env_int("ORDER_POLL_INTERVAL", 20, 10, 300)
    chat_poll: int = env_int("CHAT_POLL_INTERVAL", 12, 5, 180)
    session_interval: int = env_int("SESSION_CHECK_INTERVAL", 300, 60, 1800)
    watchdog_interval: int = env_int("WATCHDOG_INTERVAL", 30, 10, 300)
    backup_interval: int = env_int("BACKUP_INTERVAL", 86400, 3600, 604800)
    max_retries: int = env_int("MAX_RETRIES", 4, 1, 8)
    max_upload_bytes: int = env_int("MAX_UPLOAD_MB", 5, 1, 20) * 1024 * 1024
    startup_grace: int = env_int("STARTUP_GRACE_SECONDS", 120, 15, 600)

    @property
    def admins(self) -> set[int]:
        return ({self.admin_id} if self.admin_id else set()) | set(self.trusted_admins)


CFG = Config()
Path(CFG.logs_dir).mkdir(parents=True, exist_ok=True)
logger.remove()
logger.add(
    str(Path(CFG.logs_dir) / "funpay_{time:YYYY-MM-DD}.log"),
    level="INFO", rotation="10 MB", retention="21 days", encoding="utf-8",
    enqueue=True, backtrace=False, diagnose=False,
)
logger.add(lambda msg: print(msg, end=""), level="INFO", colorize=False)

# === СЕКЦИЯ: УТИЛИТЫ И НОРМАЛИЗАЦИЯ МОДЕЛЕЙ ===

def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=False)


def obj_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump(mode="python")
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        try:
            return vars(obj)
        except TypeError:
            pass
    return {}


def field(obj: Any, *names: str, default: Any = None) -> Any:
    data = obj_dict(obj)
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
        value = getattr(obj, name, None) if obj is not None else None
        if value is not None:
            return value
    return default


def enum_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "value"):
        value = value.value
    if hasattr(value, "name") and not isinstance(value, str):
        return str(value.name)
    return str(value)


def nested_username(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return enum_text(field(value, "username", "name", "display_name", "login", default="")).strip()


def safe_format(template: str, **variables: Any) -> str:
    class SafeMap(dict):
        def __missing__(self, key: str) -> str:
            return "{" + key + "}"
    try:
        return str(template).format_map(SafeMap({k: str(v) for k, v in variables.items()}))
    except (ValueError, TypeError):
        return str(template)


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.replace(" ", "").replace(",", ".")
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number / 1000 if number > 10_000_000_000 else number
    if isinstance(value, datetime):
        return value.timestamp()
    raw = str(value).strip()
    if raw.isdigit():
        return parse_timestamp(int(raw))
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        pass
    # FunPay order cards may expose local dates instead of ISO-8601.
    # Parse common day-first formats so the startup baseline does not
    # accidentally classify every existing order as an unknown timestamp.
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%y %H:%M:%S",
                "%d.%m.%y %H:%M", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(raw, fmt).astimezone().timestamp()
        except ValueError:
            continue
    return None


def is_paid_status(status: str) -> bool:
    normalized = status.casefold().replace("_", " ").replace("-", " ")
    negative = ("refund", "cancel", "отмен", "возврат", "dispute", "спор")
    if any(token in normalized for token in negative):
        return False
    positive = ("paid", "оплачен", "оплач", "delivery", "выдач", "completed", "confirmed", "active")
    return any(token in normalized for token in positive)


def money(value: float, currency: str = "") -> str:
    return f"{value:,.2f} {currency}".replace(",", " ").strip()


def safe_filename(name: str) -> str:
    cleaned = Path(name).name
    cleaned = re.sub(r"[^a-zA-Z0-9А-Яа-я._ -]", "_", cleaned).strip(" .")
    return (cleaned or "stock.txt")[:100]

# === СЕКЦИЯ: FUNPAY ADAPTER ===
class FunPayGateway:
    """Thin async adapter around the documented public methods of the selected client."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.client: Any = None
        self.account: Any = None
        self.ready = False
        self.me: Any = None
        self.last_error = ""
        self._context_open = False

    async def connect(self) -> None:
        if not self.cfg.golden_key:
            self.ready = False
            self.last_error = "FUNPAY_GOLDEN_KEY is not configured"
            return
        if self.cfg.proxy:
            # The selected package reads FUNPAY_PROXY from its environment.
            os.environ["FUNPAY_PROXY"] = self.cfg.proxy
        os.environ["FUNPAY_TIMEOUT"] = str(self.cfg.request_timeout)
        try:
            from funpay_api_python import FunPayAccount, FunPayClient
            self.client = FunPayClient(
                golden_key=self.cfg.golden_key,
                user_agent=self.cfg.user_agent,
            )
            await self.client.__aenter__()
            self._context_open = True
            self.account = FunPayAccount(self.client)
            self.me = await self._invoke("get_me", retry_read=True)
            self.ready = True
            self.last_error = ""
            logger.info("FunPay connected; account={}", nested_username(field(self.me, "username", default="—")))
        except Exception as exc:
            self.ready = False
            self.last_error = f"{type(exc).__name__}: {str(exc)[:300]}"
            await self.close()
            raise

    async def close(self) -> None:
        self.ready = False
        if self.client is not None and self._context_open:
            try:
                await self.client.__aexit__(None, None, None)
            except Exception as exc:
                logger.warning("FunPay client close warning: {}", exc)
        self._context_open = False
        self.client = None
        self.account = None

    async def _invoke(self, method_name: str, *args: Any, retry_read: bool = False, **kwargs: Any) -> Any:
        if self.account is None:
            raise RuntimeError("FunPay session is not connected")
        method = getattr(self.account, method_name, None)
        if not callable(method):
            raise RuntimeError(f"FunPay client method is unavailable: {method_name}")
        try:
            signature = inspect.signature(method)
            accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values())
            if not accepts_kwargs:
                kwargs = {k: v for k, v in kwargs.items() if k in signature.parameters}
        except (TypeError, ValueError):
            pass

        attempts = self.cfg.max_retries if retry_read else 1
        for attempt in range(attempts):
            try:
                result = method(*args, **kwargs)
                if inspect.isawaitable(result):
                    return await asyncio.wait_for(result, timeout=self.cfg.request_timeout)
                return result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {str(exc)[:300]}"
                message = str(exc).casefold()
                if any(x in message for x in ("401", "unauthorized", "invalid golden", "not authorized", "авторизац")):
                    self.ready = False
                    raise PermissionError("FunPay authorization expired or rejected") from exc
                retryable = any(x in message for x in ("429", "rate limit", "503", "502", "timeout", "tempor", "connection"))
                if not retry_read or not retryable or attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(min(30.0, (2 ** attempt) + random.random()))
        raise RuntimeError("FunPay request retry loop ended unexpectedly")

    async def get_me(self) -> Any:
        self.me = await self._invoke("get_me", retry_read=True)
        return self.me

    async def get_sales(self) -> list[Any]:
        result = await self._invoke(
            "get_sales", include_paid=True, include_closed=False,
            include_refunded=False, start_from=None, limit=50, retry_read=True,
        )
        return list(result or [])

    async def get_chats(self) -> list[Any]:
        return list(await self._invoke("get_chats", retry_read=True) or [])

    async def get_chat_history(self, chat_id: str, last_message_id: str | None) -> list[Any]:
        kwargs = {"last_message_id": last_message_id} if last_message_id else {}
        return list(await self._invoke("get_chat_history", chat_id, retry_read=True, **kwargs) or [])

    async def send_message(self, chat_id: str, text: str) -> Any:
        # Mutations are deliberately not retried: a timeout can mean message accepted.
        return await self._invoke("send_message", chat_id, text)

    async def get_balance(self) -> Any:
        return await self._invoke("get_balance", retry_read=True)

    async def get_categories(self) -> list[Any]:
        return list(await self._invoke("get_categories", retry_read=True) or [])

    async def get_my_lots(self, subcategory_id: int) -> list[Any]:
        return list(await self._invoke("get_my_lots", int(subcategory_id), retry_read=True) or [])

    async def get_lot_fields(self, lot_id: int) -> dict[str, Any]:
        result = await self._invoke("get_lot_fields", int(lot_id), retry_read=True)
        return dict(field(result, "fields", default={}) or {})

    async def save_lot(self, lot_id: int, fields: dict[str, Any]) -> Any:
        # Saving a lot changes a live listing; no automatic retry is performed.
        return await self._invoke("save_lot", int(lot_id), fields)

    async def raise_lots(self, lot_ids: list[int]) -> Any:
        # This is a mutation. Never retry automatically after an ambiguous timeout.
        return await self._invoke("raise_lots", lot_ids)

# === СЕКЦИЯ: FSM И КЛАВИАТУРЫ ===
class InputFlow(StatesGroup):
    waiting_text = State()
    confirm_lot_changes = State()


def ikb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text=title, callback_data=data)
            for title, data in row
        ] for row in rows]
    )


def main_keyboard() -> InlineKeyboardMarkup:
    return ikb([
        [("⚙️ МОДУЛИ", "menu:modules"), ("💬 АВТООТВЕТЫ", "menu:replies")],
        [("📦 ТОВАРЫ / СКЛАД", "menu:products"), ("🧾 ЗАКАЗЫ", "menu:orders")],
        [("📊 ФИНАНСЫ", "menu:finance"), ("👥 ПОКУПАТЕЛИ", "menu:users")],
        [("🛠 ИНСТРУМЕНТЫ", "menu:tools"), ("📜 ЖУРНАЛ", "menu:logs")],
        [("🔄 ОБНОВИТЬ", "menu:home")],
    ])


def back_keyboard(target: str = "home") -> InlineKeyboardMarkup:
    return ikb([[("↩️ Назад", f"menu:{target}"), ("⌂ Главное меню", "menu:home")]])


def section_keyboard(section: str) -> InlineKeyboardMarkup:
    common = [("↩️ Назад", "menu:home"), ("⌂ Главное", "menu:home")]
    if section == "modules":
        return ikb([
            [("📦 Автовыдача", "toggle:auto_delivery"), ("💬 Автоответчик", "toggle:auto_responder")],
            [("🚀 Автоподнятие", "toggle:auto_bump"), ("🔕 Тихий режим", "toggle:quiet_mode")],
            [("⏸ Пауза всего", "toggle:paused")],
            [("☀️ День", "preset:day"), ("🌙 Ночь", "preset:night"), ("⚡ Фокус", "preset:focus")],
            common,
        ])
    if section == "replies":
        return ikb([
            [("✍️ Приветствие", "action:welcome"), ("🧾 Шаблон выдачи", "action:delivery_template")],
            [("➕ Добавить триггер", "action:add_rule"), ("🧩 Все правила", "action:rules")],
            [("🧪 Проверить ответ", "action:test_rule")], common,
        ])
    if section == "products":
        return ikb([
            [("📚 Каталог FunPay", "action:catalog"), ("🔗 Привязать лот", "action:add_map")],
            [("📥 Инструкция по импорту", "action:stock_help"), ("📋 Остатки", "action:inventory")],
            [("📄 Все привязки", "action:maps"), ("🚀 Настроить автоподнятие", "action:bump_config")],
            [("💰 Репрайсинг (с предпросмотром)", "action:reprice")], common,
        ])
    if section == "orders":
        return ikb([
            [("🔄 Обновить заказы", "action:refresh_orders"), ("⚠️ Неопределённые выдачи", "action:uncertain")],
            [("📦 Последние заказы", "action:orders_list")], common,
        ])
    if section == "finance":
        return ikb([
            [("Сегодня", "finance:today"), ("7 дней", "finance:week")],
            [("30 дней", "finance:month"), ("Всё время", "finance:all")],
            [("💳 Баланс FunPay", "action:balance")], common,
        ])
    if section == "users":
        return ikb([
            [("➕ Чёрный список", "action:list:black"), ("➕ Белый список", "action:list:white")],
            [("⭐ Добавить VIP", "action:list:vip"), ("🗑 Удалить из списка", "action:list:remove")],
            [("👁 Показать списки", "action:lists_show")], common,
        ])
    if section == "tools":
        return ikb([
            [("📦 Создать бэкап", "action:backup"), ("🩺 Проверка системы", "action:health")],
            [("💸 Комиссия", "action:commission"), ("📉 Порог остатков", "action:low_stock")],
            [("⏱ Интервал bump", "action:bump_interval")],
            [("🧹 Сбросить FSM", "action:cancel_fsm"), ("ℹ️ Помощь", "action:help")], common,
        ])
    if section == "logs":
        return ikb([
            [("📜 Последние действия", "action:audit")],
            [("📦 Скачать бэкап", "action:backup")], common,
        ])
    return main_keyboard()

# === СЕКЦИЯ: ОСНОВНОЙ КЛАСС ПРИЛОЖЕНИЯ ===
class App:
    def __init__(self) -> None:
        if not CFG.bot_token:
            raise RuntimeError("BOT_TOKEN is missing in .env")
        if not CFG.admin_id:
            raise RuntimeError("ADMIN_ID is missing in .env")
        self.storage = Storage(CFG.database, CFG.products_dir, CFG.backups_dir)
        self.fp = FunPayGateway(CFG)
        self.bot = Bot(CFG.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.stop_event = asyncio.Event()
        self.tasks: dict[str, asyncio.Task] = {}
        self.worker_intervals: dict[str, int] = {}
        self.heartbeats: dict[str, float] = {}
        self.failures: dict[str, int] = {}
        self.started_at = time.time()
        self.last_connect_alert = 0.0
        self.last_connect_attempt = 0.0
        self.me_username = ""
        self.register_handlers()

    def is_admin(self, user_id: int | None) -> bool:
        return bool(user_id and user_id in CFG.admins)

    async def audit(self, actor: str, action: str, details: str = "") -> None:
        await self.storage.add_audit(actor, action, details)
        logger.info("AUDIT actor={} action={} details={}", actor, action, details[:300])

    async def notify(self, message: str, important: bool = False) -> None:
        quiet = bool(await self.storage.get_setting("quiet_mode", False))
        if quiet and not important:
            return
        for admin_id in CFG.admins:
            try:
                await self.bot.send_message(admin_id, message[:3900])
            except Exception as exc:
                logger.warning("Telegram notification failed for admin {}: {}", admin_id, exc)

    async def enabled(self, key: str) -> bool:
        return bool(await self.storage.get_setting(key, False)) and not bool(await self.storage.get_setting("paused", False))

    async def render_home(self) -> str:
        stats = await self.storage.stats(time.time() - 86400)
        settings = await self.storage.settings()
        fp_state = "🟢 CONNECTED" if self.fp.ready else "🔴 OFFLINE"
        user = esc(self.me_username or "не определён")
        switches = sum(bool(settings.get(k)) for k in ("auto_delivery", "auto_responder", "auto_bump"))
        task_names = sum(1 for task in self.tasks.values() if not task.done())
        uncertain = await self.storage.deliveries_by_state(("uncertain",), 100)
        if len(stats.get("by_currency", [])) == 1:
            curr = stats["by_currency"][0].get("currency") or ""
            revenue_view = money(stats["revenue"], curr)
            net_view = money(stats["net"], curr)
        elif len(stats.get("by_currency", [])) > 1:
            revenue_view = "см. раздел Финансы (валюты раздельно)"
            net_view = "см. раздел Финансы (валюты раздельно)"
        else:
            revenue_view = net_view = "—"
        return (
            "<pre>╔══════════════════════════╗\n"
            "║   FUNPAY CONTROL CENTER ║\n"
            "╚══════════════════════════╝</pre>"
            f"<b>Аккаунт:</b> <code>{user}</code>\n"
            f"<b>Соединение:</b> {fp_state}\n"
            f"<b>Режим:</b> {'⏸ ПАУЗА' if settings.get('paused') else '⚡ АКТИВЕН'}\n"
            f"<b>Профиль:</b> {esc(settings.get('profile', 'Custom'))}\n"
            f"<b>Активных модулей:</b> {switches}/3\n"
            f"<b>Фоновые процессы:</b> {task_names}\n\n"
            "<b>📈 ЗА ПОСЛЕДНИЕ 24 ЧАСА</b>\n"
            f"Заказов в учёте: <b>{stats['n']}</b>\n"
            f"Выручка: <b>{esc(revenue_view)}</b>\n"
            f"Расчётная прибыль: <b>{esc(net_view)}</b>\n"
            f"Ожидают выдачи: <b>{stats['pending_orders']}</b>\n"
            f"Неопределённые отправки: <b>{len(uncertain)}</b>\n\n"
            "<i>Выбери раздел ниже. Изменения настроек сохраняются в SQLite.</i>"
        )

    async def render_modules(self) -> str:
        items = [
            ("auto_delivery", "Автовыдача товаров"),
            ("auto_responder", "Автоответчик"),
            ("auto_bump", "Автоподнятие разрешённых лотов"),
            ("quiet_mode", "Тихий режим уведомлений"),
            ("paused", "Общая пауза"),
        ]
        lines = ["<b>⚙️ MODULE CONTROL</b>", "<i>Нажми кнопку, чтобы переключить модуль.</i>", ""]
        for key, title in items:
            enabled = bool(await self.storage.get_setting(key, False))
            lines.append(f"{'🟢 ВКЛ' if enabled else '🔴 ВЫКЛ'}  ·  {title}")
        lines.append(f"\nПрофиль: <b>{esc(await self.storage.get_setting('profile', 'Custom'))}</b>")
        if not self.fp.ready:
            lines.append("\n⚠️ FunPay не подключён: торговые модули не выполняют операции.")
        return "\n".join(lines)

    async def render_replies(self) -> str:
        welcome = str(await self.storage.get_setting("welcome_text", ""))
        delivery = str(await self.storage.get_setting("delivery_template", ""))
        rules = await self.storage.rules(include_disabled=True)
        enabled_rules = sum(bool(r["enabled"]) for r in rules)
        lines = [
            "<b>💬 AUTO-REPLY STUDIO</b>",
            f"Приветствие: {'✅ настроено' if welcome else '— пусто'}",
            f"Шаблон выдачи: {'✅ настроен' if delivery else '— пусто'}",
            f"Активных правил: <b>{enabled_rules}</b> / {len(rules)}",
            "",
            "<b>Переменные:</b> <code>{username}</code>, <code>{order_id}</code>, <code>{lot_title}</code>, <code>{count}</code>, <code>{price}</code>, <code>{items}</code>",
            "<i>Формат правила: режим | условие | ответ | приоритет | cooldown_сек</i>",
        ]
        return "\n".join(lines)

    async def render_products(self) -> str:
        items = await self.storage.inventory()
        lines = ["<b>📦 INVENTORY / LOTS</b>"]
        if not items:
            lines.append("Пока нет привязанных лотов. Добавь первый через кнопку ниже.")
        for item in items[:25]:
            lines.append(
                f"\n<b>{esc(item['lot_title'])}</b> · <code>{esc(item['lot_key'])}</code>\n"
                f"Тип: <code>{esc(item['mode'])}</code> · Остаток: <b>{item['available'] or 0}</b> · Всего: {item['total'] or 0}"
            )
        maps = await self.storage.lot_maps()
        lines.append(f"\nВсего привязок: <b>{len(maps)}</b>")
        return "\n".join(lines)

    async def render_orders(self) -> str:
        rows = await self.storage.recent_orders(12)
        pending = await self.storage.deliveries_by_state(("uncertain", "prepared", "out_of_stock"), 20)
        lines = ["<b>🧾 ORDER TERMINAL</b>", f"Заказов в базе: <b>{len(rows)}</b>", f"Требуют внимания: <b>{len(pending)}</b>", ""]
        if not rows:
            lines.append("Заказов пока нет или FunPay ещё не синхронизирован.")
        for row in rows:
            status = "✅ выдан" if row["delivered"] else f"⏳ {esc(row.get('delivery_state') or 'ожидает')}"
            lines.append(
                f"<code>#{esc(row['order_id'])}</code> · {esc(row.get('buyer') or '—')}\n"
                f"{esc(row.get('status') or 'Статус неизвестен')} · {status}"
            )
        return "\n".join(lines)

    async def render_finance(self, period: str = "today") -> str:
        windows = {
            "today": 86400,
            "week": 7 * 86400,
            "month": 30 * 86400,
            "all": 0,
        }
        since = 0 if period == "all" else time.time() - windows.get(period, 86400)
        stats = await self.storage.stats(since)
        labels = {"today": "Сегодня / 24 часа", "week": "Последние 7 дней", "month": "Последние 30 дней", "all": "Всё время"}
        currency_lines = []
        for group in stats.get("by_currency", []):
            cur = str(group.get("currency") or "без валюты")
            currency_lines.append(
                f"\n<b>{esc(cur)}</b> · {group['n']} продаж\n"
                f"Выручка: {money(group['revenue'], cur)}\n"
                f"Себестоимость: {money(group['cost'], cur)}\n"
                f"Комиссия: {money(group['fee'], cur)}\n"
                f"Чистая прибыль: <b>{money(group['net'], cur)}</b>"
            )
        grouped_text = "\n".join(currency_lines) if currency_lines else "Пока нет подтверждённых выдач с финансовыми данными."
        return (
            f"<b>📊 FINANCIAL OVERVIEW · {labels.get(period, 'Период')}</b>\n\n"
            f"Продаж учтено: <b>{stats['n']}</b>\n"
            f"{grouped_text}\n\n"
            f"Ожидают обработки: {stats['pending_orders']}\n"
            "<i>Валюты не суммируются между собой. Данные относятся к заказам, успешно выданным этим ботом; комиссия и прибыль расчётные.</i>"
        )

    async def render_users(self) -> str:
        lines = ["<b>👥 BUYER ACCESS</b>"]
        for kind, label in (("black", "⛔ Чёрный список"), ("white", "🛡 Белый список"), ("vip", "⭐ VIP")):
            users = await self.storage.list_users(kind)
            preview = ", ".join(esc(x["username"]) for x in users[:15]) or "—"
            suffix = f" (+{len(users)-15})" if len(users) > 15 else ""
            lines.append(f"\n{label}: <b>{len(users)}</b>\n<code>{preview}{suffix}</code>")
        lines.append("\nБелый список работает как allowlist автоответчика, только если он не пуст.")
        return "\n".join(lines)

    async def render_tools(self) -> str:
        return (
            "<b>🛠 SYSTEM TOOLBOX</b>\n\n"
            f"FunPay: {'🟢 online' if self.fp.ready else '🔴 offline'}\n"
            f"Последняя ошибка: <code>{esc(self.fp.last_error or 'нет')}</code>\n"
            f"Задачи: <b>{sum(not t.done() for t in self.tasks.values())}</b>\n"
            "\nДоступно: резервная копия, проверка соединения, сброс незавершённого ввода.\n"
            "\nАвто-претензии, автоответы на отзывы, создание новых лотов и массовое редактирование в этой сборке пока не включены в панель. Клиент имеет отдельные методы для части операций с лотами, но перед включением требуется отдельный мастер с предварительным просмотром и подтверждением."
        )

    async def render_logs(self) -> str:
        rows = await self.storage.audit_tail(15)
        lines = ["<b>📜 AUDIT LOG</b>"]
        for row in rows:
            stamp = datetime.fromtimestamp(row["created"]).strftime("%d.%m %H:%M:%S")
            lines.append(f"<code>{stamp}</code> · <b>{esc(row['action'])}</b> · {esc(row['details'][:110])}")
        if not rows:
            lines.append("Записей пока нет.")
        return "\n".join(lines)

    async def render_uncertain(self) -> tuple[str, InlineKeyboardMarkup]:
        rows = await self.storage.deliveries_by_state(("uncertain", "prepared", "out_of_stock"), 15)
        lines = ["<b>⚠️ DELIVERY REVIEW</b>", "Отправки с неясным результатом нельзя повторять автоматически.", ""]
        buttons: list[list[tuple[str, str]]] = []
        for row in rows:
            oid = str(row["order_id"])
            lines.append(f"<code>#{esc(oid)}</code> · <b>{esc(row['state'])}</b> · {len(row['items'])} поз.")
            if row["state"] in {"uncertain", "prepared"} and len(oid) <= 32:
                buttons.append([(f"#{oid} · отметить отправленным", f"delivery:sent:{oid}")])
                buttons.append([(f"#{oid} · разрешить повтор", f"delivery:reset:{oid}")])
        if not rows:
            lines.append("Нет отправок, требующих проверки.")
        buttons.append([("↩️ Назад", "menu:orders"), ("⌂ Главное", "menu:home")])
        return "\n".join(lines), ikb(buttons)

    # === ACCESS AND CALLBACK HELPERS ===
    async def guarded_callback(self, call: CallbackQuery) -> bool:
        if not self.is_admin(call.from_user.id if call.from_user else None):
            await call.answer("Доступ запрещён", show_alert=True)
            return False
        return True

    async def edit_screen(self, call: CallbackQuery, text: str, markup: InlineKeyboardMarkup) -> None:
        try:
            await call.message.edit_text(text, reply_markup=markup)
        except Exception as exc:
            if "message is not modified" not in str(exc).casefold():
                await call.message.answer(text, reply_markup=markup)

    async def ask_text(self, call: CallbackQuery, state: FSMContext, action: str, prompt: str, back: str) -> None:
        await state.set_state(InputFlow.waiting_text)
        await state.update_data(action=action)
        await call.message.answer(prompt, reply_markup=back_keyboard(back))

    # === СЕКЦИЯ: TELEGRAM HANDLERS ===
    def register_handlers(self) -> None:
        @self.dp.message(Command("start", "menu"))
        async def cmd_start(message: Message) -> None:
            if not self.is_admin(message.from_user.id if message.from_user else None):
                return
            await message.answer(await self.render_home(), reply_markup=main_keyboard())

        @self.dp.message(Command("status"))
        async def cmd_status(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await message.answer(await self.render_home(), reply_markup=main_keyboard())

        @self.dp.message(Command("id"))
        async def cmd_id(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await message.answer(f"Твой Telegram ID: <code>{message.from_user.id}</code>")

        @self.dp.message(Command("ping"))
        async def cmd_ping(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await message.answer(
                    f"🏓 <b>PONG</b>\nTelegram: отвечает\nFunPay: {'🟢 connected' if self.fp.ready else '🔴 offline'}\n"
                    f"SQLite: {'🟢 ready' if self.storage.db else '🔴 closed'}"
                )

        @self.dp.message(Command("help"))
        async def cmd_help(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await message.answer(
                    "<b>КОМАНДЫ</b>\n/start — панель\n/menu — главное меню\n/status — состояние\n"
                    "/pause — общая пауза\n/resume — снять паузу\n/backup — резервная копия\n"
                    "/rules — управление триггерами\n/stats — финансы за 24 часа\n/ping — быстрая проверка\n/id — Telegram ID\n/cancel — отменить текущий ввод",
                    reply_markup=main_keyboard(),
                )

        @self.dp.message(Command("pause"))
        async def cmd_pause(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await self.storage.set_setting("paused", True)
                await self.audit(str(message.from_user.id), "pause", "global pause enabled")
                await message.answer("⏸ Все автоматические торговые действия поставлены на паузу.", reply_markup=main_keyboard())

        @self.dp.message(Command("resume"))
        async def cmd_resume(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await self.storage.set_setting("paused", False)
                await self.audit(str(message.from_user.id), "resume", "global pause disabled")
                await message.answer("▶️ Пауза снята. Проверь включённые модули через /menu.", reply_markup=main_keyboard())

        @self.dp.message(Command("backup"))
        async def cmd_backup(message: Message) -> None:
            if not self.is_admin(message.from_user.id if message.from_user else None):
                return
            path = await self.storage.backup()
            await message.answer_document(BufferedInputFile(path.read_bytes(), filename=path.name), caption="📦 Резервная копия готова")
            await self.audit(str(message.from_user.id), "backup", path.name)

        @self.dp.message(Command("stats"))
        async def cmd_stats(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await message.answer(await self.render_finance("today"), reply_markup=section_keyboard("finance"))

        @self.dp.message(Command("rules"))
        async def cmd_rules(message: Message) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await self.show_rules(message)

        @self.dp.message(Command("cancel"))
        async def cmd_cancel(message: Message, state: FSMContext) -> None:
            if self.is_admin(message.from_user.id if message.from_user else None):
                await state.clear()
                await message.answer("Ввод отменён.", reply_markup=main_keyboard())

        @self.dp.callback_query(F.data.startswith("menu:"))
        async def menu_callback(call: CallbackQuery) -> None:
            if not await self.guarded_callback(call):
                return
            section = call.data.split(":", 1)[1]
            renderers: dict[str, Callable[[], Awaitable[str]]] = {
                "home": self.render_home,
                "modules": self.render_modules,
                "replies": self.render_replies,
                "products": self.render_products,
                "orders": self.render_orders,
                "finance": lambda: self.render_finance("today"),
                "users": self.render_users,
                "tools": self.render_tools,
                "logs": self.render_logs,
            }
            renderer = renderers.get(section, self.render_home)
            await self.edit_screen(call, await renderer(), main_keyboard() if section == "home" else section_keyboard(section))
            await call.answer()

        @self.dp.callback_query(F.data.startswith("toggle:"))
        async def toggle_callback(call: CallbackQuery) -> None:
            if not await self.guarded_callback(call):
                return
            key = call.data.split(":", 1)[1]
            if key not in {"auto_delivery", "auto_responder", "auto_bump", "quiet_mode", "paused"}:
                await call.answer("Неизвестный модуль", show_alert=True)
                return
            current = bool(await self.storage.get_setting(key, False))
            new_value = not current
            if new_value and key in {"auto_delivery", "auto_responder", "auto_bump"} and not self.fp.ready:
                await call.answer("Сначала подключи FunPay: проверь golden_key и User-Agent", show_alert=True)
                return
            if new_value and key == "auto_bump":
                ids = await self.storage.get_setting("bump_allowlist", [])
                if not ids:
                    await call.answer("Сначала задай список ID лотов для поднятия", show_alert=True)
                    return
            await self.storage.set_setting(key, new_value)
            await self.audit(str(call.from_user.id), "module_toggle", f"{key}={new_value}")
            await self.edit_screen(call, await self.render_modules(), section_keyboard("modules"))
            await call.answer("Включено" if new_value else "Выключено")

        @self.dp.callback_query(F.data.startswith("preset:"))
        async def preset_callback(call: CallbackQuery) -> None:
            if not await self.guarded_callback(call):
                return
            presets = {
                "day": {"profile": "☀️ День", "paused": False, "auto_responder": True, "auto_delivery": False, "auto_bump": False},
                "night": {"profile": "🌙 Ночь", "paused": False, "auto_responder": False, "auto_delivery": False, "auto_bump": False},
                "focus": {"profile": "⚡ Фокус", "paused": False, "auto_responder": True, "auto_delivery": False, "auto_bump": False},
            }
            profile = call.data.split(":", 1)[1]
            if profile not in presets:
                await call.answer("Профиль не найден", show_alert=True)
                return
            if presets[profile]["auto_responder"] and not self.fp.ready:
                await call.answer("FunPay не подключён; подключи аккаунт перед применением", show_alert=True)
                return
            for key, val in presets[profile].items():
                await self.storage.set_setting(key, val)
            await self.audit(str(call.from_user.id), "preset", profile)
            await self.edit_screen(call, await self.render_modules(), section_keyboard("modules"))
            await call.answer("Профиль применён; автовыдача осталась выключена")

        @self.dp.callback_query(F.data.startswith("finance:"))
        async def finance_callback(call: CallbackQuery) -> None:
            if not await self.guarded_callback(call):
                return
            period = call.data.split(":", 1)[1]
            await self.edit_screen(call, await self.render_finance(period), section_keyboard("finance"))
            await call.answer()

        @self.dp.callback_query(F.data.startswith("catalog:sub:"))
        async def catalog_subcategory_callback(call: CallbackQuery) -> None:
            if not await self.guarded_callback(call):
                return
            raw_id = call.data.rsplit(":", 1)[-1]
            if not raw_id.isdigit() or not self.fp.ready:
                await call.answer("FunPay не подключён или неверный ID", show_alert=True)
                return
            try:
                lots = await self.fp.get_my_lots(int(raw_id))
                lines = [f"<b>📚 МОИ ЛОТЫ · РАЗДЕЛ {esc(raw_id)}</b>", ""]
                for lot in lots[:30]:
                    lot_id = field(lot, "id", default="?")
                    title = field(lot, "description", "title", default="Без названия")
                    price = parse_float(field(lot, "price", default=0))
                    currency = enum_text(field(lot, "currency", default="RUB"))
                    amount = field(lot, "amount", default=None)
                    quantity_text = "не указан" if amount is None else str(amount)
                    lines.append(
                        f"<b>{esc(title)}</b>\nID: <code>{esc(lot_id)}</code> · "
                        f"Цена: <b>{money(price, currency)}</b> · Количество: {esc(quantity_text)}"
                    )
                if not lots:
                    lines.append("В этом разделе лотов не найдено.")
                lines.append("\n<i>Это просмотр каталога. Редактирование не выполняется.</i>")
                await self.edit_screen(call, "\n\n".join(lines), section_keyboard("products"))
                await call.answer(f"Лотов: {len(lots)}")
            except Exception as exc:
                logger.exception("Could not load FunPay lots: {}", exc)
                await call.answer(f"Не удалось получить лоты: {str(exc)[:120]}", show_alert=True)

        @self.dp.callback_query(F.data.startswith("action:"))
        async def action_callback(call: CallbackQuery, state: FSMContext) -> None:
            if not await self.guarded_callback(call):
                return
            action = call.data.split(":", 1)[1]
            if action == "welcome":
                await self.ask_text(call, state, "welcome", "Отправь текст приветствия. Переменная: {username}", "replies")
            elif action == "delivery_template":
                await self.ask_text(call, state, "delivery_template", "Отправь шаблон выдачи. Переменные: {username}, {order_id}, {lot_title}, {count}, {price}, {items}", "replies")
            elif action == "add_rule":
                await self.ask_text(
                    call, state, "add_rule",
                    "Отправь правило одной строкой:\n<code>contains | привет | Здравствуйте, {username}! | 10 | 60</code>\n"
                    "Режимы: contains, exact, regex. Приоритет и cooldown необязательны.", "replies"
                )
            elif action == "test_rule":
                await self.ask_text(call, state, "test_rule", "Отправь тестовое сообщение покупателя — покажу, какое правило сработает, без отправки в FunPay.", "replies")
            elif action == "rules":
                await self.show_rules(call.message)
            elif action == "add_map":
                await self.ask_text(
                    call, state, "add_map",
                    "Формат привязки:\n<code>LOT_ID | stock | Название лота | себестоимость</code>\n"
                    "или\n<code>LOT_ID | random | Название лота | себестоимость</code>\n"
                    "или\n<code>LOT_ID | static | Название лота | текст выдачи</code>\n"
                    "Себестоимость можно указать 0. Для stock/random загрузи TXT отдельно.", "products"
                )
            elif action == "reprice":
                if not self.fp.ready:
                    await call.answer("Сначала подключи FunPay", show_alert=True)
                    return
                await self.ask_text(
                    call, state, "reprice",
                    "Отправь одну или несколько строк (до 10 лотов):\n"
                    "<code>LOT_ID | НОВАЯ_ЦЕНА</code>\n"
                    "Пример: <code>123456 | 150</code>\n"
                    "Сначала будет получен предпросмотр текущих и новых цен. Ничего не изменится до отдельного подтверждения.",
                    "products",
                )
            elif action == "catalog":
                if not self.fp.ready:
                    await call.answer("Сначала подключи FunPay", show_alert=True)
                    return
                try:
                    categories = await self.fp.get_categories()
                    rows: list[list[tuple[str, str]]] = []
                    lines = ["<b>📚 FUNPAY CATALOG EXPLORER</b>", "Выбери раздел, чтобы посмотреть свои лоты.", ""]
                    for category in categories:
                        category_data = obj_dict(category)
                        category_name = str(category_data.get("name") or "Категория")
                        subs = category_data.get("subcategories") or []
                        if subs:
                            lines.append(f"<b>{esc(category_name)}</b>")
                        for sub in subs:
                            sub_data = obj_dict(sub)
                            sub_id = sub_data.get("id")
                            sub_name = str(sub_data.get("name") or "Раздел")
                            if sub_id is not None and str(sub_id).isdigit():
                                rows.append([(f"{sub_name[:38]} · {sub_id}", f"catalog:sub:{sub_id}")])
                            if len(rows) >= 45:
                                break
                        if len(rows) >= 45:
                            break
                    if not rows:
                        lines.append("Разделы не найдены. Проверь доступность каталога FunPay.")
                    rows.append([("↩️ Назад", "menu:products"), ("⌂ Главное", "menu:home")])
                    await self.edit_screen(call, "\n".join(lines), ikb(rows))
                except Exception as exc:
                    logger.exception("Catalog explorer failed: {}", exc)
                    await call.answer(f"Не удалось загрузить каталог: {str(exc)[:120]}", show_alert=True)
                    return
            elif action == "stock_help":
                await call.message.answer(
                    "<b>ИМПОРТ ОСТАТКОВ</b>\n1. Подготовь TXT: одна позиция = одна строка.\n"
                    "2. Отправь файл боту с подписью <code>lot_id:123456</code>.\n"
                    "3. Пустые строки игнорируются, дубликаты одного лота пропускаются.\n"
                    "4. Файл ограничен 5 МБ. Кодировка UTF-8.", reply_markup=back_keyboard("products")
                )
            elif action == "inventory":
                await call.message.answer(await self.render_products(), reply_markup=section_keyboard("products"))
            elif action == "maps":
                maps = await self.storage.lot_maps()
                text = "<b>📄 ПРИВЯЗКИ ЛОТОВ</b>\n" + ("\n".join(
                    f"<code>{esc(m['lot_key'])}</code> · {esc(m['lot_title'])} · {esc(m['mode'])}" for m in maps
                ) if maps else "Пока нет привязок.")
                await call.message.answer(text, reply_markup=section_keyboard("products"))
            elif action == "bump_config":
                await self.ask_text(call, state, "bump_config", "Отправь ID лотов через запятую для автоподнятия. Будут использоваться только эти ID.", "products")
            elif action == "refresh_orders":
                if not self.fp.ready:
                    await call.answer("FunPay не подключён", show_alert=True)
                    return
                else:
                    await self.sync_orders_once()
                    await self.edit_screen(call, await self.render_orders(), section_keyboard("orders"))
            elif action == "orders_list":
                await call.message.answer(await self.render_orders(), reply_markup=section_keyboard("orders"))
            elif action == "uncertain":
                text, markup = await self.render_uncertain()
                await self.edit_screen(call, text, markup)
            elif action == "balance":
                if not self.fp.ready:
                    await call.answer("FunPay не подключён", show_alert=True)
                    return
                else:
                    try:
                        balance = await self.fp.get_balance()
                        await call.message.answer(f"<b>💳 FUNPAY BALANCE</b>\n<code>{esc(obj_dict(balance) or balance)}</code>", reply_markup=section_keyboard("finance"))
                    except Exception as exc:
                        await call.answer(f"Ошибка баланса: {str(exc)[:150]}", show_alert=True)
                        return
            elif action == "list:remove":
                await self.ask_text(call, state, "remove_user", "Отправь тип и username: <code>black | username</code> / white / vip.", "users")
            elif action.startswith("list:"):
                kind = action.split(":", 1)[1]
                if kind not in {"black", "white", "vip"}:
                    await call.answer("Неизвестный список", show_alert=True)
                    return
                await self.ask_text(call, state, f"add_user:{kind}", "Отправь username FunPay без @. Можно добавить заметку через | заметка.", "users")
            elif action == "lists_show":
                await call.message.answer(await self.render_users(), reply_markup=section_keyboard("users"))
            elif action == "backup":
                path = await self.storage.backup()
                await call.message.answer_document(BufferedInputFile(path.read_bytes(), filename=path.name), caption="📦 Бэкап создан")
                await self.audit(str(call.from_user.id), "backup", path.name)
            elif action == "health":
                health = await self.health_text()
                await call.message.answer(health, reply_markup=section_keyboard("tools"))
            elif action == "commission":
                current = await self.storage.get_setting("commission_percent", 0)
                await self.ask_text(call, state, "commission", f"Текущая расчётная комиссия: {current}%. Отправь новое число от 0 до 50.", "tools")
            elif action == "low_stock":
                current = await self.storage.get_setting("low_stock_threshold", 3)
                await self.ask_text(call, state, "low_stock", f"Текущий порог уведомления: {current} шт. Отправь целое число от 0 до 100.", "tools")
            elif action == "bump_interval":
                current = await self.storage.get_setting("bump_interval", 3600)
                await self.ask_text(call, state, "bump_interval", f"Текущий интервал: {current} сек. Отправь новый интервал от 1800 до 604800 секунд.", "tools")
            elif action == "audit":
                await call.message.answer(await self.render_logs(), reply_markup=section_keyboard("logs"))
            elif action == "cancel_fsm":
                await state.clear()
            elif action == "help":
                await call.message.answer("/start · /status · /pause · /resume · /backup · /stats · /rules · /cancel", reply_markup=section_keyboard("tools"))
            await call.answer()

        @self.dp.callback_query(F.data.startswith("rule:"))
        async def rule_callback(call: CallbackQuery) -> None:
            if not await self.guarded_callback(call):
                return
            parts = call.data.split(":")
            if len(parts) != 3 or not parts[2].isdigit():
                await call.answer("Некорректное правило", show_alert=True)
                return
            action, rule_id = parts[1], int(parts[2])
            if action == "toggle":
                enabled = await self.storage.toggle_rule(rule_id)
                await self.audit(str(call.from_user.id), "rule_toggle", f"id={rule_id};enabled={enabled}")
            elif action == "delete":
                await self.storage.delete_rule(rule_id)
                await self.audit(str(call.from_user.id), "rule_delete", f"id={rule_id}")
            await self.show_rules(call.message, edit=True)
            await call.answer("Готово")

        @self.dp.callback_query(F.data.startswith("lotchange:"))
        async def lot_change_callback(call: CallbackQuery, state: FSMContext) -> None:
            if not await self.guarded_callback(call):
                return
            action = call.data.split(":", 1)[1]
            data = await state.get_data()
            changes = data.get("pending_lot_changes") or []
            if action == "cancel":
                await state.clear()
                await call.answer("Изменения отменены")
                await call.message.answer("🛑 Репрайсинг отменён. Лоты не изменены.", reply_markup=section_keyboard("products"))
                return
            if action != "confirm" or not changes:
                await call.answer("Нет ожидающих изменений", show_alert=True)
                return
            if not self.fp.ready:
                await call.answer("FunPay-сессия недоступна. Изменения не отправлены.", show_alert=True)
                return
            results: list[str] = []
            for item in changes:
                try:
                    result = await self.fp.save_lot(int(item["lot_id"]), item["fields"])
                    saved = bool(field(result, "saved", default=False))
                    error = str(field(result, "error", default="") or "")
                    results.append(
                        f"{'✅' if saved else '❌'} <code>{esc(item['lot_id'])}</code>: "
                        f"{money(float(item['new_price']), item.get('currency',''))}"
                        + (f" · {esc(error[:140])}" if error else "")
                    )
                    await self.audit(str(call.from_user.id), "lot_reprice", f"lot={item['lot_id']};old={item['old_price']};new={item['new_price']};saved={saved}")
                except Exception as exc:
                    results.append(f"❌ <code>{esc(item['lot_id'])}</code>: {esc(str(exc)[:160])}")
            await state.clear()
            await call.answer("Сохранение завершено")
            await call.message.answer("<b>💰 РЕЗУЛЬТАТ РЕПРАЙСИНГА</b>\n\n" + "\n".join(results), reply_markup=section_keyboard("products"))

        @self.dp.callback_query(F.data.startswith("delivery:"))
        async def delivery_callback(call: CallbackQuery) -> None:
            if not await self.guarded_callback(call):
                return
            parts = call.data.split(":", 2)
            action = parts[1] if len(parts) > 1 else ""
            order_id = parts[2] if len(parts) > 2 else ""
            if not order_id:
                await call.answer("Нет ID заказа", show_alert=True)
                return
            if action == "sent":
                row = await self.storage.delivery(order_id)
                order_rows = await self.storage.recent_orders(100)
                order = next((x for x in order_rows if str(x["order_id"]) == order_id), None)
                if not row or not order:
                    await call.answer("Запись не найдена", show_alert=True)
                    return
                if row["state"] not in {"uncertain", "prepared"}:
                    await call.answer("Статус уже изменён", show_alert=True)
                    return
                mapping = await self.storage.get_lot_map(str(row["lot_key"]))
                payload = mapping["payload"] if mapping else {}
                unit_cost = parse_float(payload.get("unit_cost", 0))
                quantity = len(row.get("items", [])) or 1
                revenue = float(order["price"] or 0)
                cost = unit_cost * quantity
                commission = max(0.0, parse_float(await self.storage.get_setting("commission_percent", 0)))
                fee = revenue * commission / 100
                await self.storage.confirm_delivery(
                    order_id, revenue, cost, fee, str(order["currency"] or ""),
                )
                await self.audit(str(call.from_user.id), "delivery_manual_confirm", order_id)
                await call.answer("Отмечено как выданное")
            elif action == "reset":
                await call.message.answer(
                    f"⚠️ <b>Подтверди сброс выдачи #{esc(order_id)}</b>\n\n"
                    "Это освободит зарезервированные позиции. Продолжай, только если вручную проверил чат и уверен, что товар покупателю НЕ отправлен.",
                    reply_markup=ikb([[("✅ Я проверил, товара не было", f"delivery:reset_confirm:{order_id}")], [("Отмена", "menu:orders")]])
                )
                await call.answer()
            elif action == "reset_confirm":
                ok = await self.storage.reset_delivery_after_manual_check(order_id, verified_not_sent=True)
                await self.audit(str(call.from_user.id), "delivery_manual_reset", f"{order_id};verified={ok}")
                await call.answer("Резерв снят" if ok else "Сброс невозможен", show_alert=not ok)
            text, markup = await self.render_uncertain()
            await self.edit_screen(call, text, markup)

        @self.dp.message(InputFlow.waiting_text)
        async def receive_text(message: Message, state: FSMContext) -> None:
            if not self.is_admin(message.from_user.id if message.from_user else None):
                return
            raw = (message.text or "").strip()
            if not raw:
                await message.answer("Нужен текст. Отправь текст или /cancel.")
                return
            data = await state.get_data()
            action = str(data.get("action", ""))
            try:
                if action in {"welcome", "delivery_template"}:
                    if len(raw) > 3000:
                        raise ValueError("Максимальная длина — 3000 символов")
                    setting = "welcome_text" if action == "welcome" else "delivery_template"
                    await self.storage.set_setting(setting, raw)
                    await self.audit(str(message.from_user.id), "text_template_update", setting)
                    await message.answer("✅ Шаблон сохранён.", reply_markup=section_keyboard("replies"))
                elif action == "add_rule":
                    parts = [x.strip() for x in raw.split("|", 4)]
                    if len(parts) < 3:
                        raise ValueError("Нужно минимум три поля: режим | условие | ответ")
                    mode, pattern, reply = parts[:3]
                    priority = int(parts[3]) if len(parts) >= 4 and parts[3] else (20 if mode == "exact" else 10)
                    cooldown = int(parts[4]) if len(parts) >= 5 and parts[4] else 60
                    rule_id = await self.storage.add_rule(pattern, mode, reply, priority, cooldown)
                    await self.audit(str(message.from_user.id), "rule_add", f"id={rule_id};mode={mode};pattern={pattern[:120]}")
                    await message.answer(f"✅ Правило #{rule_id} сохранено.", reply_markup=section_keyboard("replies"))
                elif action.startswith("add_user:"):
                    kind = action.split(":", 1)[1]
                    username, _, note = raw.partition("|")
                    username = username.strip().lstrip("@")
                    if not re.fullmatch(r"[A-Za-z0-9_.-]{2,64}", username):
                        raise ValueError("Некорректный username")
                    await self.storage.set_user(kind, username, note.strip())
                    await self.audit(str(message.from_user.id), "user_list_add", f"{kind}:{username}")
                    await message.answer(f"✅ {esc(username)} добавлен в список {esc(kind)}.", reply_markup=section_keyboard("users"))
                elif action == "remove_user":
                    parts = [x.strip() for x in raw.split("|", 1)]
                    if len(parts) != 2 or parts[0] not in {"black", "white", "vip"}:
                        raise ValueError("Формат: black | username")
                    await self.storage.remove_user(parts[0], parts[1])
                    await self.audit(str(message.from_user.id), "user_list_remove", raw)
                    await message.answer("✅ Пользователь удалён, если он был в списке.", reply_markup=section_keyboard("users"))
                elif action == "add_map":
                    parts = [x.strip() for x in raw.split("|", 3)]
                    if len(parts) < 3 or not parts[0].isdigit():
                        raise ValueError("Формат: LOT_ID | stock/static | название | себестоимость/текст")
                    lot_id, mode, title = parts[:3]
                    if mode not in {"stock", "static", "random"}:
                        raise ValueError("Поддерживаются режимы stock, random и static")
                    payload: dict[str, Any] = {"unit_cost": 0.0}
                    if mode == "static":
                        if len(parts) < 4 or not parts[3]:
                            raise ValueError("Для static укажи текст выдачи в четвёртом поле")
                        payload["message"] = parts[3]
                    elif len(parts) >= 4 and parts[3]:
                        payload["unit_cost"] = parse_float(parts[3], -1)
                        if payload["unit_cost"] < 0:
                            raise ValueError("Себестоимость должна быть числом >= 0")
                    await self.storage.map_lot(lot_id, title, mode, payload)
                    await self.audit(str(message.from_user.id), "lot_map", f"{lot_id}:{mode}:{title}")
                    await message.answer(f"✅ Лот <code>{lot_id}</code> привязан ({mode}).", reply_markup=section_keyboard("products"))
                elif action == "bump_config":
                    ids = sorted({int(x.strip()) for x in re.split(r"[,;\s]+", raw) if x.strip()})
                    if not ids or len(ids) > 50:
                        raise ValueError("Укажи от 1 до 50 числовых ID")
                    await self.storage.set_setting("bump_allowlist", ids)
                    await self.audit(str(message.from_user.id), "bump_allowlist", ",".join(map(str, ids)))
                    await message.answer(f"✅ Список автоподнятия сохранён: {len(ids)} лотов.", reply_markup=section_keyboard("products"))
                elif action in {"commission", "low_stock", "bump_interval"}:
                    number = parse_float(raw, -1)
                    if number < 0:
                        raise ValueError("Нужно указать неотрицательное число")
                    if action == "commission":
                        if number > 50:
                            raise ValueError("Комиссия должна быть от 0 до 50 процентов")
                        await self.storage.set_setting("commission_percent", number)
                    elif action == "low_stock":
                        if not number.is_integer() or number > 100:
                            raise ValueError("Порог должен быть целым числом от 0 до 100")
                        await self.storage.set_setting("low_stock_threshold", int(number))
                    else:
                        if not number.is_integer() or not 1800 <= number <= 604800:
                            raise ValueError("Интервал должен быть целым числом от 1800 до 604800 секунд")
                        await self.storage.set_setting("bump_interval", int(number))
                    await self.audit(str(message.from_user.id), "numeric_setting", f"{action}={number}")
                    await message.answer("✅ Настройка сохранена.", reply_markup=section_keyboard("tools"))
                elif action == "reprice":
                    if not self.fp.ready:
                        raise ValueError("FunPay не подключён. Репрайсинг недоступен.")
                    lines = [line.strip() for line in raw.splitlines() if line.strip()]
                    if not lines or len(lines) > 10:
                        raise ValueError("Нужно указать от 1 до 10 строк LOT_ID | НОВАЯ_ЦЕНА")
                    pending: list[dict[str, Any]] = []
                    seen_ids: set[str] = set()
                    commission = max(0.0, parse_float(await self.storage.get_setting("commission_percent", 0)))
                    for line in lines:
                        parts = [part.strip() for part in line.split("|", 1)]
                        if len(parts) != 2 or not parts[0].isdigit():
                            raise ValueError(f"Неверная строка: {line}. Формат LOT_ID | НОВАЯ_ЦЕНА")
                        lot_id, price_raw = parts
                        if lot_id in seen_ids:
                            raise ValueError(f"ID {lot_id} указан дважды")
                        seen_ids.add(lot_id)
                        new_price = parse_float(price_raw, -1)
                        if new_price <= 0:
                            raise ValueError(f"Цена для лота {lot_id} должна быть больше нуля")
                        fields = await self.fp.get_lot_fields(int(lot_id))
                        price_key = next((key for key in fields if str(key).casefold() == "price" or str(key).casefold().endswith("[price]")), None)
                        if not price_key:
                            names = ", ".join(str(key) for key in list(fields)[:18])
                            raise ValueError(f"В форме лота {lot_id} не найдено поле price. Доступные поля: {names or 'нет'}")
                        old_price = parse_float(fields.get(price_key), -1)
                        if old_price < 0:
                            raise ValueError(f"Не удалось прочитать текущую цену лота {lot_id}")
                        mapping = await self.storage.get_lot_map(lot_id)
                        unit_cost = parse_float((mapping or {}).get("payload", {}).get("unit_cost", 0))
                        minimum = (unit_cost / (1 - commission / 100)) if unit_cost > 0 and commission < 100 else 0
                        if minimum and new_price + 1e-9 < minimum:
                            raise ValueError(
                                f"Цена лота {lot_id} ({new_price}) ниже расчётного минимума {minimum:.2f} "
                                f"с учётом себестоимости {unit_cost:.2f} и комиссии {commission:.2f}%"
                            )
                        fields[price_key] = str(price_raw.replace(",", "."))
                        currency = enum_text(field(mapping or {}, "currency", default=""))
                        pending.append({
                            "lot_id": lot_id, "old_price": old_price, "new_price": new_price,
                            "currency": currency, "fields": fields,
                        })
                    await state.update_data(pending_lot_changes=pending)
                    await state.set_state(InputFlow.confirm_lot_changes)
                    preview = ["<b>💰 ПРЕДПРОСМОТР РЕПРАЙСИНГА</b>", "Ни один лот ещё не изменён.", ""]
                    for item in pending:
                        preview.append(
                            f"Лот <code>{esc(item['lot_id'])}</code>\n"
                            f"{money(item['old_price'], item['currency'])} → <b>{money(item['new_price'], item['currency'])}</b>"
                        )
                    preview.append("Подтверди действие только если цены и ID проверены.")
                    await message.answer(
                        "\n".join(preview),
                        reply_markup=ikb([[ ("✅ Подтвердить изменение", "lotchange:confirm") ], [ ("🛑 Отмена", "lotchange:cancel") ]]),
                    )
                elif action == "test_rule":
                    result = await self.match_rule(raw)
                    if result:
                        await message.answer(
                            f"✅ Сработало правило #{result['id']} · {esc(result['mode'])}\n"
                            f"<b>Предпросмотр ответа:</b>\n{esc(safe_format(result['reply'], username='Покупатель', order_id='123', lot_title='Тестовый лот', count=1, price='100', items='ТЕСТОВЫЙ ТОВАР'))}",
                            reply_markup=section_keyboard("replies"),
                        )
                    else:
                        await message.answer("Совпадений нет. Ни одно активное правило не подошло.", reply_markup=section_keyboard("replies"))
                else:
                    raise ValueError("Неизвестный режим ввода. Отправь /cancel и начни заново.")
                await state.clear()
            except (ValueError, re.error) as exc:
                await message.answer(f"⚠️ {esc(exc)}\nПопробуй ещё раз или отправь /cancel.")

        @self.dp.message(F.document)
        async def receive_document(message: Message) -> None:
            if not self.is_admin(message.from_user.id if message.from_user else None):
                return
            doc = message.document
            if not doc.file_name or not doc.file_name.lower().endswith(".txt"):
                await message.answer("Принимаю только TXT-файлы.")
                return
            if doc.file_size and doc.file_size > CFG.max_upload_bytes:
                await message.answer(f"Файл больше {CFG.max_upload_bytes // 1024 // 1024} МБ — загрузка отклонена.")
                return
            match = re.search(r"lot_id\s*:\s*(\d+)", message.caption or "", re.I)
            if not match:
                await message.answer("Добавь подпись к файлу: <code>lot_id:123456</code>. Тогда строки будут импортированы в остатки.")
                return
            lot_id = match.group(1)
            mapping = await self.storage.get_lot_map(lot_id)
            if not mapping:
                await message.answer(f"Сначала создай привязку лота {lot_id} в разделе Товары.")
                return
            if mapping["mode"] != "stock":
                await message.answer("Этот лот настроен как static. Импорт остатков для него не требуется.")
                return
            file = await self.bot.get_file(doc.file_id)
            buffer = BytesIO()
            await self.bot.download_file(file.file_path, buffer)
            try:
                content = buffer.getvalue().decode("utf-8-sig", errors="strict")
            except UnicodeDecodeError:
                await message.answer("Файл не UTF-8. Сохрани TXT в кодировке UTF-8 и отправь снова.")
                return
            lines = [line.strip() for line in content.splitlines() if line.strip()]
            if not lines:
                await message.answer("В файле нет товарных строк.")
                return
            source_name = safe_filename(doc.file_name)
            inserted, duplicates = await self.storage.add_stock(lot_id, lines, source_name)
            # Keep the original TXT alongside the DB inventory so the next ZIP backup
            # contains both the imported rows and the original supplier file.
            products_dir = Path(CFG.products_dir)
            products_dir.mkdir(parents=True, exist_ok=True)
            archived_name = f"lot_{lot_id}_{int(time.time())}_{source_name}"
            await asyncio.to_thread((products_dir / archived_name).write_bytes, buffer.getvalue())
            await self.audit(str(message.from_user.id), "stock_import", f"lot={lot_id};added={inserted};duplicates={duplicates};file={archived_name}")
            await message.answer(
                f"<b>📦 Импорт завершён</b>\nЛот: <code>{lot_id}</code>\nДобавлено: <b>{inserted}</b>\nДубликаты: <b>{duplicates}</b>\nТекущий остаток: <b>{await self.storage.stock_count(lot_id)}</b>",
                reply_markup=section_keyboard("products"),
            )

        @self.dp.message(F.reply_to_message)
        async def telegram_reply_to_buyer(message: Message) -> None:
            if not self.is_admin(message.from_user.id if message.from_user else None):
                return
            source_text = message.reply_to_message.text or message.reply_to_message.caption or ""
            marker = re.search(r"CHAT_ID=([A-Za-z0-9_-]+)", source_text)
            if not marker:
                return
            if not self.fp.ready:
                await message.reply("FunPay не подключён. Ответ не отправлен.")
                return
            text = (message.text or "").strip()
            if not text:
                await message.reply("Отправь текстовый ответ.")
                return
            try:
                await self.fp.send_message(marker.group(1), text)
                await self.audit(str(message.from_user.id), "manual_buyer_reply", marker.group(1))
                await message.reply("✅ Ответ отправлен в FunPay.")
            except Exception as exc:
                await message.reply(f"⚠️ Результат отправки не подтверждён: {esc(str(exc)[:300])}")

    async def show_rules(self, message: Message, edit: bool = False) -> None:
        rules = await self.storage.rules(include_disabled=True)
        lines = ["<b>🧩 REPLY RULE MANAGER</b>", "Формат: режим | условие | ответ | приоритет | cooldown", ""]
        rows: list[list[tuple[str, str]]] = []
        for rule in rules[:25]:
            status = "🟢" if rule["enabled"] else "🔴"
            lines.append(f"{status} <b>#{rule['id']}</b> · <code>{esc(rule['mode'])}</code> · {esc(rule['pattern'][:80])}")
            rows.append([(f"{status} #{rule['id']}", f"rule:toggle:{rule['id']}"), ("🗑", f"rule:delete:{rule['id']}")])
        if not rules:
            lines.append("Правил пока нет.")
        rows.append([("➕ Добавить", "action:add_rule"), ("↩️ Назад", "menu:replies")])
        markup = ikb(rows)
        if edit:
            try:
                await message.edit_text("\n".join(lines), reply_markup=markup)
            except Exception:
                await message.answer("\n".join(lines), reply_markup=markup)
        else:
            await message.answer("\n".join(lines), reply_markup=markup)

    # === RULE ENGINE ===
    async def match_rule(self, text: str) -> dict[str, Any] | None:
        body = text.strip()
        for rule in await self.storage.rules():
            pattern = str(rule["pattern"])
            matched = False
            if rule["mode"] == "exact":
                matched = body.casefold() == pattern.casefold()
            elif rule["mode"] == "contains":
                matched = pattern.casefold() in body.casefold()
            elif rule["mode"] == "regex":
                try:
                    import regex as safe_regex
                    matched = safe_regex.search(pattern, body[:4000], safe_regex.IGNORECASE, timeout=0.025) is not None
                except ImportError:
                    logger.warning("The 'regex' package is missing; regex rules are skipped")
                    matched = False
                except TimeoutError:
                    logger.warning("Regex rule timed out; rule_id={}", rule["id"])
                    matched = False
                except Exception:
                    matched = False
            if matched:
                return rule
        return None

    async def process_responder(self, chat_id: str, username: str, text: str) -> None:
        if not await self.enabled("auto_responder") or not text.strip():
            return
        if await self.storage.user_in("black", username):
            return
        allowlist = await self.storage.list_users("white")
        if allowlist and not await self.storage.user_in("white", username):
            return
        if username and username.casefold() not in {"unknown", "покупатель"} and await self.storage.first_contact(username):
            welcome = str(await self.storage.get_setting("welcome_text", ""))
            if welcome and await self.storage.cooldown_ready("welcome", username.casefold(), 86400):
                try:
                    await self.fp.send_message(chat_id, safe_format(welcome, username=username))
                except Exception as exc:
                    logger.warning("Welcome send uncertain for {}: {}", username, exc)
                    await self.notify(f"⚠️ Не удалось подтвердить отправку приветствия для {esc(username)}.", important=True)
                # A welcome and a trigger on the same first message can look spammy.
                return
        rule = await self.match_rule(text)
        if not rule:
            return
        ready = await self.storage.cooldown_ready("rule", f"{username.casefold()}:{rule['id']}", int(rule["cooldown"]))
        if not ready:
            return
        reply = safe_format(rule["reply"], username=username)
        try:
            await self.fp.send_message(chat_id, reply)
            await self.audit("system", "auto_reply", f"user={username};rule={rule['id']}")
        except Exception as exc:
            await self.notify(f"⚠️ Автоответ для {esc(username)}: результат отправки не подтверждён. {esc(str(exc)[:250])}", important=True)

    # === ORDER NORMALIZATION AND PROCESSING ===
    def normalize_sale(self, sale: Any) -> dict[str, Any]:
        order_id = enum_text(field(sale, "id", "order_id", "orderId", default=""))
        buyer_obj = field(sale, "buyer", "buyer_username", "buyerUsername", default="")
        buyer = nested_username(buyer_obj)
        title = enum_text(field(sale, "description", "lot_title", "title", "name", default=""))
        status = enum_text(field(sale, "status", "state", default=""))
        price = parse_float(field(sale, "price", "amount", default=0))
        currency = enum_text(field(sale, "currency", default=""))
        lot_id = enum_text(field(sale, "lot_id", "offer_id", "lotId", "offerId", default=""))
        quantity = int(parse_float(field(sale, "quantity", "count", "amount_count", default=1), 1))
        chat_id = enum_text(field(sale, "chat_id", "chatId", default=""))
        created_at = parse_timestamp(field(sale, "created_at", "created", "date", "timestamp", default=None))
        return {
            "order_id": order_id, "buyer": buyer, "lot_title": title,
            "status": status, "price": price, "currency": currency,
            "lot_id": lot_id, "quantity": max(1, min(quantity, 100)), "chat_id": chat_id,
            "created_at": created_at,
        }

    async def find_chat_id(self, buyer: str, preferred: str = "") -> str:
        if preferred:
            return preferred
        if not buyer:
            return ""
        for chat in await self.fp.get_chats():
            chat_id = enum_text(field(chat, "id", "chat_id", default=""))
            name = nested_username(field(chat, "username", "interlocutor_username", "buyer_username", "name", "user", default=""))
            if chat_id and name.casefold() == buyer.casefold():
                return chat_id
        return ""

    async def sync_orders_once(self) -> None:
        if not self.fp.ready:
            return
        sales = await self.fp.get_sales()
        bootstrapping = not bool(await self.storage.get_setting("orders_bootstrapped", False))
        for sale in sales:
            order = self.normalize_sale(sale)
            order_id = order["order_id"]
            if not order_id:
                logger.warning("Skipped FunPay sale without order ID")
                continue
            await self.storage.save_order(order)
            if bootstrapping:
                # Establish a conservative baseline on the first successful sync.
                # Date strings from marketplace HTML may be locale/timezone dependent;
                # never infer that an existing paid order is new from an ambiguous date.
                # Orders appearing after this baseline are handled on subsequent polls.
                await self.storage.mark_once("preexisting_order", order_id)
                continue
            elif await self.storage.is_marked("preexisting_order", order_id):
                continue
            if not is_paid_status(order["status"]):
                continue
            if await self.storage.order_delivered(order_id):
                continue
            if not await self.enabled("auto_delivery"):
                continue
            buyer = order["buyer"]
            if buyer and await self.storage.user_in("black", buyer):
                await self.notify(f"⛔ Заказ <code>#{esc(order_id)}</code> от пользователя из Blacklist: {esc(buyer)}", important=True)
                continue
            mapping = await self.storage.find_lot_map(order["lot_id"], order["lot_title"])
            if not mapping:
                continue
            payload = mapping["payload"]
            if mapping["mode"] == "static":
                # source is the legacy key used by the first project revision.
                static_text = str(payload.get("message") or payload.get("source") or "").strip()
                prepared = await self.storage.prepare_delivery(
                    order_id, mapping["lot_key"], order["quantity"], static_items=[static_text] * order["quantity"]
                )
            else:
                prepared = await self.storage.prepare_delivery(
                    order_id, mapping["lot_key"], order["quantity"], randomize=(mapping["mode"] == "random")
                )
            if prepared["state"] == "out_of_stock":
                await self.notify(
                    f"🚨 <b>НЕ ХВАТАЕТ ТОВАРА</b>\nЗаказ: <code>#{esc(order_id)}</code>\n"
                    f"Лот: <code>{esc(mapping['lot_key'])}</code>\nНужно: {order['quantity']}\n"
                    f"Остаток: {await self.storage.stock_count(mapping['lot_key'])}", important=True
                )
                continue
            if prepared["state"] != "prepared" or prepared.get("existing"):
                # Prepared/uncertain/existing records are never auto-sent a second time.
                continue
            if any(not str(item).strip() for item in prepared["items"]):
                await self.storage.set_delivery_state(order_id, "uncertain", "Empty product item in prepared delivery")
                await self.notify(f"⚠️ Пустая позиция в заказе #{esc(order_id)}. Нужна ручная проверка.", important=True)
                continue
            chat_id = await self.find_chat_id(buyer, order["chat_id"])
            if not chat_id:
                await self.storage.set_delivery_state(order_id, "uncertain", "Cannot resolve buyer chat ID")
                await self.notify(f"⚠️ Не найден чат покупателя для заказа #{esc(order_id)}. Товар зарезервирован, повтор запрещён.", important=True)
                continue
            item_text = "\n\n".join(f"{i + 1}. {item}" for i, item in enumerate(prepared["items"]))
            delivery_template = str(await self.storage.get_setting("delivery_template", "{items}"))
            message_text = safe_format(
                delivery_template, username=buyer, order_id=order_id,
                lot_title=order["lot_title"], count=order["quantity"],
                price=order["price"], items=item_text,
            )
            unit_cost = parse_float(payload.get("unit_cost", 0))
            total_cost = unit_cost * order["quantity"]
            commission = max(0.0, parse_float(await self.storage.get_setting("commission_percent", 0)))
            fee = order["price"] * commission / 100
            try:
                await self.fp.send_message(chat_id, message_text)
                await self.storage.confirm_delivery(order_id, order["price"], total_cost, fee, order["currency"])
                await self.audit("system", "auto_delivery", f"order={order_id};qty={order['quantity']};lot={mapping['lot_key']}")
                await self.notify(
                    f"✅ <b>ЗАКАЗ ВЫДАН</b>\nЗаказ: <code>#{esc(order_id)}</code>\n"
                    f"Покупатель: {esc(buyer or '—')}\nЛот: {esc(order['lot_title'])}\n"
                    f"Позиций: {order['quantity']}\nВыручка: {money(order['price'], order['currency'])}",
                    important=True,
                )
                remaining = await self.storage.stock_count(mapping["lot_key"]) if mapping["mode"] != "static" else -1
                threshold = int(await self.storage.get_setting("low_stock_threshold", 3))
                if remaining >= 0 and threshold > 0 and remaining <= threshold:
                    await self.notify(
                        f"📉 <b>НИЗКИЙ ОСТАТОК</b>\nЛот: <code>{esc(mapping['lot_key'])}</code>\nДоступно: <b>{remaining}</b>",
                        important=True,
                    )
            except Exception as exc:
                await self.storage.set_delivery_state(order_id, "uncertain", str(exc))
                await self.notify(
                    f"🚨 <b>ОТПРАВКА НЕ ПОДТВЕРЖДЕНА</b>\nЗаказ: <code>#{esc(order_id)}</code>\n"
                    f"Товар зарезервирован. Автоповтор заблокирован. Проверь чат вручную.\n{esc(str(exc)[:300])}",
                    important=True,
                )
        if bootstrapping:
            await self.storage.set_setting("orders_bootstrapped", True)
            await self.notify(
                "🛡 Первичная синхронизация заказов завершена. Уже существующие сделки добавлены в историю без автоматической выдачи; новые заказы будут обрабатываться дальше.",
                important=True,
            )

    # === CHAT POLLING / LIVE TELEGRAM MIRROR ===
    async def sync_chats_once(self) -> None:
        if not self.fp.ready:
            return
        chats = await self.fp.get_chats()
        for chat in chats:
            chat_id = enum_text(field(chat, "id", "chat_id", default=""))
            username = nested_username(field(chat, "username", "interlocutor_username", "buyer_username", "name", "user", default=""))
            if not chat_id:
                continue
            cursor = await self.storage.get_chat_cursor(chat_id)
            history = await self.fp.get_chat_history(chat_id, cursor)
            # Client returns chronological history; cursor advances after each item.
            for msg in history:
                message_id = enum_text(field(msg, "id", "message_id", "node_id", default=""))
                text = enum_text(field(msg, "text", "message", "content", default=""))
                author_obj = field(msg, "author", "sender", "username", default="")
                author = nested_username(author_obj) or username
                timestamp = parse_timestamp(field(msg, "timestamp", "created_at", "date", "time", default=None))
                incoming_value = field(msg, "is_incoming", default=None)
                is_mine_value = field(msg, "is_my", "is_mine", "outgoing", "is_outgoing", default=None)
                if incoming_value is not None:
                    is_mine = not bool(incoming_value)
                elif is_mine_value is not None:
                    is_mine = bool(is_mine_value)
                else:
                    is_mine = bool(self.me_username and author.casefold() == self.me_username.casefold())
                if not message_id:
                    digest = hashlib.sha256(f"{chat_id}|{author}|{text}|{timestamp}".encode("utf-8")).hexdigest()[:32]
                    message_id = f"fallback-{digest}"
                event_key = f"{chat_id}:{message_id}"
                if not await self.storage.mark_once("message", event_key):
                    if message_id:
                        await self.storage.set_chat_cursor(chat_id, message_id)
                    continue
                await self.storage.set_chat_cursor(chat_id, message_id)
                if is_mine or not text.strip():
                    continue
                # On a brand-new chat, suppress old history to prevent old greetings after restart.
                if cursor is None and (timestamp is None or timestamp < self.started_at):
                    continue
                if not author:
                    author = username or "unknown"
                count, should_alert = await self.storage.record_user_message(
                    author,
                    int(await self.storage.get_setting("spam_window", 60)),
                    int(await self.storage.get_setting("spam_limit", 8)),
                )
                vip = await self.storage.user_in("vip", author)
                black = await self.storage.user_in("black", author)
                if not black:
                    await self.notify(
                        f"{'⭐ VIP · ' if vip else ''}💬 <b>{esc(author)}</b>\n{esc(text[:2400])}\n"
                        f"<code>CHAT_ID={esc(chat_id)}</code>",
                        important=vip,
                    )
                if should_alert:
                    await self.notify(f"🚨 Возможный спам: {esc(author)} — {count} сообщений за окно", important=True)
                if not black and count <= int(await self.storage.get_setting("spam_limit", 8)):
                    await self.process_responder(chat_id, author, text)

    # === BACKGROUND WORKERS / WATCHDOG ===
    async def supervised_worker(
        self, name: str, interval: int, operation: Callable[[], Awaitable[None]],
    ) -> None:
        delay = interval
        while not self.stop_event.is_set():
            self.heartbeats[name] = time.monotonic()
            try:
                await operation()
                self.failures[name] = 0
                delay = interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.failures[name] = self.failures.get(name, 0) + 1
                delay = min(300, max(interval, 2 ** min(self.failures[name], 8)))
                logger.exception("Worker {} failed ({}): {}", name, self.failures[name], exc)
                if self.failures[name] in {1, 3, 6}:
                    await self.notify(
                        f"⚠️ <b>Сбой процесса {esc(name)}</b>\nПопытка: {self.failures[name]}\n{esc(str(exc)[:350])}",
                        important=True,
                    )
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def order_operation(self) -> None:
        if self.fp.ready:
            await self.sync_orders_once()

    async def chat_operation(self) -> None:
        if self.fp.ready:
            await self.sync_chats_once()

    async def bump_operation(self) -> None:
        if not await self.enabled("auto_bump") or not self.fp.ready:
            return
        interval = max(1800, int(await self.storage.get_setting("bump_interval", 3600)))
        last = float(await self.storage.get_setting("last_bump_at", 0))
        if time.time() - last < interval:
            return
        ids = await self.storage.get_setting("bump_allowlist", [])
        clean_ids = [int(x) for x in ids if str(x).isdigit()][:50]
        if not clean_ids:
            return
        result = await self.fp.raise_lots(clean_ids)
        if isinstance(result, dict) and result.get("error"):
            raise RuntimeError(f"FunPay did not confirm lot bump: {result['error']}")
        await self.storage.set_setting("last_bump_at", time.time())
        await self.audit("system", "lots_bumped", f"count={len(clean_ids)}")
        await self.notify(f"🚀 Запрос на поднятие отправлен для {len(clean_ids)} разрешённых лотов.")

    async def session_operation(self) -> None:
        if not self.fp.ready:
            if CFG.golden_key and time.time() - self.last_connect_attempt >= CFG.session_interval:
                self.last_connect_attempt = time.time()
                try:
                    await self.fp.connect()
                    self.me_username = nested_username(field(self.fp.me, "username", default=""))
                    await self.notify(f"🟢 FunPay-сессия восстановлена: {esc(self.me_username)}", important=True)
                except Exception as exc:
                    if time.time() - self.last_connect_alert >= 600:
                        self.last_connect_alert = time.time()
                        await self.notify(f"🔴 FunPay offline: {esc(str(exc)[:300])}", important=True)
            return
        try:
            me = await self.fp.get_me()
            self.me_username = nested_username(field(me, "username", default=self.me_username)) or self.me_username
        except PermissionError:
            await self.fp.close()
            await self.notify("🔒 FunPay отклонил сессию. Проверь golden_key и обнови его в окружении.", important=True)
            raise

    async def backup_operation(self) -> None:
        path = await self.storage.backup()
        for admin_id in CFG.admins:
            try:
                await self.bot.send_document(
                    admin_id, BufferedInputFile(path.read_bytes(), filename=path.name),
                    caption="📦 Плановая резервная копия FunPay Control Center",
                )
            except Exception as exc:
                logger.warning("Scheduled backup delivery failed: {}", exc)

    async def watchdog_operation(self) -> None:
        now = time.monotonic()
        for name, task in list(self.tasks.items()):
            stale_after = max(CFG.watchdog_interval * 5, self.worker_intervals.get(name, 30) * 2 + 60)
            if task.done() and not self.stop_event.is_set():
                try:
                    task.exception()
                except (asyncio.CancelledError, Exception):
                    pass
                await self.notify(f"♻️ Watchdog заметил завершение задачи <code>{esc(name)}</code>.", important=True)
                self.restart_worker(name)
            elif now - self.heartbeats.get(name, now) > stale_after:
                await self.notify(f"♻️ Процесс <code>{esc(name)}</code> завис: watchdog перезапускает цикл. Неоднозначные отправки останутся заблокированы.", important=True)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                self.restart_worker(name)

    def restart_worker(self, name: str) -> None:
        specs: dict[str, tuple[int, Callable[[], Awaitable[None]]]] = {
            "orders": (CFG.order_poll, self.order_operation),
            "chats": (CFG.chat_poll, self.chat_operation),
            "bump": (60, self.bump_operation),
            "session": (CFG.session_interval, self.session_operation),
            "backup": (CFG.backup_interval, self.backup_operation),
            "watchdog": (CFG.watchdog_interval, self.watchdog_operation),
        }
        if name not in specs or self.stop_event.is_set():
            return
        interval, operation = specs[name]
        self.worker_intervals[name] = interval
        self.tasks[name] = asyncio.create_task(self.supervised_worker(name, interval, operation), name=name)

    async def health_text(self) -> str:
        lines = ["<b>🩺 SYSTEM HEALTH</b>", f"FunPay: {'🟢 connected' if self.fp.ready else '🔴 offline'}", f"Telegram: 🟢 polling", f"Database: {'🟢 ready' if self.storage.db else '🔴 closed'}", "", "<b>Workers</b>"]
        for name, task in self.tasks.items():
            age = int(time.monotonic() - self.heartbeats.get(name, time.monotonic()))
            lines.append(f"{'🟢' if not task.done() else '🔴'} {esc(name)} · heartbeat {age}s · errors {self.failures.get(name, 0)}")
        if self.fp.last_error:
            lines.append(f"\nПоследняя ошибка FunPay: <code>{esc(self.fp.last_error)}</code>")
        return "\n".join(lines)

    async def startup(self) -> None:
        await self.storage.open()
        for key, value in {
            "auto_delivery": env_bool("AUTO_DELIVERY", False),
            "auto_responder": env_bool("AUTO_RESPONDER", False),
            "auto_bump": env_bool("AUTO_BUMP", False),
        }.items():
            # Environment values are defaults only; saved panel preferences take precedence.
            await self.storage.set_setting(key, value, only_if_missing=True)
        await self.storage.set_setting("backup_interval", CFG.backup_interval, only_if_missing=True)
        try:
            await self.fp.connect()
            self.me_username = nested_username(field(self.fp.me, "username", default=""))
        except Exception as exc:
            await self.notify(f"⚠️ Панель запущена, FunPay пока offline: {esc(str(exc)[:300])}", important=True)
        self.restart_worker("orders")
        self.restart_worker("chats")
        self.restart_worker("bump")
        self.restart_worker("session")
        self.restart_worker("backup")
        self.restart_worker("watchdog")

    async def run(self) -> None:
        await self.startup()
        try:
            await self.dp.start_polling(self.bot, allowed_updates=self.dp.resolve_used_update_types())
        finally:
            self.stop_event.set()
            for task in self.tasks.values():
                task.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            await self.fp.close()
            await self.storage.close()
            await self.bot.session.close()

# === СЕКЦИЯ: ЗАПУСК ===
async def main() -> None:
    app = App()
    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
