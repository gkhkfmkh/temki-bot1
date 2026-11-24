"""FunPay seller assistant with Telegram control panel.

The FunPay integration uses the third-party async package funpay-api-python.
It is not an official FunPay API. Keep irreversible/claim-related operations
manual and reviewable; never retry a message/claim blindly after an ambiguous
network failure.
"""
from __future__ import annotations

import asyncio
import html
import inspect
import json
import os
import random
import re
import shutil
import signal
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                           Message, BufferedInputFile)
from dotenv import load_dotenv
from loguru import logger

from storage import Storage

load_dotenv()


# === СЕКЦИЯ: КОНФИГУРАЦИЯ ===
@dataclass(frozen=True)
class Config:
    bot_token: str = os.getenv("BOT_TOKEN", "")
    admin_id: int = int(os.getenv("ADMIN_ID", "0"))
    trusted_admins: tuple[int, ...] = tuple(int(x) for x in os.getenv("TRUSTED_ADMINS", "").split(",") if x.strip().isdigit())
    golden_key: str = os.getenv("FUNPAY_GOLDEN_KEY", "")
    user_agent: str = os.getenv("FUNPAY_USER_AGENT", "Mozilla/5.0")
    proxy: str = os.getenv("FUNPAY_PROXY", "")
    database: str = os.getenv("DATABASE_PATH", "funpay.db")
    products: str = os.getenv("PRODUCTS_DIR", "products")
    backups: str = os.getenv("BACKUPS_DIR", "backups")
    logs: str = os.getenv("LOGS_DIR", "logs")
    order_poll: int = max(5, int(os.getenv("ORDER_POLL_INTERVAL", "15")))
    chat_poll: int = max(5, int(os.getenv("CHAT_POLL_INTERVAL", "10")))
    online_interval: int = max(60, int(os.getenv("ONLINE_INTERVAL", "300")))
    watchdog_interval: int = max(10, int(os.getenv("WATCHDOG_INTERVAL", "30")))
    backup_interval: int = max(3600, int(os.getenv("BACKUP_INTERVAL", "86400")))
    max_retries: int = max(1, int(os.getenv("MAX_RETRIES", "5")))
    timeout: int = max(5, int(os.getenv("REQUEST_TIMEOUT", "20")))
    spam_limit: int = max(1, int(os.getenv("MAX_MESSAGES_PER_USER", "10")))
    spam_window: int = max(10, int(os.getenv("SPAM_WINDOW_SECONDS", "60")))

    @property
    def admins(self) -> set[int]:
        return ({self.admin_id} if self.admin_id else set()) | set(self.trusted_admins)


CFG = Config()
Path(CFG.logs).mkdir(parents=True, exist_ok=True)
logger.add(str(Path(CFG.logs) / "bot_{time:YYYY-MM-DD}.log"), rotation="10 MB", retention="14 days", level="INFO", encoding="utf-8")


# === СЕКЦИЯ: УТИЛИТЫ И НОРМАЛИЗАЦИЯ ОБЪЕКТОВ API ===
def value(obj: Any, *names: str, default: Any = None) -> Any:
    """Read dict/Pydantic/object fields without assuming a specific model class."""
    if obj is None:
        return default
    if hasattr(obj, "model_dump"):
        try:
            obj = obj.model_dump()
        except Exception:
            pass
    if isinstance(obj, dict):
        for name in names:
            if name in obj and obj[name] is not None:
                return obj[name]
    else:
        for name in names:
            result = getattr(obj, name, None)
            if result is not None:
                return result
    return default


def as_text(obj: Any) -> str:
    if hasattr(obj, "value"):
        obj = obj.value
    return str(obj if obj is not None else "")


def fmt(template: str, **kwargs: Any) -> str:
    class Safe(dict):
        def __missing__(self, key: str) -> str:
            return "{" + key + "}"
    return template.format_map(Safe(kwargs))


def is_paid(status: str) -> bool:
    s = status.lower()
    return any(x in s for x in ("paid", "оплачен", "оплач", "delivery", "выдач", "active")) and not any(x in s for x in ("refund", "возврат", "cancel", "отмен"))


# === СЕКЦИЯ: FUNPAY АДАПТЕР ===
class FunPayGateway:
    """Narrow adapter around third-party async FunPay client.

    All network calls go through call(); retries are limited to idempotent reads.
    Mutations (send_message, raise_lots, save_lot, claim) are never blindly retried.
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.client = None
        self.account = None
        self.ready = False
        self.last_error = ""

    async def connect(self) -> None:
        if not self.cfg.golden_key:
            logger.warning("FUNPAY_GOLDEN_KEY is empty; running Telegram panel in offline mode")
            return
        try:
            from funpay_api_python import FunPayAccount, FunPayClient
            kwargs: dict[str, Any] = {"golden_key": self.cfg.golden_key, "user_agent": self.cfg.user_agent}
            # The client reads FUNPAY_PROXY from its environment (documented config).
            if self.cfg.proxy:
                os.environ["FUNPAY_PROXY"] = self.cfg.proxy
            self.client = FunPayClient(**kwargs)
            if hasattr(self.client, "__aenter__"):
                await self.client.__aenter__()
            self.account = FunPayAccount(self.client)
            await self._call("get_me", retry_read=True)
            self.ready = True
            logger.info("FunPay session initialized")
        except Exception as exc:
            self.last_error = type(exc).__name__ + ": " + str(exc)[:300]
            self.ready = False
            logger.exception("FunPay connection failed")
            raise

    async def close(self) -> None:
        if self.client and hasattr(self.client, "__aexit__"):
            await self.client.__aexit__(None, None, None)
        self.ready = False

    async def _call(self, method: str, *args: Any, retry_read: bool = False, **kwargs: Any) -> Any:
        if not self.account:
            raise RuntimeError("FunPay is not connected")
        fn = getattr(self.account, method, None)
        if fn is None:
            raise RuntimeError(f"Installed FunPay client has no method {method!r}; check pinned library version")
        attempts = self.cfg.max_retries if retry_read else 1
        for attempt in range(attempts):
            try:
                if inspect.iscoroutinefunction(fn):
                    return await asyncio.wait_for(fn(*args, **kwargs), timeout=self.cfg.timeout)
                result = await asyncio.wait_for(asyncio.to_thread(fn, *args, **kwargs), timeout=self.cfg.timeout)
                if inspect.isawaitable(result):
                    return await asyncio.wait_for(result, timeout=self.cfg.timeout)
                return result
            except Exception as exc:
                self.last_error = type(exc).__name__ + ": " + str(exc)[:300]
                text = str(exc).lower()
                if any(x in text for x in ("unauthorized", "401", "invalid cookie", "golden_key")):
                    self.ready = False
                    raise PermissionError("FunPay session expired or unauthorized") from exc
                retryable = any(x in text for x in ("429", "503", "timeout", "tempor", "connection", "rate limit"))
                if not retry_read or not retryable or attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(min(60, 2 ** attempt + random.random()))
        raise RuntimeError("Unreachable retry state")

    async def sales(self) -> list[Any]:
        return await self._call("get_sales", include_closed=False, limit=50, retry_read=True)

    async def chats(self) -> list[Any]:
        return await self._call("get_chats", retry_read=True)

    async def history(self, chat_id: str, last_message_id: str | None = None) -> list[Any]:
        return await self._call("get_chat_history", chat_id, last_message_id=last_message_id, retry_read=True)

    async def send_message(self, chat_id: str, text: str) -> Any:
        # A send is not retried: timeout can mean that the platform accepted it.
        return await self._call("send_message", chat_id, text)

    async def balance(self) -> Any:
        return await self._call("get_balance", retry_read=True)

    async def me(self) -> Any:
        return await self._call("get_me", retry_read=True)

    async def bump(self, lot_ids: list[int]) -> Any:
        return await self._call("raise_lots", lot_ids)


# === СЕКЦИЯ: СОСТОЯНИЕ ПРИЛОЖЕНИЯ И АДМИНИСТРАТОРЫ ===
class App:
    def __init__(self):
        self.storage = Storage(CFG.database, CFG.products, CFG.backups)
        self.fp = FunPayGateway(CFG)
        self.bot = Bot(CFG.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.tasks: dict[str, asyncio.Task] = {}
        self.task_heartbeats: dict[str, float] = {}
        self.task_failures: dict[str, int] = {}
        self.stop_event = asyncio.Event()
        self.chat_seen: dict[str, str] = {}
        self.message_rate: dict[str, list[float]] = {}
        self.register_handlers()

    def admin(self, user_id: int) -> bool:
        return user_id in CFG.admins

    async def alert(self, text: str) -> None:
        for admin_id in CFG.admins:
            try:
                await self.bot.send_message(admin_id, text[:3900])
            except Exception as exc:
                logger.warning("Cannot notify admin {}: {}", admin_id, exc)

    async def audit(self, actor: str, action: str, details: str) -> None:
        await self.storage.add_audit(actor, action, details)
        logger.info("AUDIT actor={} action={} details={}", actor, action, details[:500])

    async def guarded(self, name: str, coro: Awaitable[Any]) -> None:
        try:
            await coro
            self.task_heartbeats[name] = time.monotonic()
            self.task_failures[name] = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.task_failures[name] = self.task_failures.get(name, 0) + 1
            logger.exception("Background task {} failed", name)
            if self.task_failures[name] in (1, 3, 5):
                await self.alert(f"⚠️ <b>Сбой фоновой задачи</b> <code>{html.escape(name)}</code>\n{html.escape(str(exc)[:800])}")
            raise

    def spawn(self, name: str, factory: Callable[[], Awaitable[Any]]) -> None:
        old = self.tasks.get(name)
        if old and not old.done():
            return
        async def runner() -> None:
            while not self.stop_event.is_set():
                try:
                    await factory()
                    if not self.stop_event.is_set():
                        await asyncio.sleep(1)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await asyncio.sleep(min(60, 2 ** min(self.task_failures.get(name, 1), 6)))
        self.tasks[name] = asyncio.create_task(runner(), name=name)

    async def enabled(self, key: str) -> bool:
        return bool(await self.storage.get_setting(key, False)) and not bool(await self.storage.get_setting("paused", False))


# === СЕКЦИЯ: КЛАВИАТУРЫ TELEGRAM ===
def kb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t, callback_data=d) for t, d in row] for row in rows])


def main_kb() -> InlineKeyboardMarkup:
    return kb([
        [("⚙️ Модули и пресеты", "menu:modules"), ("📝 Настройки и тексты", "menu:texts")],
        [("📦 Товары и остатки", "menu:products"), ("🧾 Заказы и финансы", "menu:orders")],
        [("👥 Списки пользователей", "menu:lists"), ("📜 Журнал и бэкап", "menu:logs")],
        [("🔄 Обновить", "menu:home")],
    ])


def back_kb(target: str = "home") -> InlineKeyboardMarkup:
    return kb([[ ("⬅️ Назад", f"menu:{target}"), ("🏠 Главное меню", "menu:home") ]])


# === СЕКЦИЯ: FSM ===
class InputFlow(StatesGroup):
    setting_value = State()
    add_list_user = State()
    add_rule = State()
    add_lot_map = State()
    upload_items = State()
    reply_buyer = State()


# === СЕКЦИЯ: TELEGRAM ПАНЕЛЬ ===
def register_handlers(self: App) -> None:
    @self.dp.message(Command("start", "menu"))
    async def start(message: Message):
        if not self.admin(message.from_user.id):
            return
        await message.answer(await self.dashboard_text(), reply_markup=main_kb())

    @self.dp.message(Command("status"))
    async def status_cmd(message: Message):
        if self.admin(message.from_user.id):
            await message.answer(await self.dashboard_text(), reply_markup=main_kb())

    @self.dp.callback_query(F.data.startswith("menu:"))
    async def menu_callback(call: CallbackQuery):
        if not self.admin(call.from_user.id):
            await call.answer("Нет доступа", show_alert=True)
            return
        section = call.data.split(":", 1)[1]
        renderers = {"home": self.dashboard_text, "modules": self.modules_text, "texts": self.texts_text,
                     "products": self.products_text, "orders": self.orders_text, "lists": self.lists_text, "logs": self.logs_text}
        renderer = renderers.get(section, dashboard_text)
        text = await renderer()
        markup = main_kb() if section == "home" else section_kb(section)
        await call.message.edit_text(text, reply_markup=markup)
        await call.answer()

    @self.dp.callback_query(F.data.startswith("toggle:"))
    async def toggle_callback(call: CallbackQuery):
        if not self.admin(call.from_user.id): return
        key = call.data.split(":", 1)[1]
        allowed = {"auto_delivery", "auto_responder", "auto_bump", "auto_online", "auto_complaints", "paused", "quiet_mode"}
        if key not in allowed: return
        new_value = not bool(await self.storage.get_setting(key, False))
        await self.storage.set_setting(key, new_value)
        await self.audit(str(call.from_user.id), "toggle", f"{key}={new_value}")
        await call.message.edit_text(await self.modules_text(), reply_markup=section_kb("modules"))
        await call.answer("Включено" if new_value else "Выключено")

    @self.dp.callback_query(F.data.startswith("preset:"))
    async def preset_callback(call: CallbackQuery):
        if not self.admin(call.from_user.id): return
        preset = call.data.split(":", 1)[1]
        presets = {
            "day": {"profile": "☀️ День", "paused": False, "auto_delivery": True, "auto_responder": True, "auto_bump": False, "auto_online": True},
            "night": {"profile": "🌙 Ночь", "paused": False, "auto_delivery": True, "auto_responder": False, "auto_bump": False, "auto_online": True},
            "rush": {"profile": "⚡ Аврал", "paused": False, "auto_delivery": True, "auto_responder": True, "auto_bump": False, "auto_online": True},
        }
        if preset not in presets: return
        for k, v in presets[preset].items(): await self.storage.set_setting(k, v)
        await self.audit(str(call.from_user.id), "preset", preset)
        await call.message.edit_text(await self.modules_text(), reply_markup=section_kb("modules"))
        await call.answer("Профиль применён")

    @self.dp.callback_query(F.data.startswith("action:"))
    async def action_callback(call: CallbackQuery, state: FSMContext):
        if not self.admin(call.from_user.id): return
        action = call.data.split(":", 1)[1]
        if action == "welcome":
            await state.set_state(InputFlow.setting_value); await state.update_data(setting="welcome_text")
            await call.message.answer("Отправьте новый текст приветствия. Переменная: {username}", reply_markup=back_kb("texts"))
        elif action == "add_black" or action == "add_white" or action == "add_vip":
            await state.set_state(InputFlow.add_list_user); await state.update_data(list_kind=action[4:])
            await call.message.answer("Отправьте username FunPay без @:", reply_markup=back_kb("lists"))
        elif action == "add_rule":
            await state.set_state(InputFlow.add_rule)
            await call.message.answer("Отправьте правило в формате: режим | шаблон | ответ\nРежим: contains или exact. Например:\ncontains | привет | Здравствуйте!", reply_markup=back_kb("texts"))
        elif action == "add_map":
            await state.set_state(InputFlow.add_lot_map)
            await call.message.answer("Отправьте: lot_id | режим | данные\nРежимы: stock, static, random. Для static/random данные — текст или относительный путь к файлу.", reply_markup=back_kb("products"))
        elif action == "backup":
            path = await self.storage.backup()
            await call.message.answer_document(BufferedInputFile(path.read_bytes(), filename=path.name), caption="Резервная копия")
        elif action == "orders":
            await call.message.answer(await self.orders_text(), reply_markup=section_kb("orders"))
        elif action == "complaint_review":
            await call.message.answer("⚖️ Автоматическая отправка претензий отключена: перед подачей нужно проверить конкретный заказ, сроки и доказательства. Пришлите ID заказа администратору для ручной проверки.", reply_markup=back_kb("orders"))
        await call.answer()

    @self.dp.message(InputFlow.setting_value)
    async def setting_value(message: Message, state: FSMContext):
        if not self.admin(message.from_user.id): return
        data = await state.get_data(); key = data.get("setting")
        if key == "welcome_text":
            await self.storage.set_setting(key, message.text[:1500])
            await self.audit(str(message.from_user.id), "setting", key)
            await message.answer("Приветствие сохранено.", reply_markup=section_kb("texts"))
        await state.clear()

    @self.dp.message(InputFlow.add_list_user)
    async def add_user(message: Message, state: FSMContext):
        if not self.admin(message.from_user.id): return
        kind = (await state.get_data()).get("list_kind")
        username = (message.text or "").strip().lstrip("@").lower()
        if not re.fullmatch(r"[a-zA-Z0-9_]{2,40}", username):
            await message.answer("Некорректный username. Повторите ввод."); return
        await self.storage.set_user(kind, username)
        await self.audit(str(message.from_user.id), "list_add", f"{kind}:{username}")
        await state.clear(); await message.answer(f"Добавлен в список {kind}: <code>{html.escape(username)}</code>", reply_markup=section_kb("lists"))

    @self.dp.message(InputFlow.add_rule)
    async def add_rule(message: Message, state: FSMContext):
        if not self.admin(message.from_user.id): return
        try:
            mode, pattern, reply = [x.strip() for x in (message.text or "").split("|", 2)]
            if mode not in {"contains", "exact"} or not pattern or not reply: raise ValueError
            await self.storage.add_rule(pattern, mode, reply, priority=10 if mode == "exact" else 0)
            await self.audit(str(message.from_user.id), "rule_add", f"{mode}:{pattern}")
            await message.answer("Правило добавлено.", reply_markup=section_kb("texts")); await state.clear()
        except ValueError:
            await message.answer("Формат не распознан. Используйте: contains | слово | ответ")

    @self.dp.message(InputFlow.add_lot_map)
    async def add_lot_map(message: Message, state: FSMContext):
        if not self.admin(message.from_user.id): return
        try:
            lot_id, mode, payload = [x.strip() for x in (message.text or "").split("|", 2)]
            if not lot_id.isdigit() or mode not in {"stock", "static", "random"}: raise ValueError
            await self.storage.map_lot(lot_id, f"Лот {lot_id}", mode, {"source": payload})
            await self.audit(str(message.from_user.id), "lot_map", f"{lot_id}:{mode}")
            await message.answer("Привязка сохранена.", reply_markup=section_kb("products")); await state.clear()
        except ValueError:
            await message.answer("Формат: lot_id | stock/static/random | путь_или_текст")

    @self.dp.message(F.document)
    async def upload_document(message: Message):
        if not self.admin(message.from_user.id): return
        doc = message.document
        if not doc.file_name or not doc.file_name.lower().endswith(".txt"):
            return
        if doc.file_size and doc.file_size > 2_000_000:
            await message.answer("Файл больше 2 МБ — загрузка отклонена."); return
        file = await self.bot.get_file(doc.file_id)
        import io
        buf = io.BytesIO(); await self.bot.download_file(file.file_path, buf)
        text = buf.getvalue().decode("utf-8-sig", errors="replace")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        # Upload format: caption `lot_id:123`; no lot id means save file only.
        match = re.search(r"lot_id\s*:\s*(\d+)", message.caption or "", re.I)
        if match:
            count = await self.storage.add_stock(match.group(1), lines, doc.file_name)
            await message.answer(f"Загружено товаров: {count} для лота {match.group(1)}")
        else:
            dest = Path(CFG.products) / Path(doc.file_name).name
            dest.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            await message.answer(f"Файл сохранён: <code>{html.escape(dest.name)}</code>. Для импорта в остатки отправьте его с подписью lot_id:123.")

    @self.dp.message(F.reply_to_message)
    async def reply_to_buyer(message: Message):
        if not self.admin(message.from_user.id) or not self.fp.ready: return
        marker = re.search(r"CHAT_ID=(\w+)", message.reply_to_message.text or "")
        if not marker:
            return
        try:
            await self.fp.send_message(marker.group(1), message.text or "")
            await self.audit(str(message.from_user.id), "buyer_reply", marker.group(1))
            await message.reply("Ответ отправлен.")
        except Exception as exc:
            await message.reply(f"Не удалось отправить: {html.escape(str(exc)[:500])}")



def section_kb(section: str) -> InlineKeyboardMarkup:
    common = [("⬅️ Назад", "menu:home"), ("🏠 Главное меню", "menu:home")]
    if section == "modules":
        return kb([[ ("Автовыдача", "toggle:auto_delivery"), ("Автоответчик", "toggle:auto_responder") ],
                   [("Автоподнятие", "toggle:auto_bump"), ("Онлайн", "toggle:auto_online")],
                   [("Автожалобы", "toggle:auto_complaints"), ("Пауза", "toggle:paused")],
                   [("☀️ День", "preset:day"), ("🌙 Ночь", "preset:night"), ("⚡ Аврал", "preset:rush")], common])
    if section == "texts": return kb([[ ("Изменить приветствие", "action:welcome"), ("Добавить триггер", "action:add_rule") ], common])
    if section == "products": return kb([[ ("Привязать лот к выдаче", "action:add_map") ], common])
    if section == "orders": return kb([[ ("Обзор заказов", "action:orders"), ("Проверка претензии", "action:complaint_review") ], common])
    if section == "lists": return kb([[ ("➕ Чёрный список", "action:add_black"), ("➕ Белый список", "action:add_white") ], [("➕ VIP", "action:add_vip")], common])
    if section == "logs": return kb([[ ("📦 Создать бэкап", "action:backup") ], common])
    return kb([common])


# === СЕКЦИЯ: ТЕКСТЫ ЭКРАНОВ ===
async def dashboard_text(self: App) -> str:
    s = await self.storage.stats(time.time() - 86400)
    settings = await self.storage.settings()
    fp = "🟢 подключён" if self.fp.ready else "🔴 не подключён"
    active = sum(bool(settings.get(k)) for k in ("auto_delivery", "auto_responder", "auto_bump", "auto_online"))
    return ("<b>FUNPAY CONTROL PANEL</b>\n"
            f"FunPay: {fp}\nРежим: {'⏸ пауза' if settings.get('paused') else '▶️ активен'}\n"
            f"Профиль: {html.escape(str(settings.get('profile','—')))}\n"
            f"Активные модули: <b>{active}/4</b>\n\n"
            f"Заказы за 24 часа: <b>{s['n']}</b>\nВыручка: <b>{s['revenue']:.2f}</b>\n"
            f"Расчётная прибыль: <b>{s['net']:.2f}</b>\nНе выдано: <b>{s['pending_orders']}</b>\n\n"
            f"Фоновые задачи: <b>{sum(not t.done() for t in self.tasks.values())}</b>")


async def modules_text(self: App) -> str:
    keys = [("auto_delivery", "Автовыдача"), ("auto_responder", "Автоответчик"), ("auto_bump", "Автоподнятие"), ("auto_online", "Поддержание активности"), ("auto_complaints", "Автожалобы"), ("paused", "Общая пауза"), ("quiet_mode", "Тихий режим")]
    lines = ["<b>⚙️ Модули и пресеты</b>"]
    for key, title in keys:
        enabled = bool(await self.storage.get_setting(key, False))
        lines.append(f"{'🟢 ВКЛ' if enabled else '🔴 ВЫКЛ'} — {title}")
    lines.append(f"\nПрофиль: {html.escape(str(await self.storage.get_setting('profile','—')))}")
    return "\n".join(lines)


async def texts_text(self: App) -> str:
    welcome = await self.storage.get_setting("welcome_text", "")
    rules = await self.storage.rules()
    return f"<b>📝 Настройки и тексты</b>\nПриветствие: <code>{html.escape(welcome[:250])}</code>\nАктивных триггеров: <b>{len(rules)}</b>\n\nПеременные приветствия: {{username}}"


async def products_text(self: App) -> str:
    cur = await self.storage._conn().execute("SELECT lot_key,lot_title,mode FROM lot_map ORDER BY lot_key")
    rows = await cur.fetchall()
    lines = ["<b>📦 Товары и остатки</b>"]
    for row in rows:
        n = await self.storage.stock_count(row["lot_key"])
        lines.append(f"Лот <code>{html.escape(row['lot_key'])}</code> · {html.escape(row['mode'])} · остаток <b>{n}</b>")
    if len(lines) == 1: lines.append("Привязок пока нет. Добавьте лот через меню.")
    return "\n".join(lines)


async def orders_text(self: App) -> str:
    rows = await self.storage.recent_orders(12)
    stats = await self.storage.stats(0)
    lines = ["<b>🧾 Заказы и финансы</b>", f"Всего учтённых продаж: {stats['n']}", f"Выручка: {stats['revenue']:.2f}", f"Себестоимость: {stats['cost']:.2f}", f"Комиссия: {stats['fee']:.2f}", f"Чистая расчётная прибыль: {stats['net']:.2f}", "", "<b>Последние заказы:</b>"]
    for r in rows:
        lines.append(f"<code>#{html.escape(r['order_id'])}</code> · {html.escape(str(r['buyer'] or '—'))} · {html.escape(str(r['status'] or '—'))} · {'выдан' if r['delivered'] else 'ожидает'}")
    return "\n".join(lines)


async def lists_text(self: App) -> str:
    lines = ["<b>👥 Списки пользователей</b>"]
    for kind, title in (("black", "Чёрный"), ("white", "Белый"), ("vip", "VIP")):
        users = await self.storage.list_users(kind)
        lines.append(f"{title}: {', '.join(html.escape(x['username']) for x in users) or '—'}")
    return "\n".join(lines)


async def logs_text(self: App) -> str:
    rows = await self.storage.audit_tail(12)
    lines = ["<b>📜 Журнал действий</b>"]
    for r in rows:
        stamp = datetime.fromtimestamp(r["created"]).strftime("%d.%m %H:%M:%S")
        lines.append(f"<code>{stamp}</code> · {html.escape(r['action'])} · {html.escape(r['details'][:120])}")
    if len(lines) == 1: lines.append("Записей пока нет.")
    return "\n".join(lines)


# Bind UI functions as methods after definitions.
App.register_handlers = register_handlers
App.dashboard_text = dashboard_text
App.modules_text = modules_text
App.texts_text = texts_text
App.products_text = products_text
App.orders_text = orders_text
App.lists_text = lists_text
App.logs_text = logs_text


# === СЕКЦИЯ: ОБРАБОТКА ЗАКАЗОВ И АВТОВЫДАЧА ===
async def process_orders(self: App) -> None:
    if not self.fp.ready:
        await asyncio.sleep(CFG.order_poll); return
    sales = await self.fp.sales()
    for sale in sales or []:
        order_id = as_text(value(sale, "id", "order_id", "orderId"))
        if not order_id: continue
        buyer = as_text(value(sale, "buyer_username", "buyerUsername", "buyer", default=""))
        title = as_text(value(sale, "description", "lot_title", "title", default=""))
        status = as_text(value(sale, "status", default=""))
        price_raw = value(sale, "price", default=0)
        try: price = float(price_raw or 0)
        except (TypeError, ValueError): price = 0.0
        currency = as_text(value(sale, "currency", default=""))
        await self.storage.save_order({"order_id": order_id, "buyer": buyer, "lot_title": title, "price": price, "currency": currency, "status": status})
        if not is_paid(status) or await self.storage.order_delivered(order_id): continue
        if not await self.enabled("auto_delivery"): continue
        # Resolve lot mapping by the explicit lot ID first; title matching is fallback.
        lot_id = as_text(value(sale, "lot_id", "offer_id", "lotId", default=""))
        mapping = await self.storage.get_lot_map(lot_id) if lot_id else None
        if not mapping:
            cur = await self.storage._conn().execute("SELECT * FROM lot_map ORDER BY length(lot_key) DESC")
            for row in await cur.fetchall():
                if row["lot_key"].lower() in title.lower():
                    mapping = {"lot_key": row["lot_key"], "lot_title": row["lot_title"], "mode": row["mode"], "payload": json.loads(row["payload"])}; break
        if not mapping: continue
        if buyer and await self.storage.user_in("black", buyer):
            await self.alert(f"⛔ Заказ #{html.escape(order_id)} от пользователя из чёрного списка: {html.escape(buyer)}")
            continue
        qty = int(value(sale, "quantity", "count", default=1) or 1)
        payload = mapping["payload"]
        items: list[str] = []
        mode = mapping["mode"]
        if mode == "stock":
            items = await self.storage.allocate_stock(mapping["lot_key"], order_id, qty) or []
            if not items:
                await self.alert(f"🚨 Недостаточно товара для заказа #{html.escape(order_id)}. Лот {html.escape(mapping['lot_key'])}, требуется {qty}.")
                continue
        elif mode == "static":
            items = [str(payload.get("source", ""))] * qty
        elif mode == "random":
            source = str(payload.get("source", ""))
            source_path = Path(CFG.products) / Path(source).name
            if source_path.exists():
                pool = [x.strip() for x in source_path.read_text(encoding="utf-8").splitlines() if x.strip()]
                if pool: items = random.sample(pool, min(qty, len(pool)))
        if len(items) != qty or any(not x.strip() for x in items):
            await self.alert(f"🚨 Ошибка источника товара для заказа #{html.escape(order_id)}")
            continue
        message = "\n\n".join(f"{i+1}. {item}" for i, item in enumerate(items))
        rendered = fmt(message, username=buyer, order_id=order_id, lot_title=title, count=qty, price=price, items=message)
        try:
            chat_id = as_text(value(sale, "chat_id", "chatId", default=""))
            if not chat_id and buyer:
                chat_list = await self.fp.chats()
                for chat in chat_list or []:
                    if as_text(value(chat, "username", "interlocutor_username", "buyer_username", default="")).lower() == buyer.lower():
                        chat_id = as_text(value(chat, "id", "chat_id", default="")); break
            if not chat_id: raise RuntimeError("Не удалось определить chat_id покупателя")
            await self.fp.send_message(chat_id, rendered)
            await self.storage.set_delivered(order_id)
            fee_pct = float(await self.storage.get_setting("commission_percent", 0.0))
            fee = price * fee_pct / 100
            await self.storage.add_profit(order_id, price, 0.0, fee, currency)
            await self.audit("system", "auto_delivery", f"order={order_id}; qty={qty}")
            await self.alert(f"✅ Выдан заказ <b>#{html.escape(order_id)}</b>\nПокупатель: {html.escape(buyer)}\nКоличество: {qty}")
        except Exception as exc:
            # Stock reservation remains attached to order; operator must inspect before retry.
            await self.alert(f"⚠️ Не подтверждена отправка товара по заказу #{html.escape(order_id)}. Автоповтор заблокирован во избежание дубля. Ошибка: {html.escape(str(exc)[:500])}")
    await asyncio.sleep(CFG.order_poll)


# === СЕКЦИЯ: ЧАТЫ И АВТООТВЕТЧИК ===
async def process_chats(self: App) -> None:
    if not self.fp.ready:
        await asyncio.sleep(CFG.chat_poll); return
    chats = await self.fp.chats()
    for chat in chats or []:
        chat_id = as_text(value(chat, "id", "chat_id", default=""))
        username = as_text(value(chat, "username", "interlocutor_username", "buyer_username", default=""))
        if not chat_id: continue
        last_id = self.chat_seen.get(chat_id)
        history = await self.fp.history(chat_id, last_message_id=last_id)
        for msg in history or []:
            msg_id = as_text(value(msg, "id", "message_id", default=""))
            if not msg_id or not await self.storage.mark_once("message", msg_id): continue
            self.chat_seen[chat_id] = msg_id
            author = as_text(value(msg, "author", "username", "sender", default=""))
            text = as_text(value(msg, "text", "message", "content", default=""))
            is_mine = bool(value(msg, "is_my", "is_mine", "outgoing", default=False))
            if is_mine or not text: continue
            now = time.time(); bucket = self.message_rate.setdefault(author.lower(), [])
            bucket[:] = [x for x in bucket if now - x < CFG.spam_window]; bucket.append(now)
            if len(bucket) > CFG.spam_limit:
                await self.alert(f"🚨 Всплеск сообщений от {html.escape(author)}: {len(bucket)} за {CFG.spam_window} сек.")
                continue
            vip = await self.storage.user_in("vip", author)
            if await self.storage.user_in("black", author): continue
            # Telegram live chat. CHAT_ID marker enables replying to this message in Telegram.
            await self.alert(f"{'⭐ VIP · ' if vip else ''}💬 <b>{html.escape(author or 'Покупатель')}</b>\n{html.escape(text[:2500])}\n<code>CHAT_ID={html.escape(chat_id)}</code>")
            if not await self.enabled("auto_responder"):
                continue
            if await self.storage.first_contact(author):
                welcome = await self.storage.get_setting("welcome_text", "")
                if welcome and await self.storage.cooldown_ready("welcome", author.lower(), 86400):
                    await self.fp.send_message(chat_id, fmt(welcome, username=author))
            rules = await self.storage.rules()
            for rule in rules:
                body = text.strip(); pattern = rule["pattern"]
                matched = (rule["mode"] == "exact" and body.casefold() == pattern.casefold()) or (rule["mode"] == "contains" and pattern.casefold() in body.casefold())
                if rule["mode"] == "regex":
                    try: matched = bool(re.search(pattern, body, re.I))
                    except re.error: matched = False
                if matched and await self.storage.cooldown_ready("rule", f"{author.lower()}:{rule['id']}", int(rule["cooldown"])):
                    await self.fp.send_message(chat_id, fmt(rule["reply"], username=author))
                    break
    await asyncio.sleep(CFG.chat_poll)


# === СЕКЦИЯ: АВТОПОДНЯТИЕ И ПЕРИОДИЧЕСКИЕ ЗАДАЧИ ===
async def bump_cycle(self: App) -> None:
    if await self.enabled("auto_bump") and self.fp.ready:
        cur = await self.storage._conn().execute("SELECT lot_key FROM lot_map WHERE lot_key GLOB '[0-9]*'")
        ids = [int(r["lot_key"]) for r in await cur.fetchall()]
        if ids:
            # Respect platform limits; do not add random timing to evade detection.
            await self.fp.bump(ids[:20])
            await self.audit("system", "bump", f"lots={len(ids[:20])}")
    interval = int(await self.storage.get_setting("bump_interval", 3600))
    await asyncio.sleep(max(300, interval))


async def online_cycle(self: App) -> None:
    # There is no documented, stable seller heartbeat method in the supported client.
    # This task checks session health; it does not simulate activity or evade platform controls.
    if await self.enabled("auto_online") and self.fp.ready:
        await self.fp.me()
    await asyncio.sleep(CFG.online_interval)


async def backup_cycle(self: App) -> None:
    path = await self.storage.backup()
    for admin_id in CFG.admins:
        try:
            await self.bot.send_document(admin_id, BufferedInputFile(path.read_bytes(), filename=path.name), caption="Автоматический бэкап")
        except Exception as exc:
            logger.warning("Backup delivery failed: {}", exc)
    await asyncio.sleep(CFG.backup_interval)


async def watchdog_cycle(self: App) -> None:
    now = time.monotonic()
    for name, task in list(self.tasks.items()):
        if task.done() and not self.stop_event.is_set():
            exc = None
            try: exc = task.exception()
            except asyncio.CancelledError: pass
            await self.alert(f"♻️ Watchdog перезапускает задачу <code>{html.escape(name)}</code>. {html.escape(str(exc)[:300] if exc else 'Задача завершилась')}")
            factory = {"orders": lambda: process_orders(self), "chats": lambda: process_chats(self), "bump": lambda: bump_cycle(self), "online": lambda: online_cycle(self), "backup": lambda: backup_cycle(self)}.get(name)
            if factory: self.spawn(name, factory)
        elif now - self.task_heartbeats.get(name, now) > max(CFG.watchdog_interval * 4, 120) and not task.done():
            logger.warning("Task {} heartbeat is stale", name)
    await asyncio.sleep(CFG.watchdog_interval)


# === СЕКЦИЯ: ЗАПУСК И ЗАВЕРШЕНИЕ ===
async def run(self: App) -> None:
    if not CFG.bot_token or not CFG.admin_id:
        raise RuntimeError("Set BOT_TOKEN and ADMIN_ID in .env")
    await self.storage.open()
    try:
        await self.fp.connect()
    except Exception as exc:
        await self.alert(f"⚠️ FunPay пока недоступен. Панель Telegram запущена в ограниченном режиме.\n{html.escape(str(exc)[:500])}")
    self.spawn("orders", lambda: process_orders(self))
    self.spawn("chats", lambda: process_chats(self))
    self.spawn("bump", lambda: bump_cycle(self))
    self.spawn("online", lambda: online_cycle(self))
    self.spawn("backup", lambda: backup_cycle(self))
    self.spawn("watchdog", lambda: watchdog_cycle(self))
    try:
        await self.dp.start_polling(self.bot, allowed_updates=self.dp.resolve_used_update_types())
    finally:
        self.stop_event.set()
        for task in self.tasks.values(): task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        await self.fp.close()
        await self.storage.close()
        await self.bot.session.close()


async def main() -> None:
    app = App()
    await run(app)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
