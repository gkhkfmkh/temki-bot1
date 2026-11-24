"""Persistent state for FunPay Automation Bot. Python 3.11+."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

import aiosqlite


class Storage:
    """SQLite repository. Every write that allocates stock is serialized by a lock."""

    def __init__(self, path: str, products_dir: str, backups_dir: str):
        self.path = Path(path)
        self.products_dir = Path(products_dir)
        self.backups_dir = Path(backups_dir)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.products_dir.mkdir(parents=True, exist_ok=True)
        self.backups_dir.mkdir(parents=True, exist_ok=True)
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._db.executescript("""
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS processed(kind TEXT NOT NULL, item_id TEXT NOT NULL, created REAL NOT NULL, PRIMARY KEY(kind,item_id));
        CREATE TABLE IF NOT EXISTS stock(id INTEGER PRIMARY KEY AUTOINCREMENT, lot_key TEXT NOT NULL, item TEXT NOT NULL, source TEXT NOT NULL, used_order TEXT UNIQUE, created REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stock_available ON stock(lot_key, used_order);
        CREATE TABLE IF NOT EXISTS lot_map(lot_key TEXT PRIMARY KEY, lot_title TEXT NOT NULL, mode TEXT NOT NULL, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS lists(kind TEXT NOT NULL, username TEXT NOT NULL, note TEXT DEFAULT '', PRIMARY KEY(kind,username));
        CREATE TABLE IF NOT EXISTS rules(id INTEGER PRIMARY KEY AUTOINCREMENT, pattern TEXT NOT NULL, mode TEXT NOT NULL, reply TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0, enabled INTEGER NOT NULL DEFAULT 1, cooldown INTEGER NOT NULL DEFAULT 60);
        CREATE TABLE IF NOT EXISTS cooldowns(scope TEXT NOT NULL, key TEXT NOT NULL, sent REAL NOT NULL, PRIMARY KEY(scope,key));
        CREATE TABLE IF NOT EXISTS orders(order_id TEXT PRIMARY KEY, buyer TEXT, lot_title TEXT, price REAL DEFAULT 0, currency TEXT DEFAULT '', status TEXT DEFAULT '', created REAL NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, cost REAL DEFAULT 0, fee REAL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT, action TEXT NOT NULL, details TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS profit(id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT UNIQUE, revenue REAL NOT NULL, cost REAL NOT NULL, fee REAL NOT NULL, currency TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS first_contact(username TEXT PRIMARY KEY, created REAL NOT NULL);
        """)
        await self._db.commit()
        defaults = {
            "paused": False, "auto_delivery": False, "auto_responder": False,
            "auto_bump": False, "auto_online": False, "auto_complaints": False,
            "welcome_text": "Здравствуйте, {username}! Чем могу помочь?",
            "low_stock_alert": True, "bump_interval": 3600,
            "commission_percent": 0.0, "minimum_price": 0.0,
            "profile": "☀️ День", "quiet_mode": False,
        }
        for key, value in defaults.items():
            await self.set_setting(key, value)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    def _conn(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("Storage is not open")
        return self._db

    async def set_setting(self, key: str, value: Any) -> None:
        encoded = json.dumps(value, ensure_ascii=False)
        async with self._write_lock:
            await self._conn().execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, encoded))
            await self._conn().commit()

    async def get_setting(self, key: str, default: Any = None) -> Any:
        cur = await self._conn().execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except (TypeError, json.JSONDecodeError):
            return default

    async def settings(self) -> dict[str, Any]:
        cur = await self._conn().execute("SELECT key,value FROM settings")
        rows = await cur.fetchall()
        return {r["key"]: json.loads(r["value"]) for r in rows}

    async def mark_once(self, kind: str, item_id: str) -> bool:
        """Atomic anti-duplicate gate. True means this process owns the event."""
        async with self._write_lock:
            try:
                await self._conn().execute("INSERT INTO processed(kind,item_id,created) VALUES(?,?,?)", (kind, str(item_id), time.time()))
                await self._conn().commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def add_audit(self, actor: str, action: str, details: str) -> None:
        async with self._write_lock:
            await self._conn().execute("INSERT INTO audit(actor,action,details,created) VALUES(?,?,?,?)", (actor, action, details[:4000], time.time()))
            await self._conn().commit()

    async def audit_tail(self, limit: int = 20) -> list[dict[str, Any]]:
        cur = await self._conn().execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in await cur.fetchall()]

    async def set_user(self, kind: str, username: str, note: str = "") -> None:
        if kind not in {"black", "white", "vip"}:
            raise ValueError("Unknown list")
        async with self._write_lock:
            await self._conn().execute("INSERT INTO lists(kind,username,note) VALUES(?,?,?) ON CONFLICT(kind,username) DO UPDATE SET note=excluded.note", (kind, username.lower(), note))
            await self._conn().commit()

    async def remove_user(self, kind: str, username: str) -> None:
        async with self._write_lock:
            await self._conn().execute("DELETE FROM lists WHERE kind=? AND username=?", (kind, username.lower()))
            await self._conn().commit()

    async def list_users(self, kind: str) -> list[dict[str, str]]:
        cur = await self._conn().execute("SELECT username,note FROM lists WHERE kind=? ORDER BY username", (kind,))
        return [dict(r) for r in await cur.fetchall()]

    async def user_in(self, kind: str, username: str) -> bool:
        cur = await self._conn().execute("SELECT 1 FROM lists WHERE kind=? AND username=?", (kind, username.lower()))
        return await cur.fetchone() is not None

    async def add_rule(self, pattern: str, mode: str, reply: str, priority: int = 0, cooldown: int = 60) -> int:
        if mode not in {"contains", "exact", "regex"}:
            raise ValueError("mode must be contains, exact or regex")
        async with self._write_lock:
            cur = await self._conn().execute("INSERT INTO rules(pattern,mode,reply,priority,cooldown) VALUES(?,?,?,?,?)", (pattern, mode, reply, priority, cooldown))
            await self._conn().commit()
            return int(cur.lastrowid)

    async def rules(self) -> list[dict[str, Any]]:
        cur = await self._conn().execute("SELECT * FROM rules WHERE enabled=1 ORDER BY priority DESC,id")
        return [dict(r) for r in await cur.fetchall()]

    async def cooldown_ready(self, scope: str, key: str, seconds: int) -> bool:
        now = time.time()
        async with self._write_lock:
            cur = await self._conn().execute("SELECT sent FROM cooldowns WHERE scope=? AND key=?", (scope, key))
            row = await cur.fetchone()
            if row and now - row["sent"] < seconds:
                return False
            await self._conn().execute("INSERT INTO cooldowns(scope,key,sent) VALUES(?,?,?) ON CONFLICT(scope,key) DO UPDATE SET sent=excluded.sent", (scope, key, now))
            await self._conn().commit()
            return True

    async def first_contact(self, username: str) -> bool:
        async with self._write_lock:
            try:
                await self._conn().execute("INSERT INTO first_contact(username,created) VALUES(?,?)", (username.lower(), time.time()))
                await self._conn().commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def map_lot(self, lot_key: str, title: str, mode: str, payload: dict[str, Any]) -> None:
        async with self._write_lock:
            await self._conn().execute("INSERT INTO lot_map(lot_key,lot_title,mode,payload) VALUES(?,?,?,?) ON CONFLICT(lot_key) DO UPDATE SET lot_title=excluded.lot_title,mode=excluded.mode,payload=excluded.payload", (str(lot_key), title, mode, json.dumps(payload, ensure_ascii=False)))
            await self._conn().commit()

    async def get_lot_map(self, lot_key: str) -> dict[str, Any] | None:
        cur = await self._conn().execute("SELECT * FROM lot_map WHERE lot_key=?", (str(lot_key),))
        row = await cur.fetchone()
        if not row:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    async def add_stock(self, lot_key: str, items: list[str], source: str) -> int:
        clean = [x.strip() for x in items if x and x.strip()]
        async with self._write_lock:
            await self._conn().executemany("INSERT INTO stock(lot_key,item,source,created) VALUES(?,?,?,?)", [(str(lot_key), x, source, time.time()) for x in clean])
            await self._conn().commit()
        return len(clean)

    async def stock_count(self, lot_key: str) -> int:
        cur = await self._conn().execute("SELECT COUNT(*) n FROM stock WHERE lot_key=? AND used_order IS NULL", (str(lot_key),))
        return int((await cur.fetchone())["n"])

    async def allocate_stock(self, lot_key: str, order_id: str, count: int) -> list[str] | None:
        """Transactionally reserves N items for an order. Unique order_id prevents re-delivery."""
        if count < 1 or count > 100:
            raise ValueError("Invalid quantity")
        async with self._write_lock:
            db = self._conn()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cur = await db.execute("SELECT item FROM stock WHERE lot_key=? AND used_order IS NULL ORDER BY id LIMIT ?", (str(lot_key), count))
                rows = await cur.fetchall()
                if len(rows) != count:
                    await db.rollback()
                    return None
                items = [r["item"] for r in rows]
                cur = await db.execute("SELECT 1 FROM orders WHERE order_id=? AND delivered=1", (str(order_id),))
                if await cur.fetchone():
                    await db.rollback()
                    return None
                cur = await db.execute("SELECT id FROM stock WHERE lot_key=? AND used_order IS NULL ORDER BY id LIMIT ?", (str(lot_key), count))
                ids = [r["id"] for r in await cur.fetchall()]
                await db.executemany("UPDATE stock SET used_order=? WHERE id=?", [(str(order_id), i) for i in ids])
                await db.commit()
                return items
            except Exception:
                await db.rollback()
                raise

    async def save_order(self, order: dict[str, Any]) -> None:
        async with self._write_lock:
            await self._conn().execute("INSERT INTO orders(order_id,buyer,lot_title,price,currency,status,created,delivered,cost,fee) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET status=excluded.status,buyer=COALESCE(excluded.buyer,orders.buyer),lot_title=COALESCE(excluded.lot_title,orders.lot_title),price=CASE WHEN excluded.price>0 THEN excluded.price ELSE orders.price END", (str(order["order_id"]), order.get("buyer"), order.get("lot_title"), float(order.get("price") or 0), order.get("currency") or "", order.get("status") or "", time.time(), int(order.get("delivered", False)), float(order.get("cost") or 0), float(order.get("fee") or 0)))
            await self._conn().commit()

    async def set_delivered(self, order_id: str) -> None:
        async with self._write_lock:
            await self._conn().execute("UPDATE orders SET delivered=1 WHERE order_id=?", (str(order_id),))
            await self._conn().commit()

    async def order_delivered(self, order_id: str) -> bool:
        cur = await self._conn().execute("SELECT delivered FROM orders WHERE order_id=?", (str(order_id),))
        row = await cur.fetchone()
        return bool(row and row["delivered"])

    async def add_profit(self, order_id: str, revenue: float, cost: float, fee: float, currency: str) -> None:
        async with self._write_lock:
            await self._conn().execute("INSERT OR IGNORE INTO profit(order_id,revenue,cost,fee,currency,created) VALUES(?,?,?,?,?,?)", (str(order_id), revenue, cost, fee, currency, time.time()))
            await self._conn().commit()

    async def stats(self, since: float = 0) -> dict[str, Any]:
        cur = await self._conn().execute("SELECT COUNT(*) n,COALESCE(SUM(revenue),0) revenue,COALESCE(SUM(cost),0) cost,COALESCE(SUM(fee),0) fee,COUNT(DISTINCT currency) currencies FROM profit WHERE created>=?", (since,))
        row = dict(await cur.fetchone())
        row["net"] = row["revenue"] - row["cost"] - row["fee"]
        cur = await self._conn().execute("SELECT COUNT(*) n FROM orders WHERE delivered=0")
        row["pending_orders"] = int((await cur.fetchone())["n"])
        return row

    async def recent_orders(self, limit: int = 15) -> list[dict[str, Any]]:
        cur = await self._conn().execute("SELECT * FROM orders ORDER BY created DESC LIMIT ?", (limit,))
        return [dict(r) for r in await cur.fetchall()]

    async def backup(self) -> Path:
        """Consistent SQLite backup plus products directory in a timestamped ZIP."""
        stamp = time.strftime("%Y%m%d_%H%M%S")
        db_copy = self.backups_dir / f"funpay_{stamp}.db"
        target = await aiosqlite.connect(db_copy)
        try:
            await self._conn().backup(target)
        finally:
            await target.close()
        archive = shutil.make_archive(str(self.backups_dir / f"backup_{stamp}"), "zip", root_dir=self.products_dir.parent, base_dir=self.products_dir.name)
        return Path(archive)
