"""SQLite storage layer for FunPay Control Center.

Keeps the old database compatible while adding transactional delivery state,
rule management, persistent chat cursors, inventory deduplication and audit data.
Python 3.11+; aiosqlite.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

import aiosqlite


class Storage:
    """Async SQLite repository. All multi-step writes are serialized and transactional."""

    def __init__(self, path: str, products_dir: str = "products", backups_dir: str = "backups") -> None:
        self.path = Path(path)
        self.products_dir = Path(products_dir)
        self.backups_dir = Path(backups_dir)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.products_dir.mkdir(parents=True, exist_ok=True)
        self.backups_dir.mkdir(parents=True, exist_ok=True)
        self.db: aiosqlite.Connection | None = None
        self.lock = asyncio.Lock()

    def conn(self) -> aiosqlite.Connection:
        if self.db is None:
            raise RuntimeError("Storage is not open")
        return self.db

    async def open(self) -> None:
        if self.db is not None:
            return
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA foreign_keys=ON")
        await self.db.execute("PRAGMA busy_timeout=10000")
        await self.db.execute("PRAGMA synchronous=NORMAL")
        await self.db.executescript("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS processed(
            kind TEXT NOT NULL, item_id TEXT NOT NULL, created REAL NOT NULL,
            PRIMARY KEY(kind,item_id)
        );
        CREATE TABLE IF NOT EXISTS stock(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lot_key TEXT NOT NULL, item TEXT NOT NULL, source TEXT NOT NULL,
            used_order TEXT, created REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS stock_available ON stock(lot_key, used_order, id);
        CREATE TABLE IF NOT EXISTS lot_map(
            lot_key TEXT PRIMARY KEY, lot_title TEXT NOT NULL,
            mode TEXT NOT NULL, payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS lists(
            kind TEXT NOT NULL, username TEXT NOT NULL, note TEXT DEFAULT '',
            PRIMARY KEY(kind,username)
        );
        CREATE TABLE IF NOT EXISTS rules(
            id INTEGER PRIMARY KEY AUTOINCREMENT, pattern TEXT NOT NULL,
            mode TEXT NOT NULL, reply TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            cooldown INTEGER NOT NULL DEFAULT 60
        );
        CREATE TABLE IF NOT EXISTS cooldowns(
            scope TEXT NOT NULL, key TEXT NOT NULL, sent REAL NOT NULL,
            PRIMARY KEY(scope,key)
        );
        CREATE TABLE IF NOT EXISTS orders(
            order_id TEXT PRIMARY KEY, buyer TEXT, lot_title TEXT,
            price REAL DEFAULT 0, currency TEXT DEFAULT '', status TEXT DEFAULT '',
            created REAL NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
            cost REAL DEFAULT 0, fee REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT,
            action TEXT NOT NULL, details TEXT NOT NULL, created REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS profit(
            id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT UNIQUE,
            revenue REAL NOT NULL, cost REAL NOT NULL, fee REAL NOT NULL,
            currency TEXT NOT NULL, created REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS first_contact(
            username TEXT PRIMARY KEY, created REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS deliveries(
            order_id TEXT PRIMARY KEY, lot_key TEXT NOT NULL,
            state TEXT NOT NULL, items_json TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1,
            created REAL NOT NULL, updated REAL NOT NULL, error TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS stock_fingerprints(
            lot_key TEXT NOT NULL, fingerprint TEXT NOT NULL,
            PRIMARY KEY(lot_key,fingerprint)
        );
        CREATE TABLE IF NOT EXISTS chat_cursors(
            chat_id TEXT PRIMARY KEY, last_message_id TEXT NOT NULL,
            updated REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS user_activity(
            username TEXT PRIMARY KEY, window_start REAL NOT NULL,
            message_count INTEGER NOT NULL, alerted_at REAL NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS orders_created ON orders(created DESC);
        CREATE INDEX IF NOT EXISTS audit_created ON audit(created DESC);
        CREATE INDEX IF NOT EXISTS deliveries_state ON deliveries(state, updated DESC);
        """)
        await self._migrate_legacy()
        await self.db.commit()

        defaults: dict[str, Any] = {
            "paused": False,
            "auto_delivery": False,
            "auto_responder": False,
            "auto_bump": False,
            "quiet_mode": False,
            "welcome_text": "Здравствуйте, {username}! 👋 Спасибо за обращение. Чем могу помочь?",
            "delivery_template": "Здравствуйте, {username}!\n\nВаш заказ #{order_id}:\n{items}\n\nСпасибо за покупку!",
            "commission_percent": float(os.getenv("DEFAULT_COMMISSION_PERCENT", "0") or 0),
            "bump_interval": 3600,
            "bump_allowlist": [],
            "last_bump_at": 0,
            "profile": "☀️ День",
            "spam_limit": 8,
            "spam_window": 60,
            "low_stock_threshold": 3,
            "backup_interval": 86400,
        }
        for key, value in defaults.items():
            await self.set_setting(key, value, only_if_missing=True)

    async def _migrate_legacy(self) -> None:
        """Add optional columns/tables without dropping the user's existing data."""
        # The first revision declared stock.used_order UNIQUE. That prevented a
        # single order from reserving more than one item. Rebuild only that table
        # when the legacy auto-index is present; all item rows and IDs are copied.
        index_cur = await self.conn().execute("PRAGMA index_list(stock)")
        indexes = await index_cur.fetchall()
        unique_used_order = False
        for index in indexes:
            if not bool(index["unique"]):
                continue
            index_name = str(index["name"]).replace('"', '""')
            cols_cur = await self.conn().execute(f'PRAGMA index_info("{index_name}")')
            indexed_columns = [str(col["name"]) for col in await cols_cur.fetchall()]
            if indexed_columns == ["used_order"]:
                unique_used_order = True
                break
        if unique_used_order:
            await self.conn().execute("ALTER TABLE stock RENAME TO stock_legacy_migrate")
            await self.conn().execute("""
                CREATE TABLE stock(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lot_key TEXT NOT NULL, item TEXT NOT NULL, source TEXT NOT NULL,
                    used_order TEXT, created REAL NOT NULL
                )
            """)
            await self.conn().execute(
                "INSERT INTO stock(id,lot_key,item,source,used_order,created) "
                "SELECT id,lot_key,item,source,used_order,created FROM stock_legacy_migrate"
            )
            await self.conn().execute("DROP TABLE stock_legacy_migrate")
            await self.conn().execute("CREATE INDEX IF NOT EXISTS stock_available ON stock(lot_key,used_order,id)")

        cur = await self.conn().execute("PRAGMA table_info(rules)")
        columns = {row["name"] for row in await cur.fetchall()}
        if "cooldown" not in columns:
            await self.conn().execute("ALTER TABLE rules ADD COLUMN cooldown INTEGER NOT NULL DEFAULT 60")
        if "enabled" not in columns:
            await self.conn().execute("ALTER TABLE rules ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
        # Seed fingerprints for old inventory rows; duplicate stock rows are preserved.
        cur = await self.conn().execute("SELECT lot_key,item FROM stock")
        rows = await cur.fetchall()
        await self.conn().executemany(
            "INSERT OR IGNORE INTO stock_fingerprints(lot_key,fingerprint) VALUES(?,?)",
            [(r["lot_key"], hashlib.sha256(r["item"].strip().encode("utf-8")).hexdigest()) for r in rows],
        )

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()
            self.db = None

    # === SETTINGS ===
    async def set_setting(self, key: str, value: Any, only_if_missing: bool = False) -> None:
        encoded = json.dumps(value, ensure_ascii=False)
        async with self.lock:
            if only_if_missing:
                await self.conn().execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, encoded))
            else:
                await self.conn().execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, encoded)
                )
            await self.conn().commit()

    async def get_setting(self, key: str, default: Any = None) -> Any:
        cur = await self.conn().execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except (TypeError, json.JSONDecodeError):
            return default

    async def settings(self) -> dict[str, Any]:
        cur = await self.conn().execute("SELECT key,value FROM settings")
        result: dict[str, Any] = {}
        for row in await cur.fetchall():
            try:
                result[row["key"]] = json.loads(row["value"])
            except json.JSONDecodeError:
                result[row["key"]] = row["value"]
        return result

    # === AUDIT / EVENT DEDUPLICATION ===
    async def add_audit(self, actor: str, action: str, details: str = "") -> None:
        async with self.lock:
            await self.conn().execute(
                "INSERT INTO audit(actor,action,details,created) VALUES(?,?,?,?)",
                (str(actor), action[:100], details[:4000], time.time()),
            )
            await self.conn().commit()

    async def audit_tail(self, limit: int = 20) -> list[dict[str, Any]]:
        cur = await self.conn().execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (max(1, min(limit, 100)),))
        return [dict(row) for row in await cur.fetchall()]

    async def mark_once(self, kind: str, item_id: str) -> bool:
        """Atomically claim event identity; True means it has not been seen before."""
        async with self.lock:
            try:
                await self.conn().execute(
                    "INSERT INTO processed(kind,item_id,created) VALUES(?,?,?)",
                    (kind[:80], str(item_id), time.time()),
                )
                await self.conn().commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def is_marked(self, kind: str, item_id: str) -> bool:
        cur = await self.conn().execute(
            "SELECT 1 FROM processed WHERE kind=? AND item_id=?", (kind[:80], str(item_id))
        )
        return await cur.fetchone() is not None

    async def set_chat_cursor(self, chat_id: str, message_id: str) -> None:
        async with self.lock:
            await self.conn().execute(
                "INSERT INTO chat_cursors(chat_id,last_message_id,updated) VALUES(?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET last_message_id=excluded.last_message_id,updated=excluded.updated",
                (str(chat_id), str(message_id), time.time()),
            )
            await self.conn().commit()

    async def get_chat_cursor(self, chat_id: str) -> str | None:
        cur = await self.conn().execute("SELECT last_message_id FROM chat_cursors WHERE chat_id=?", (str(chat_id),))
        row = await cur.fetchone()
        return str(row["last_message_id"]) if row else None

    async def first_contact(self, username: str) -> bool:
        normalized = username.casefold().strip()
        if not normalized:
            return False
        async with self.lock:
            try:
                await self.conn().execute(
                    "INSERT INTO first_contact(username,created) VALUES(?,?)", (normalized, time.time())
                )
                await self.conn().commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def record_user_message(self, username: str, window_seconds: int, limit: int) -> tuple[int, bool]:
        """Persist a sliding-window-ish counter; returns (count, should_alert)."""
        key = username.casefold().strip() or "unknown"
        now = time.time()
        async with self.lock:
            cur = await self.conn().execute("SELECT * FROM user_activity WHERE username=?", (key,))
            row = await cur.fetchone()
            if not row or now - row["window_start"] >= window_seconds:
                count, start, alerted = 1, now, 0.0
            else:
                count, start, alerted = int(row["message_count"]) + 1, float(row["window_start"]), float(row["alerted_at"])
            should_alert = count > limit and now - alerted > window_seconds
            await self.conn().execute(
                "INSERT INTO user_activity(username,window_start,message_count,alerted_at) VALUES(?,?,?,?) "
                "ON CONFLICT(username) DO UPDATE SET window_start=excluded.window_start, "
                "message_count=excluded.message_count,alerted_at=excluded.alerted_at",
                (key, start, count, now if should_alert else alerted),
            )
            await self.conn().commit()
            return count, should_alert

    # === USER LISTS ===
    async def set_user(self, kind: str, username: str, note: str = "") -> None:
        if kind not in {"black", "white", "vip"}:
            raise ValueError("Unknown user list")
        name = username.strip().lstrip("@").casefold()
        if not name:
            raise ValueError("Username is empty")
        async with self.lock:
            await self.conn().execute(
                "INSERT INTO lists(kind,username,note) VALUES(?,?,?) "
                "ON CONFLICT(kind,username) DO UPDATE SET note=excluded.note",
                (kind, name, note[:500]),
            )
            await self.conn().commit()

    async def remove_user(self, kind: str, username: str) -> None:
        async with self.lock:
            await self.conn().execute("DELETE FROM lists WHERE kind=? AND username=?", (kind, username.casefold().lstrip("@")))
            await self.conn().commit()

    async def list_users(self, kind: str) -> list[dict[str, str]]:
        cur = await self.conn().execute("SELECT username,note FROM lists WHERE kind=? ORDER BY username", (kind,))
        return [dict(row) for row in await cur.fetchall()]

    async def user_in(self, kind: str, username: str) -> bool:
        if not username:
            return False
        cur = await self.conn().execute(
            "SELECT 1 FROM lists WHERE kind=? AND username=?", (kind, username.casefold().lstrip("@"))
        )
        return await cur.fetchone() is not None

    # === AUTO-REPLY RULES ===
    async def add_rule(
        self, pattern: str, mode: str, reply: str, priority: int = 0, cooldown: int = 60
    ) -> int:
        if mode not in {"contains", "exact", "regex"}:
            raise ValueError("mode must be contains, exact or regex")
        if not pattern.strip() or len(pattern) > 160 or not reply.strip() or len(reply) > 3000:
            raise ValueError("Pattern/reply is empty or too long")
        if mode == "regex":
            import re
            re.compile(pattern, re.IGNORECASE)
        async with self.lock:
            cur = await self.conn().execute(
                "INSERT INTO rules(pattern,mode,reply,priority,enabled,cooldown) VALUES(?,?,?,?,1,?)",
                (pattern.strip(), mode, reply.strip(), int(priority), max(5, min(int(cooldown), 86400))),
            )
            await self.conn().commit()
            return int(cur.lastrowid)

    async def rules(self, include_disabled: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM rules" + ("" if include_disabled else " WHERE enabled=1") + " ORDER BY priority DESC,id"
        cur = await self.conn().execute(sql)
        return [dict(row) for row in await cur.fetchall()]

    async def toggle_rule(self, rule_id: int) -> bool:
        async with self.lock:
            cur = await self.conn().execute("SELECT enabled FROM rules WHERE id=?", (rule_id,))
            row = await cur.fetchone()
            if not row:
                return False
            enabled = 0 if row["enabled"] else 1
            await self.conn().execute("UPDATE rules SET enabled=? WHERE id=?", (enabled, rule_id))
            await self.conn().commit()
            return bool(enabled)

    async def delete_rule(self, rule_id: int) -> bool:
        async with self.lock:
            cur = await self.conn().execute("DELETE FROM rules WHERE id=?", (rule_id,))
            await self.conn().commit()
            return cur.rowcount > 0

    async def cooldown_ready(self, scope: str, key: str, seconds: int) -> bool:
        now = time.time()
        async with self.lock:
            cur = await self.conn().execute("SELECT sent FROM cooldowns WHERE scope=? AND key=?", (scope, key))
            row = await cur.fetchone()
            if row and now - float(row["sent"]) < seconds:
                return False
            await self.conn().execute(
                "INSERT INTO cooldowns(scope,key,sent) VALUES(?,?,?) "
                "ON CONFLICT(scope,key) DO UPDATE SET sent=excluded.sent", (scope, key, now)
            )
            await self.conn().commit()
            return True

    # === LOT MAPPING / STOCK ===
    async def map_lot(self, lot_key: str, title: str, mode: str, payload: dict[str, Any]) -> None:
        if mode not in {"stock", "static", "random"}:
            raise ValueError("Supported delivery modes: stock, static, random")
        async with self.lock:
            await self.conn().execute(
                "INSERT INTO lot_map(lot_key,lot_title,mode,payload) VALUES(?,?,?,?) "
                "ON CONFLICT(lot_key) DO UPDATE SET lot_title=excluded.lot_title,mode=excluded.mode,payload=excluded.payload",
                (str(lot_key), title[:250], mode, json.dumps(payload, ensure_ascii=False)),
            )
            await self.conn().commit()

    async def get_lot_map(self, lot_key: str) -> dict[str, Any] | None:
        cur = await self.conn().execute("SELECT * FROM lot_map WHERE lot_key=?", (str(lot_key),))
        row = await cur.fetchone()
        if not row:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"] or "{}")
        return result

    async def find_lot_map(self, lot_id: str, title: str) -> dict[str, Any] | None:
        if lot_id:
            exact = await self.get_lot_map(lot_id)
            if exact:
                return exact
        normalized = title.casefold().strip()
        cur = await self.conn().execute("SELECT * FROM lot_map ORDER BY length(lot_title) DESC")
        for row in await cur.fetchall():
            key = str(row["lot_key"]).casefold()
            name = str(row["lot_title"]).casefold()
            if (key and key in normalized) or (name and name in normalized):
                result = dict(row)
                result["payload"] = json.loads(result["payload"] or "{}")
                return result
        return None

    async def lot_maps(self) -> list[dict[str, Any]]:
        cur = await self.conn().execute("SELECT * FROM lot_map ORDER BY lot_key")
        result = []
        for row in await cur.fetchall():
            item = dict(row)
            item["payload"] = json.loads(item["payload"] or "{}")
            result.append(item)
        return result

    async def add_stock(self, lot_key: str, items: list[str], source: str) -> tuple[int, int]:
        """Import unique non-empty stock lines; returns (inserted, duplicates)."""
        clean = [line.strip() for line in items if line and line.strip()]
        inserted = 0
        duplicates = 0
        async with self.lock:
            db = self.conn()
            await db.execute("BEGIN IMMEDIATE")
            try:
                for item in clean:
                    fingerprint = hashlib.sha256(item.encode("utf-8")).hexdigest()
                    cur = await db.execute(
                        "INSERT OR IGNORE INTO stock_fingerprints(lot_key,fingerprint) VALUES(?,?)",
                        (str(lot_key), fingerprint),
                    )
                    if cur.rowcount == 0:
                        duplicates += 1
                        continue
                    await db.execute(
                        "INSERT INTO stock(lot_key,item,source,created) VALUES(?,?,?,?)",
                        (str(lot_key), item, source[:250], time.time()),
                    )
                    inserted += 1
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return inserted, duplicates

    async def stock_count(self, lot_key: str) -> int:
        cur = await self.conn().execute(
            "SELECT COUNT(*) AS n FROM stock WHERE lot_key=? AND used_order IS NULL", (str(lot_key),)
        )
        return int((await cur.fetchone())["n"])

    async def inventory(self) -> list[dict[str, Any]]:
        cur = await self.conn().execute("""
            SELECT m.lot_key,m.lot_title,m.mode,
            SUM(CASE WHEN s.used_order IS NULL THEN 1 ELSE 0 END) AS available,
            COUNT(s.id) AS total
            FROM lot_map m LEFT JOIN stock s ON s.lot_key=m.lot_key
            GROUP BY m.lot_key,m.lot_title,m.mode ORDER BY m.lot_key
        """)
        return [dict(row) for row in await cur.fetchall()]

    # === ORDERS / SAFE DELIVERY STATE MACHINE ===
    async def save_order(self, order: dict[str, Any]) -> None:
        now = time.time()
        async with self.lock:
            await self.conn().execute(
                "INSERT INTO orders(order_id,buyer,lot_title,price,currency,status,created,delivered,cost,fee) "
                "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET "
                "buyer=COALESCE(excluded.buyer,orders.buyer),lot_title=COALESCE(excluded.lot_title,orders.lot_title), "
                "price=CASE WHEN excluded.price>0 THEN excluded.price ELSE orders.price END, "
                "currency=CASE WHEN excluded.currency!='' THEN excluded.currency ELSE orders.currency END, "
                "status=excluded.status",
                (str(order["order_id"]), order.get("buyer"), order.get("lot_title"),
                 float(order.get("price") or 0), str(order.get("currency") or ""),
                 str(order.get("status") or ""), now, int(bool(order.get("delivered", False))),
                 float(order.get("cost") or 0), float(order.get("fee") or 0)),
            )
            await self.conn().commit()

    async def order_delivered(self, order_id: str) -> bool:
        cur = await self.conn().execute("SELECT delivered FROM orders WHERE order_id=?", (str(order_id),))
        row = await cur.fetchone()
        return bool(row and row["delivered"])

    async def prepare_delivery(
        self, order_id: str, lot_key: str, count: int,
        static_items: list[str] | None = None, randomize: bool = False,
    ) -> dict[str, Any]:
        """Reserve stock and create a durable delivery record in one transaction.

        Existing states are never silently resent. 'uncertain' must be reviewed by
        an administrator because the remote service may have accepted the message.
        """
        count = max(1, min(int(count), 100))
        now = time.time()
        async with self.lock:
            db = self.conn()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cur = await db.execute("SELECT * FROM deliveries WHERE order_id=?", (str(order_id),))
                existing = await cur.fetchone()
                if existing:
                    await db.rollback()
                    return {"state": existing["state"], "items": json.loads(existing["items_json"]), "existing": True}

                stock_ids: list[int] = []
                items: list[str]
                if static_items is not None:
                    items = [str(x).strip() for x in static_items if str(x).strip()]
                    if not items:
                        await db.rollback()
                        return {"state": "empty_source", "items": [], "existing": False}
                    if len(items) == 1 and count > 1:
                        items *= count
                    if len(items) != count:
                        await db.rollback()
                        return {"state": "quantity_mismatch", "items": [], "existing": False}
                else:
                    order = "RANDOM()" if randomize else "id"
                    cur = await db.execute(
                        f"SELECT id,item FROM stock WHERE lot_key=? AND used_order IS NULL ORDER BY {order} LIMIT ?",
                        (str(lot_key), count),
                    )
                    rows = await cur.fetchall()
                    if len(rows) != count:
                        await db.rollback()
                        return {"state": "out_of_stock", "items": [], "existing": False}
                    stock_ids = [int(row["id"]) for row in rows]
                    items = [str(row["item"]) for row in rows]
                    await db.executemany(
                        "UPDATE stock SET used_order=? WHERE id=? AND used_order IS NULL",
                        [(str(order_id), stock_id) for stock_id in stock_ids],
                    )

                await db.execute(
                    "INSERT INTO deliveries(order_id,lot_key,state,items_json,attempts,created,updated,error) "
                    "VALUES(?,?, 'prepared', ?,1,?,?, '')",
                    (str(order_id), str(lot_key), json.dumps(items, ensure_ascii=False), now, now),
                )
                await db.commit()
                return {"state": "prepared", "items": items, "existing": False}
            except Exception:
                await db.rollback()
                raise

    async def set_delivery_state(self, order_id: str, state: str, error: str = "") -> None:
        allowed = {"prepared", "sent", "uncertain", "out_of_stock", "cancelled"}
        if state not in allowed:
            raise ValueError("Invalid delivery state")
        async with self.lock:
            await self.conn().execute(
                "UPDATE deliveries SET state=?,updated=?,error=? WHERE order_id=?",
                (state, time.time(), error[:1500], str(order_id)),
            )
            await self.conn().commit()

    async def confirm_delivery(self, order_id: str, revenue: float, cost: float, fee: float, currency: str) -> None:
        now = time.time()
        async with self.lock:
            db = self.conn()
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    "UPDATE deliveries SET state='sent',updated=?,error='' WHERE order_id=?", (now, str(order_id))
                )
                await db.execute("UPDATE orders SET delivered=1,cost=?,fee=? WHERE order_id=?", (cost, fee, str(order_id)))
                await db.execute(
                    "INSERT OR IGNORE INTO profit(order_id,revenue,cost,fee,currency,created) VALUES(?,?,?,?,?,?)",
                    (str(order_id), revenue, cost, fee, currency, now),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def delivery(self, order_id: str) -> dict[str, Any] | None:
        cur = await self.conn().execute("SELECT * FROM deliveries WHERE order_id=?", (str(order_id),))
        row = await cur.fetchone()
        if not row:
            return None
        result = dict(row)
        result["items"] = json.loads(result.pop("items_json") or "[]")
        return result

    async def deliveries_by_state(self, states: tuple[str, ...], limit: int = 20) -> list[dict[str, Any]]:
        if not states:
            return []
        placeholders = ",".join("?" for _ in states)
        cur = await self.conn().execute(
            f"SELECT * FROM deliveries WHERE state IN ({placeholders}) ORDER BY updated DESC LIMIT ?",
            (*states, max(1, min(limit, 100))),
        )
        result = []
        for row in await cur.fetchall():
            item = dict(row)
            item["items"] = json.loads(item.pop("items_json") or "[]")
            result.append(item)
        return result

    async def reset_delivery_after_manual_check(self, order_id: str, verified_not_sent: bool) -> bool:
        """Release reserved items only after a human explicitly confirms no delivery occurred."""
        if not verified_not_sent:
            raise ValueError("Human verification is required")
        async with self.lock:
            db = self.conn()
            await db.execute("BEGIN IMMEDIATE")
            try:
                cur = await db.execute("SELECT state FROM deliveries WHERE order_id=?", (str(order_id),))
                row = await cur.fetchone()
                if not row or row["state"] not in {"uncertain", "prepared"}:
                    await db.rollback()
                    return False
                await db.execute("UPDATE stock SET used_order=NULL WHERE used_order=?", (str(order_id),))
                await db.execute("DELETE FROM deliveries WHERE order_id=?", (str(order_id),))
                await db.execute("UPDATE orders SET delivered=0 WHERE order_id=?", (str(order_id),))
                await db.commit()
                return True
            except Exception:
                await db.rollback()
                raise

    async def recent_orders(self, limit: int = 15) -> list[dict[str, Any]]:
        cur = await self.conn().execute(
            "SELECT o.*,d.state AS delivery_state FROM orders o "
            "LEFT JOIN deliveries d ON d.order_id=o.order_id ORDER BY o.created DESC LIMIT ?",
            (max(1, min(limit, 100)),),
        )
        return [dict(row) for row in await cur.fetchall()]

    async def pending_orders_count(self) -> int:
        cur = await self.conn().execute("SELECT COUNT(*) AS n FROM orders WHERE delivered=0")
        return int((await cur.fetchone())["n"])

    async def add_profit(self, order_id: str, revenue: float, cost: float, fee: float, currency: str) -> None:
        async with self.lock:
            await self.conn().execute(
                "INSERT OR IGNORE INTO profit(order_id,revenue,cost,fee,currency,created) VALUES(?,?,?,?,?,?)",
                (str(order_id), revenue, cost, fee, currency, time.time()),
            )
            await self.conn().commit()

    async def stats(self, since: float = 0) -> dict[str, Any]:
        cur = await self.conn().execute(
            "SELECT COUNT(*) AS n,COALESCE(SUM(revenue),0) AS revenue,"
            "COALESCE(SUM(cost),0) AS cost,COALESCE(SUM(fee),0) AS fee,"
            "COUNT(DISTINCT currency) AS currencies FROM profit WHERE created>=?", (since,)
        )
        row = dict(await cur.fetchone())
        row["net"] = float(row["revenue"]) - float(row["cost"]) - float(row["fee"])
        cur = await self.conn().execute(
            "SELECT currency,COUNT(*) AS n,COALESCE(SUM(revenue),0) AS revenue,"
            "COALESCE(SUM(cost),0) AS cost,COALESCE(SUM(fee),0) AS fee "
            "FROM profit WHERE created>=? GROUP BY currency ORDER BY currency", (since,)
        )
        row["by_currency"] = []
        for item in await cur.fetchall():
            grouped = dict(item)
            grouped["net"] = float(grouped["revenue"]) - float(grouped["cost"]) - float(grouped["fee"])
            row["by_currency"].append(grouped)
        row["pending_orders"] = await self.pending_orders_count()
        return row

    # === BACKUPS ===
    async def backup(self) -> Path:
        """Create a consistent DB snapshot and ZIP it with product TXT files."""
        stamp = time.strftime("%Y%m%d_%H%M%S")
        db_snapshot = self.backups_dir / f"database_{stamp}.sqlite3"
        target = await aiosqlite.connect(db_snapshot)
        try:
            await self.conn().backup(target)
        finally:
            await target.close()

        archive = self.backups_dir / f"funpay_backup_{stamp}.zip"

        def pack() -> None:
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                zf.write(db_snapshot, arcname="database/funpay.sqlite3")
                if self.products_dir.exists():
                    for path in self.products_dir.rglob("*"):
                        if path.is_file() and not path.is_symlink():
                            zf.write(path, arcname=f"products/{path.relative_to(self.products_dir)}")
            db_snapshot.unlink(missing_ok=True)

        await asyncio.to_thread(pack)
        return archive
