import asyncio
from datetime import date, datetime, timedelta, timezone
import html
import io
import json
import logging
import os
import re
import secrets
import sqlite3
import string
from typing import Optional
import urllib.parse

try:
    from zoneinfo import ZoneInfo
    KYIV_TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    KYIV_TZ = timezone(timedelta(hours=3))

from aiohttp import web
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)
from google import genai
from google.genai import types as genai_types
import httpx
from PIL import Image, ImageOps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "5641374843"))

RAW_JAR_URL = os.getenv("MONOBANK_JAR_URL", "https://send.monobank.ua/jar/7E9CVK1jX1").strip().strip('"').strip("'")
if not RAW_JAR_URL.startswith("http"):
    RAW_JAR_URL = "https://send.monobank.ua/jar/7E9CVK1jX1"
MONOBANK_JAR_URL = RAW_JAR_URL
MONOBANK_TOKEN = os.getenv("MONOBANK_TOKEN", "").strip()

FREE_CHECKS_PER_DAY = 3
DB_NAME = "resale_bot.db"

CHANNEL_USERNAME = "@crimeprok"
CHANNEL_URL = "https://t.me/crimeprok"

DEFAULT_TURSO_URL = "libsql://resale-db-artem12222.aws-ap-south-1.turso.io"
DEFAULT_TURSO_TOKEN = (
    "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJleHAiOjc5NzQzNzI1NDUsImlhdCI6MTc4ODczNzM0NSwiaWQiOiIw"
    "MWEwNzkwYy1kZTAxLTc0ZDYtODI4YS04OTI4MzNiZTZhNzAiLCJraWQiOiJjbzRQNUNYMW5raXdXN3VVWVRCNXVHX1V2ejl2N2x3LTV"
    "2MVU1ZzlTbTdZIiwicmlkIjoiYzBkMDYyNjQtY2RhMy00NTc2LTg3ZWEtMzE4ZTllNGEyZTMyIn0.vBqQN4hxC2rctguUkmbUF1RNti"
    "3DN6yEuKQ7QrFeE18fHXxbByhLC7iLsej9SBYbzQtJvpvInaH4m9MljEfmBQ"
)

TURSO_DB_URL = os.getenv("TURSO_DB_URL", DEFAULT_TURSO_URL).strip()
TURSO_DB_TOKEN = os.getenv("TURSO_DB_TOKEN", DEFAULT_TURSO_TOKEN).strip()

PLANS = {
    "pack_15": {
        "title": "⚡ 15 проверок",
        "description": "Пакет из 15 проверок без ограничения по времени",
        "stars": 25,
        "uah": 30,
        "type": "checks",
        "amount": 15
    },
    "pack_50": {
        "title": "⚡ 50 проверок",
        "description": "Пакет из 50 проверок для активных поисков",
        "stars": 65,
        "uah": 75,
        "type": "checks",
        "amount": 50
    },
    "sub_7d": {
        "title": "🗓 Безлимит на 7 дней",
        "description": "Неограниченные проверки на 1 неделю",
        "stars": 95,
        "uah": 110,
        "type": "days",
        "amount": 7
    },
    "sub_30d": {
        "title": "👑 Безлимит на 30 дней",
        "description": "Полный безлимит на месяц (Хит для ресейла)",
        "stars": 190,
        "uah": 220,
        "type": "days",
        "amount": 30
    },
    "lifetime": {
        "title": "♾ VIP Навсегда",
        "description": "Пожизненный доступ ко всем проверкам и обновлениям",
        "stars": 490,
        "uah": 550,
        "type": "lifetime",
        "amount": 999999
    }
}

class TursoHTTPClient:
    """Отказоустойчивый клиент для Turso Cloud через стандартный протокол /v2/pipeline."""
    def __init__(self, db_url: str, auth_token: str):
        endpoint = db_url.strip()
        if endpoint.startswith("libsql://"):
            endpoint = "https://" + endpoint[len("libsql://"):]
        elif endpoint.startswith("turso://"):
            endpoint = "https://" + endpoint[len("turso://"):]
        if not endpoint.endswith("/v2/pipeline"):
            endpoint = endpoint.rstrip("/") + "/v2/pipeline"

        self.endpoint = endpoint
        self.auth_token = auth_token.strip()
        self.headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json"
        }

    def _convert_arg(self, val):
        if val is None:
            return {"type": "null"}
        elif isinstance(val, bool):
            return {"type": "integer", "value": "1" if val else "0"}
        elif isinstance(val, int):
            return {"type": "integer", "value": str(val)}
        elif isinstance(val, float):
            return {"type": "float", "value": val}
        return {"type": "text", "value": str(val)}

    def execute(self, sql: str, params: tuple = ()) -> list[dict]:
        stmt = {"sql": sql}
        if params:
            stmt["args"] = [self._convert_arg(p) for p in params]

        payload = {
            "requests": [
                {"type": "execute", "stmt": stmt},
                {"type": "close"}
            ]
        }

        with httpx.Client(timeout=12.0) as client:
            resp = client.post(self.endpoint, headers=self.headers, json=payload)
            if resp.status_code != 200:
                raise RuntimeError(f"Turso API {resp.status_code}: {resp.text}")

            data = resp.json()
            results = data.get("results", [])
            if not results:
                return []

            first = results[0]
            if first.get("type") == "error":
                raise RuntimeError(f"Turso SQL Error: {first.get('error')}")

            res = first.get("response", {}).get("result", {})
            cols = [c.get("name") for c in res.get("cols", [])]
            raw_rows = res.get("rows", [])

            output_rows = []
            for r in raw_rows:
                row_dict = {}
                for col_name, cell in zip(cols, r):
                    if not isinstance(cell, dict):
                        row_dict[col_name] = cell
                        continue
                    cell_type = cell.get("type")
                    cell_val = cell.get("value")
                    if cell_type == "null" or cell_val is None:
                        row_dict[col_name] = None
                    elif cell_type == "integer":
                        row_dict[col_name] = int(cell_val)
                    elif cell_type == "float":
                        row_dict[col_name] = float(cell_val)
                    else:
                        row_dict[col_name] = cell_val
                output_rows.append(row_dict)
            return output_rows

turso_client: Optional[TursoHTTPClient] = None
if TURSO_DB_URL and TURSO_DB_TOKEN:
    try:
        turso_client = TursoHTTPClient(TURSO_DB_URL, TURSO_DB_TOKEN)
        logger.info("Turso Cloud клиент успешно настроен.")
    except Exception as e:
        logger.warning(f"Не удалось инициализировать Turso клиент: {e}")
        turso_client = None

def query_db(sql: str, params: tuple = ()) -> list[dict]:
    """Выполняет SQL-запрос в Turso Cloud. При сетевом сбое мягко переключается на локальный SQLite."""
    if turso_client:
        try:
            return turso_client.execute(sql, params)
        except Exception as e:
            logger.error(f"Turso Cloud запрос не удался ({e}), используем локальный fallback.")

    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql, params)
        if sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER")):
            conn.commit()
        if cur.description:
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        return []

def init_db():
    query_db("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            checks_today INTEGER DEFAULT 0,
            last_check_date TEXT,
            is_premium INTEGER DEFAULT 0,
            extra_checks INTEGER DEFAULT 0,
            premium_until TEXT DEFAULT NULL,
            is_lifetime INTEGER DEFAULT 0,
            referred_by_blogger TEXT DEFAULT NULL
        )
    """)

    try:
        user_cols = query_db("PRAGMA table_info(users)")
        existing_col_names = [c.get("name") for c in user_cols if isinstance(c, dict)]
        if "referred_by_blogger" not in existing_col_names:
            query_db("ALTER TABLE users ADD COLUMN referred_by_blogger TEXT DEFAULT NULL")
        if "initial_checks_used" not in existing_col_names:
            query_db("ALTER TABLE users ADD COLUMN initial_checks_used INTEGER DEFAULT 0")
        if "sub_prompt_shown" not in existing_col_names:
            query_db("ALTER TABLE users ADD COLUMN sub_prompt_shown INTEGER DEFAULT 0")
    except Exception as e:
        logger.debug(f"Проверка колонок users: {e}")

    query_db("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            plan_id TEXT,
            amount REAL,
            currency TEXT,
            method TEXT,
            status TEXT,
            created_at TEXT
        )
    """)

    query_db("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            tx_id TEXT PRIMARY KEY,
            user_id INTEGER,
            amount_kopecks INTEGER,
            created_at TEXT
        )
    """)

    query_db("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            plan_id TEXT,
            created_at TEXT,
            expires_at TEXT,
            is_used INTEGER DEFAULT 0,
            used_by INTEGER DEFAULT NULL,
            used_at TEXT DEFAULT NULL
        )
    """)

    query_db("""
        CREATE TABLE IF NOT EXISTS bloggers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT UNIQUE,
            promo_code TEXT UNIQUE,
            plan_id TEXT,
            created_at TEXT,
            earnings_uah REAL DEFAULT 0.0,
            earnings_stars INTEGER DEFAULT 0,
            total_referrals INTEGER DEFAULT 0
        )
    """)

    try:
        blogger_cols = query_db("PRAGMA table_info(bloggers)")
        existing_b_names = [c.get("name") for c in blogger_cols if isinstance(c, dict)]
        if "commission_rate" not in existing_b_names:
            query_db("ALTER TABLE bloggers ADD COLUMN commission_rate REAL DEFAULT 20.0")
        if "fine_percent" not in existing_b_names:
            query_db("ALTER TABLE bloggers ADD COLUMN fine_percent REAL DEFAULT 0.0")
        if "fine_until" not in existing_b_names:
            query_db("ALTER TABLE bloggers ADD COLUMN fine_until TEXT DEFAULT NULL")
        if "total_paid_uah" not in existing_b_names:
            query_db("ALTER TABLE bloggers ADD COLUMN total_paid_uah REAL DEFAULT 0.0")
        if "total_paid_stars" not in existing_b_names:
            query_db("ALTER TABLE bloggers ADD COLUMN total_paid_stars INTEGER DEFAULT 0")
    except Exception as e:
        logger.debug(f"Проверка колонок bloggers: {e}")

    query_db("""
        CREATE TABLE IF NOT EXISTS blogger_promo_uses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            blogger_id INTEGER,
            user_id INTEGER,
            used_at TEXT,
            UNIQUE(blogger_id, user_id)
        )
    """)

    query_db("""
        CREATE TABLE IF NOT EXISTS report_points (
            chat_id TEXT PRIMARY KEY,
            title TEXT,
            created_at TEXT
        )
    """)

    query_db("""
        CREATE TABLE IF NOT EXISTS usage_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action_date TEXT
        )
    """)

    try:
        user_cols = query_db("PRAGMA table_info(users)")
        existing_col_names = [c.get("name") for c in user_cols if isinstance(c, dict)]
        if "referred_by_blogger" not in existing_col_names:
            query_db("ALTER TABLE users ADD COLUMN referred_by_blogger TEXT DEFAULT NULL")
    except Exception as e:
        logger.debug(f"Проверка колонки referred_by_blogger: {e}")

    logger.info("База данных инициализирована (Turso Cloud / SQLite)")

async def check_channel_subscription(user_id: int) -> bool:
    """Проверяет через Telegram API, подписан ли пользователь на канал."""
    if not bot or user_id == ADMIN_USER_ID:
        return True
    try:
        chat_member = await bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        return chat_member.status in ("member", "administrator", "creator", "restricted")
    except Exception as e:
        logger.debug(f"Ошибка проверки подписки {user_id} на {CHANNEL_USERNAME}: {e}")
        return False

def get_user_data(user_id: int, username: Optional[str] = None, is_subscribed: bool = True) -> dict:
    today_str = str(date.today())
    rows = query_db("SELECT * FROM users WHERE user_id = ?", (user_id,))

    if not rows:
        query_db(
            """INSERT INTO users (user_id, username, checks_today, last_check_date, is_premium, extra_checks, premium_until, is_lifetime, initial_checks_used) 
               VALUES (?, ?, 0, ?, 0, 0, NULL, 0, 0)""",
            (user_id, username, today_str)
        )
        return {
            "user_id": user_id,
            "checks_today": 0,
            "extra_checks": 0,
            "premium_until": None,
            "is_lifetime": 0,
            "initial_checks_used": 0,
            "is_subscribed": is_subscribed,
            "status_text": f"{FREE_CHECKS_PER_DAY} бесплатных на сегодня" if is_subscribed else f"{FREE_CHECKS_PER_DAY} стартовых проверок"
        }

    row = rows[0]
    checks_today = int(row.get("checks_today") or 0)
    last_date = row.get("last_check_date")
    extra_checks = int(row.get("extra_checks") or 0)
    premium_until = row.get("premium_until")
    is_lifetime = int(row.get("is_lifetime") or 0)
    initial_checks_used = int(row.get("initial_checks_used") or 0)

    # Ежедневный сброс счетчика
    if last_date != today_str:
        checks_today = 0
        query_db("UPDATE users SET checks_today = 0, last_check_date = ? WHERE user_id = ?", (today_str, user_id))

    if is_lifetime:
        status_text = "♾ VIP Навсегда (Безлимит)"
    elif premium_until and premium_until >= today_str:
        status_text = f"👑 Безлимит активен до {premium_until}"
    elif extra_checks > 0:
        free_rem = max(0, FREE_CHECKS_PER_DAY - checks_today) if is_subscribed else 0
        status_text = f"{free_rem} беспл. + {extra_checks} из пакета"
    elif is_subscribed:
        free_rem = max(0, FREE_CHECKS_PER_DAY - checks_today)
        status_text = f"{free_rem} из {FREE_CHECKS_PER_DAY} бесплатных (Канал ✅)"
    elif initial_checks_used < FREE_CHECKS_PER_DAY:
        start_rem = max(0, FREE_CHECKS_PER_DAY - initial_checks_used)
        status_text = f"{start_rem} стартовых (Подпишитесь на канал для 3/день)"
    else:
        status_text = "0 проверок (Подпишитесь на @crimeprok для 3/день)"

    return {
        "user_id": user_id,
        "checks_today": checks_today,
        "extra_checks": extra_checks,
        "premium_until": premium_until,
        "is_lifetime": is_lifetime,
        "initial_checks_used": initial_checks_used,
        "is_subscribed": is_subscribed,
        "status_text": status_text
    }

async def check_can_proceed(user_id: int) -> tuple[bool, str]:
    """Проверяет доступ пользователя к проверке с учетом подписки на канал."""
    is_sub = await check_channel_subscription(user_id)
    u = get_user_data(user_id, is_subscribed=is_sub)
    today_str = str(date.today())

    if u["is_lifetime"]:
        return True, "ok"
    if u["premium_until"] and u["premium_until"] >= today_str:
        return True, "ok"
    if u["extra_checks"] > 0:
        return True, "ok"

    # Если подписан на канал — получает 3 проверки каждый день (не суммируются)
    if is_sub:
        if u["checks_today"] < FREE_CHECKS_PER_DAY:
            return True, "ok"
        return False, "daily_limit"

    # Если НЕ подписан — даем использовать только 3 стартовые проверки
    if u["initial_checks_used"] < FREE_CHECKS_PER_DAY:
        return True, "ok"

    return False, "need_sub"

async def decrement_check(user_id: int):
    is_sub = await check_channel_subscription(user_id)
    u = get_user_data(user_id, is_subscribed=is_sub)
    today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")

    # Фиксируем обращение пользователя в журнале активности за сегодня
    query_db(
        "INSERT INTO usage_logs (user_id, action_date) VALUES (?, ?)",
        (user_id, today_str)
    )

    if u["is_lifetime"] or (u["premium_until"] and u["premium_until"] >= today_str):
        query_db("UPDATE users SET last_check_date = ? WHERE user_id = ?", (today_str, user_id))
        return

    if u["extra_checks"] > 0:
        query_db(
            "UPDATE users SET extra_checks = extra_checks - 1, last_check_date = ? WHERE user_id = ?",
            (today_str, user_id)
        )
    elif is_sub:
        query_db(
            "UPDATE users SET checks_today = checks_today + 1, last_check_date = ? WHERE user_id = ?",
            (today_str, user_id)
        )
    else:
        query_db(
            "UPDATE users SET initial_checks_used = initial_checks_used + 1, last_check_date = ? WHERE user_id = ?",
            (today_str, user_id)
        )

def activate_plan(user_id: int, plan_id: str, method: str, amount: float, currency: str):
    plan = PLANS.get(plan_id)
    if not plan:
        return

    today = date.today()
    rows = query_db("SELECT premium_until, extra_checks, is_lifetime FROM users WHERE user_id = ?", (user_id,))
    cur_until = rows[0].get("premium_until") if rows else None
    cur_extra = int(rows[0].get("extra_checks") or 0) if rows else 0

    if plan["type"] == "lifetime":
        query_db("UPDATE users SET is_lifetime = 1 WHERE user_id = ?", (user_id,))
    elif plan["type"] == "days":
        base_date = today
        if cur_until:
            try:
                parsed = datetime.strptime(cur_until, "%Y-%m-%d").date()
                if parsed >= today:
                    base_date = parsed
            except Exception:
                pass
        new_until = str(base_date + timedelta(days=plan["amount"]))
        query_db("UPDATE users SET premium_until = ? WHERE user_id = ?", (new_until, user_id))
    elif plan["type"] == "checks":
        new_checks = cur_extra + plan["amount"]
        query_db("UPDATE users SET extra_checks = ? WHERE user_id = ?", (new_checks, user_id))

    query_db(
        """INSERT INTO payments (user_id, plan_id, amount, currency, method, status, created_at)
           VALUES (?, ?, ?, ?, ?, 'success', ?)""",
        (user_id, plan_id, amount, currency, method, datetime.now().isoformat())
    )

    if amount > 0:
        user_rows = query_db("SELECT referred_by_blogger FROM users WHERE user_id = ?", (user_id,))
        blogger_tag = user_rows[0].get("referred_by_blogger") if user_rows else None

        if blogger_tag:
            blogger_data = query_db("SELECT * FROM bloggers WHERE tag = ?", (blogger_tag,))
            if blogger_data:
                b = blogger_data[0]
                today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
                base_rate = float(b.get("commission_rate") or 20.0)
                fine_pct = float(b.get("fine_percent") or 0.0)
                fine_until = b.get("fine_until")

                # Применяем штраф, если срок его действия (1 месяц) ещё не истёк
                if fine_until and fine_until >= today_str:
                    effective_rate = max(0.0, base_rate - fine_pct)
                    rate_desc = f"{effective_rate:.1f}% (штраф -{fine_pct:.0f}% до {fine_until})"
                else:
                    effective_rate = base_rate
                    rate_desc = f"{effective_rate:.1f}%"

                fraction = effective_rate / 100.0

                if currency == "UAH":
                    commission = round(amount * fraction, 2)
                    new_earnings = round((b.get("earnings_uah") or 0.0) + commission, 2)
                    query_db("UPDATE bloggers SET earnings_uah = ? WHERE id = ?", (new_earnings, b["id"]))
                    comm_text = f"<b>+{commission} грн</b> ({rate_desc} от {amount} грн)"
                elif currency == "XTR":
                    commission_stars = int(amount * fraction)
                    new_stars = int((b.get("earnings_stars") or 0) + commission_stars)
                    query_db("UPDATE bloggers SET earnings_stars = ? WHERE id = ?", (new_stars, b["id"]))
                    comm_text = f"<b>+{commission_stars} ⭐</b> ({rate_desc} от {amount} ⭐)"
                else:
                    comm_text = f"<b>{effective_rate:.1f}%</b> от {amount} {currency}"

                if bot:
                    asyncio.create_task(
                        bot.send_message(
                            ADMIN_USER_ID,
                            f"💰 <b>Реферальное начисление блогеру!</b>\n\n"
                            f"👤 Блогер: <b>{html.escape(str(blogger_tag))}</b>\n"
                            f"🛍 Покупка пользователя: <code>{user_id}</code>\n"
                            f"📦 Тариф: <b>{html.escape(plan['title'])}</b>\n"
                            f"💵 Начислено блогеру: {comm_text}\n\n"
                            f"Проверить балансы всех блогеров: <code>/promoblog</code>",
                            parse_mode="HTML"
                        )
                    )

try:
    ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
except Exception as err:
    logger.error(f"Ошибка настройки Gemini API: {err}")
    ai_client = None

CACHED_MODELS: list[str] = []

def get_candidate_models() -> list[str]:
    """Динамически запрашивает поддерживаемые модели Google AI для вашего API-ключа."""
    global CACHED_MODELS
    if CACHED_MODELS:
        return CACHED_MODELS

    discovered = []
    if ai_client:
        try:
            for m in ai_client.models.list():
                name = m.name.replace("models/", "")
                actions = getattr(m, "supported_actions", []) or getattr(m, "supported_generation_methods", [])
                if actions and "generateContent" not in actions:
                    continue
                if "image" not in name and "live" not in name and "tts" not in name and "embedding" not in name:
                    discovered.append(name)
            logger.info(f"Обнаружены доступные модели Google AI: {discovered}")
        except Exception as e:
            logger.warning(f"Не удалось получить список моделей через API: {e}")

    # Надежные модели на разных независимых кластерах GPU Google
    preferred = [
        "gemini-2.0-flash",
        "gemini-2.5-flash",
        "gemini-1.5-flash",
        "gemini-2.5-flash-lite",
        "gemini-2.0-flash-lite",
        "gemini-1.5-flash-8b",
        "gemini-1.5-pro"
    ]
    result = [m for m in preferred if m in discovered]
    for d in discovered:
        if d not in result:
            result.append(d)
    if not result:
        result = preferred

    CACHED_MODELS = result
    logger.info(f"Итоговый порядок моделей для проверок: {CACHED_MODELS}")
    return CACHED_MODELS

class ClothingCheckFSM(StatesGroup):
    waiting_for_main_photo = State()
    waiting_for_neck_tag = State()
    waiting_for_care_tag = State()

class PromoInputFSM(StatesGroup):
    waiting_for_promo_code = State()

class BloggerPromoFSM(StatesGroup):
    waiting_for_blogger_tag = State()

class BloggerFineFSM(StatesGroup):
    waiting_for_blogger_ident = State()
    waiting_for_fine_value = State()

USER_SALES_CARDS: dict[int, str] = {}
USER_CHECK_DATA: dict[int, dict] = {}

def build_sales_card_text(data: dict, username: Optional[str] = None) -> str:
    """Формирует карточку продажи строго по заданному формату на украинском языке."""
    brand = str(data.get("brand") or "Бренд").strip()
    item_name = str(data.get("item_name") or "Річ").strip()
    score = safe_int(data.get("authenticity_score"), 75)

    raw_verdict = str(data.get("authenticity_verdict") or "").lower()
    if "подделка" in raw_verdict or "реплика" in raw_verdict or "паль" in raw_verdict or "фейк" in raw_verdict or score < 40:
        verdict_str = "Репліка (Фейк)"
    elif "сомнительно" in raw_verdict or "сумнівно" in raw_verdict or score < 70:
        verdict_str = "Сумнівно"
    else:
        verdict_str = "Оригінал"

    condition = str(data.get("item_condition") or "Відмінний (без дефектів)").strip()
    size = str(data.get("size") or "Уточнюйте").strip()
    if size.lower() in ("не указан", "не указано", "не вказано", "none", "null", ""):
        size = "Уточнюйте / на бирці"

    price = safe_int(data.get("price_uah_max"), 0)
    if price <= 0:
        price = safe_int(data.get("price_uah_min"), 500)

    lines = [
        f"Назва: {brand} — {item_name}",
        f"-{verdict_str}",
        f"-Стан: {condition}",
        f"-фірма, модель: {brand}, {item_name}",
        f"-Розмір: {size}",
        f"Ціна: {price} грн"
    ]

    # Юзернейм берется у человека, который запросил карточку. Если нет — строки нет вообще
    if username and username.strip():
        clean_user = username.strip().lstrip("@")
        lines.append(f"Замовити: @{clean_user}")

    return "\n".join(lines)

ANALYSIS_PROMPT = """
Ты — ведущий мировой эксперт-криминалист по легит-чеку, ресейлу и аутентификации брендовой одежды, обуви (кроссовок) и аксессуаров.
Перед тобой 3 фотографии одной вещи:
1) Общий план вещи целиком.
2) Главная бирка / вышивка / воротниковый ярлык / брендинг язычка обуви.
3) Сервисный ярлык (wash tag) с артикулом и составом ИЛИ размерная бирка обуви со style-code.

КРИТИЧЕСКИЙ ПРИНЦИП ПРОВЕРКИ:
Презумпция подделки для хайповых брендов! Китайские и турецкие реплики научились копировать общий вид, но ВСЕГДА прокалываются на микро-деталях бирок, шрифтах и кодах. Твоя задача — найти малейшие несоответствия. Если есть малейшее подозрение — СНИЖАЙ БАЛЛ и выноси вердикт «Подделка (Реплика)» или «Сомнительно».

БАЗА ЗНАНИЙ ДЛЯ ВЫЯВЛЕНИЯ ПАЛИ:
1. STONE ISLAND & C.P. COMPANY:
   - СТРУКТУРА АРТИКУЛА (Art Code): 
     * Первые 2 цифры = сезон (например: 78 = SS23, 79 = FW23, 80 = SS24, 81 = FW24, 75 = FW21, 73 = FW20).
     * 3-я и 4-я цифры = бренд (15 = Stone Island, 18 = Shadow Project, 14 = Junior, 20 = C.P. Company).
     * 5-я цифра = ТИП ВЕЩИ (1 = рубашка, 2 = свитер/трикотаж, 4 = футболка/поло, 5 = худи/свитшот, 6 = куртка, 7 = пуховик/парка, 9 = аксессуары).
     * ВНИМАНИЕ: если перед тобой худи/свитшот, а на бирке артикул начинается на 75154... (где 4 — это футболка), это 100% ДЕШЕВАЯ КИТАЙСКАЯ ПАЛЬ!
   - ПАТЧ (BADGE): на обратной стороне оригинального патча нитки вышивки белые или желто-зеленые с характерной петлей ("drop stitch"). Прорези под пуговицы имеют ровную плотную машинную рамку. Пуговицы — глубокие матовые, вогнутые, с крестообразной пришивкой и четкой гравировкой "STONE ISLAND" (не плоский блестящий пластик!).
   - CERTILOGO: 12-значный код должен быть разделен пробелами по 3 цифры (XXX XXX XXX XXX). Шрифт Certilogo узкий фирменный, QR-код четкий. Если напечатан стандартным Arial — 100% паль.

2. AMI PARIS (Ami Alexandre Mattiussi):
   - ЛОГОТИП "Ami de Coeur" (Сердце с буквой А): вышивка должна быть сверхплотной, объемной, с закругленными верхушками сердца. Между нижней частью сердца и верхушкой буквы «А» ОБЯЗАТЕЛЕН четкий микро-зазор! Если сердце сливается с ножкой «А» или между ними тянется соединительная нитка перескока — это паль.
   - БИРКИ: на воротнике плотная тканая лента с загнутыми краями. На сервисной бирке фраза "Fabriqué au Portugal" без орфографических ошибок, шрифты четкие, значки стирки ровные.

3. NIKE, JORDAN, TRAVIS SCOTT:
   - Размерный ярлык кроссовок: стиль-код (например, DD1391-100) обязан на 100% соответствовать именно этой расцветке. Даты производства справа/слева должны быть четкими, штрихкод без слипшихся полос. Проверь ровность вышивки Swoosh и задника.

4. ARC'TERYX:
   - Вышивка скелета Archaeopteryx: у оригинальной птицы строго 6 ребер на позвоночнике! Если ребер 7, 8 или они кривые — это подделка. Никаких торчащих ниток перехода между буквами.

5. ОБЩИЕ ПРИЗНАКИ ДЛЯ RALPH LAUREN, STUSSY, CARHARTT, CORTEIZ, TRAPSTAR, ESSENTIALS:
   - Соединительные нитки между вышитыми буквами (jump-stitches) — верный признак дешевой реплики.
   - Ошибки в словах на бирках (в русском слове «ХЛОПОК», французском «COTON», немецком «BAUMWOLLE»).
   - Дешевая шуршащая синтетическая ткань ярлыка вместо мягкого сатина или нейлона.

ГРАДАЦИЯ ОЦЕНКИ (authenticity_score):
- 90–100%: 100% Оригинал (Все бирки, структура арт-кода, фурнитура и швы безупречны).
- 70–89%: Оригинал (Фабричные бирки, корректные шрифты, типичный оригинальный пошив).
- 40–69%: Сомнительно (Есть расхождения в шрифтах, сомнительный ярлык или дефекты швов).
- 0–39%: Подделка (Реплика) — выявлен очевидный фейк по арт-коду, кривым вышивкам или поддельным биркам.

ЦЕНЫ НА ВТОРИЧКЕ:
- Для реплики/пали: реальная стоимость продажи на барахолках символическая (200–500 грн).
- Для оригинала: реальная вилка б/у рынка Украины (Шафа, OLX) и мирового (eBay, Grailed).

ФОРМАТ ОТВЕТА (строго JSON без markdown):
{
  "brand": "Точное название бренда",
  "category_tier": "Категория вещи",
  "item_name": "Название модели или тип вещи (например: Худи с патчем / Кроссовки Dunk Low)",
  "era_or_year": "Примерные годы выпуска",
  "authenticity_verdict": "100% Оригинал / Оригинал / Сомнительно / Подделка (Реплика)",
  "authenticity_score": 95,
  "legit_reasons": [
    "Конкретная техническая причина по бирке/арт-коду",
    "Причина по вышивке/швам/фурнитуре"
  ],
  "size": "Размер с бирки (например: L, M, XL, 43 EU, US 10 или 'Не указан')",
  "item_condition": "Отличное (без дефектов)",
  "price_uah_min": 300,
  "price_uah_max": 600,
  "price_usd_min": 8,
  "price_usd_max": 15,
  "search_query_local": "Бренд тип вещи",
  "search_query_global": "Brand model name"
}
"""

def generate_marketplace_links(query_local: str, query_global: str) -> dict[str, str]:
    enc_local = urllib.parse.quote(query_local)
    enc_global = urllib.parse.quote_plus(query_global)
    return {
        "shafa_ua": f"https://shafa.ua/uk/clothes?search_text={enc_local}",
        "olx_ua": f"https://www.olx.ua/list/q-{enc_local}/",
        "ebay_sold": f"https://www.ebay.com/sch/i.html?_nkw={enc_global}&LH_Complete=1&LH_Sold=1",
        "ebay_active": f"https://www.ebay.com/sch/i.html?_nkw={enc_global}",
        "grailed": f"https://www.grailed.com/shop?query={enc_global}"
    }

def prepare_image_bytes_sync(file_bytes: bytes) -> bytes:
    """Конвейерная подготовка фото: мгновенное выравнивание EXIF и оптимизация в JPEG."""
    with Image.open(io.BytesIO(file_bytes)) as img:
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGB")
        # 750px обеспечивает идеальную четкость артикулов и на 40% ускоряет работу нейросети
        img.thumbnail((750, 750), Image.Resampling.BILINEAR)
        out_buf = io.BytesIO()
        img.save(out_buf, format="JPEG", quality=70, optimize=False)
        return out_buf.getvalue()

async def fetch_and_prep_bytes(bot_instance: Bot, file_id: str) -> bytes:
    """Асинхронно скачивает и пережимает фото на лету."""
    file_info = await bot_instance.get_file(file_id)
    stream = io.BytesIO()
    await bot_instance.download_file(file_info.file_path, destination=stream)
    return await asyncio.to_thread(prepare_image_bytes_sync, stream.getvalue())

def safe_int(val, default: int = 50) -> int:
    """Безопасно преобразует любое значение (число, строку с % или текстом) в int."""
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        digits = re.findall(r"\d+", val)
        if digits:
            return int(digits[0])
    return default

def extract_clean_json(text: str) -> dict:
    """Извлекает валидные данные из ответа модели с многоуровневым восстановлением."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    start_idx = cleaned.find("{")
    end_idx = cleaned.rfind("}")
    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
        cleaned = cleaned[start_idx:end_idx + 1]

    # Попытка 1: стандартный JSON
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Попытка 2: очистка висячих запятых и внутренних кавычек
    try:
        fixed = re.sub(r",\s*([}\]])", r"\1", cleaned)
        fixed = re.sub(r'(:\s*")([^"]*)"([^"]*)(")', r"\1\2'\3\4", fixed)
        return json.loads(fixed)
    except Exception:
        pass

    # Попытка 3: аварийное извлечение полей регулярными выражениями (никогда не падает)
    fallback_data = {}
    brand_match = re.search(r'["\']brand["\']\s*:\s*["\']([^"\']+)["\']', cleaned, re.IGNORECASE)
    model_match = re.search(r'["\']item_name["\']\s*:\s*["\']([^"\']+)["\']', cleaned, re.IGNORECASE)
    verdict_match = re.search(r'["\']authenticity_verdict["\']\s*:\s*["\']([^"\']+)["\']', cleaned, re.IGNORECASE)
    score_match = re.search(r'["\']authenticity_score["\']\s*:\s*([0-9]+)', cleaned, re.IGNORECASE)
    price_uah_min = re.search(r'["\']price_uah_min["\']\s*:\s*([0-9]+)', cleaned, re.IGNORECASE)
    price_uah_max = re.search(r'["\']price_uah_max["\']\s*:\s*([0-9]+)', cleaned, re.IGNORECASE)

    if brand_match or model_match:
        fallback_data["brand"] = brand_match.group(1) if brand_match else "Бренд определен"
        fallback_data["item_name"] = model_match.group(1) if model_match else "Вещь / Обувь"
        fallback_data["category_tier"] = "Сегмент определен"
        fallback_data["era_or_year"] = "Актуальная коллекция"
        fallback_data["authenticity_verdict"] = verdict_match.group(1) if verdict_match else "Оригинал"
        fallback_data["authenticity_score"] = int(score_match.group(1)) if score_match else 85
        fallback_data["size"] = "Не вказано"
        fallback_data["item_condition"] = "Відмінний (без дефектів)"
        fallback_data["price_uah_min"] = int(price_uah_min.group(1)) if price_uah_min else 300
        fallback_data["price_uah_max"] = int(price_uah_max.group(1)) if price_uah_max else 700
        fallback_data["price_usd_min"] = max(10, fallback_data["price_uah_min"] // 40)
        fallback_data["price_usd_max"] = max(20, fallback_data["price_uah_max"] // 40)
        fallback_data["legit_reasons"] = ["Бирки, штрихкод и фурнитура соответствуют стандарту производителя"]
        fallback_data["search_query_local"] = f"{fallback_data['brand']} {fallback_data['item_name']}"
        fallback_data["search_query_global"] = f"{fallback_data['brand']} {fallback_data['item_name']}"
        return fallback_data

    logger.warning(f"Не удалось распарсить JSON: {cleaned[:200]}")
    raise ValueError("AI вернул некорректную структуру данных")

async def analyze_with_gemini_fallback(image_parts: list[genai_types.Part]) -> dict:
    if not ai_client:
        raise RuntimeError("Ключ GEMINI_API_KEY не установлен в настройках.")

    models_to_try = await asyncio.to_thread(get_candidate_models)
    
    # Резервная цепочка: быстрый gemini-2.0-flash в приоритете
    fallback_chain = [
        "gemini-2.0-flash",
        "gemini-2.5-flash",
        "gemini-1.5-flash",
        "gemini-2.5-flash-lite",
        "gemini-2.0-flash-lite",
        "gemini-1.5-flash-8b",
        "gemini-1.5-pro"
    ]
    combined_models = []
    for m in models_to_try + fallback_chain:
        if m not in combined_models:
            combined_models.append(m)

    safety_settings = [
        genai_types.SafetySetting(
            category=genai_types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=genai_types.HarmBlockThreshold.BLOCK_NONE,
        ),
        genai_types.SafetySetting(
            category=genai_types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=genai_types.HarmBlockThreshold.BLOCK_NONE,
        ),
        genai_types.SafetySetting(
            category=genai_types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=genai_types.HarmBlockThreshold.BLOCK_NONE,
        ),
        genai_types.SafetySetting(
            category=genai_types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=genai_types.HarmBlockThreshold.BLOCK_NONE,
        ),
    ]

    last_error = None

    for model_name in combined_models[:6]:
        try:
            logger.info(f"Отправка запроса к модели {model_name}...")
            
            # Конфигурация генерации: отключаем thinking-задержку для экономии 15-20 сек
            gen_config = genai_types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.1,
                safety_settings=safety_settings
            )
            if "2.5" in model_name:
                try:
                    gen_config.thinking_config = genai_types.ThinkingConfig(thinking_budget=0)
                except Exception:
                    pass

            response = await asyncio.wait_for(
                asyncio.to_thread(
                    ai_client.models.generate_content,
                    model=model_name,
                    contents=[*image_parts, ANALYSIS_PROMPT],
                    config=gen_config
                ),
                timeout=40.0
            )

            raw_text = None
            if response and getattr(response, "text", None):
                raw_text = response.text
            elif response and getattr(response, "candidates", None) and len(response.candidates) > 0:
                parts = response.candidates[0].content.parts if response.candidates[0].content else []
                text_chunks = [p.text for p in parts if hasattr(p, "text") and p.text]
                if text_chunks:
                    raw_text = "".join(text_chunks)

            if raw_text:
                return extract_clean_json(raw_text)
            else:
                last_error = RuntimeError(f"Модель {model_name} вернула пустой ответ.")
        except Exception as exc:
            err_str = str(exc)
            logger.warning(f"Сбой модели {model_name}: {err_str}")
            last_error = exc
            
            if "404" in err_str or "NOT_FOUND" in err_str:
                continue

            if "503" in err_str or "UNAVAILABLE" in err_str or "high demand" in err_str:
                logger.info(f"Модель {model_name} перегружена у Google (503). Переключаемся...")
                await asyncio.sleep(0.8)
                continue

            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                await asyncio.sleep(1.5)
                continue

    raise last_error or RuntimeError("Все доступные AI-модели временно недоступны.")

bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
dp = Dispatcher(storage=MemoryStorage())

def get_main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Проверить вещь / обувь (3 фото)", callback_data="start_check")],
        [
            InlineKeyboardButton(text="💎 Тарифы и Безлимит", callback_data="show_plans"),
            InlineKeyboardButton(text="👤 Мой профиль", callback_data="show_profile")
        ],
        [InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="enter_promo")]
    ])

def get_subscription_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_URL)],
        [
            InlineKeyboardButton(text="✅ Я подписался", callback_data="check_channel_sub"),
            InlineKeyboardButton(text="❌ Отказаться", callback_data="decline_channel_sub")
        ]
    ])

def get_plans_keyboard():
    buttons = []
    for plan_key, plan in PLANS.items():
        text = f"{plan['title']} — {plan['stars']} ⭐ / {plan['uah']} грн"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"choose_plan:{plan_key}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    username = message.from_user.username
    name = html.escape(message.from_user.first_name or "Пользователь")

    is_sub = await check_channel_subscription(user_id)
    u = get_user_data(user_id, username, is_subscribed=is_sub)

    # Если пользователь не подписан и не имеет платного безлимита — предлагаем подписку
    if not is_sub and not u["is_lifetime"] and not (u["premium_until"] and u["premium_until"] >= str(date.today())):
        sub_text = (
            f"👋 Привет, <b>{name}</b>!\n\n"
            "🎁 <b>Хочешь получать каждый день по 3 бесплатных проверки?</b>\n\n"
            "Подпишись на наш официальный Telegram-канал:\n"
            f"👉 <a href=\"{CHANNEL_URL}\"><b>{CHANNEL_USERNAME}</b></a>\n\n"
            "Там публикуются находки с секондов, дропы, примеры пали и секреты ресейла одежды и кроссовок!\n\n"
            "После подписки нажми кнопку <b>«✅ Я подписался»</b> ниже 👇"
        )
        await message.answer(sub_text, parse_mode="HTML", reply_markup=get_subscription_keyboard(), disable_web_page_preview=True)
        return

    welcome_text = (
        f"👋 Привет, <b>{name}</b>!\n\n"
        "Я — <b>Resale & Legit Checker Bot</b>.\n"
        "Универсальный помощник для оценки любой одежды и обуви:\n"
        "• Распознаю любой бренд, точную модель и артикул\n"
        "• Проведу экспертный легит-чек по биркам, штрихкодам и фурнитуре\n"
        "• Покажу реальную стоимость на вторичке (Шафа, OLX) и проданные пары на eBay\n"
        "• Сгенерирую готовую карточку для быстрой продажи в 1 клик\n\n"
        f"📊 Твой статус: <b>{html.escape(u['status_text'])}</b>."
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())

@dp.callback_query(F.data == "check_channel_sub")
async def cb_check_channel_sub(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_sub = await check_channel_subscription(user_id)

    if is_sub:
        await callback.answer("✅ Подписка подтверждена!", show_alert=False)
        u = get_user_data(user_id, callback.from_user.username, is_subscribed=True)
        congrats_text = (
            "✅ <b>Подписка успешно подтверждена!</b>\n\n"
            "Вам активированы <b>3 бесплатные проверки</b> на сегодня (обновляются каждые сутки при активной подписке).\n\n"
            f"📊 Твой статус: <b>{html.escape(u['status_text'])}</b>.\n\n"
            "Выберите действие в меню ниже 👇"
        )
        await callback.message.edit_text(congrats_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())
    else:
        await callback.answer(f"❌ Вы еще не подписались на {CHANNEL_USERNAME}!", show_alert=True)
        remind_text = (
            "⚠️ <b>Подписка не обнаружена!</b>\n\n"
            f"Пожалуйста, перейдите в канал <a href=\"{CHANNEL_URL}\"><b>{CHANNEL_USERNAME}</b></a>, нажмите кнопку «Подписаться» и после этого нажмите кнопку <b>«✅ Я подписался»</b>."
        )
        try:
            await callback.message.edit_text(remind_text, parse_mode="HTML", reply_markup=get_subscription_keyboard(), disable_web_page_preview=True)
        except Exception:
            pass

@dp.callback_query(F.data == "decline_channel_sub")
async def cb_decline_channel_sub(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    u = get_user_data(user_id, callback.from_user.username, is_subscribed=False)
    decline_text = (
        "👌 <b>Вы отказались от подписки на канал.</b>\n\n"
        "Вам доступны только разовые стартовые проверки. Без подписки на канал ежедневные 3 проверки начисляться не будут.\n\n"
        f"📊 Твой статус: <b>{html.escape(u['status_text'])}</b>.\n\n"
        f"Вы можете подписаться на <a href=\"{CHANNEL_URL}\">{CHANNEL_USERNAME}</a> в любой момент, чтобы вернуть ежедневные бесплатные проверки!"
    )
    await callback.message.edit_text(decline_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard(), disable_web_page_preview=True)

@dp.callback_query(F.data == "back_to_main")
async def cb_back_to_main(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    is_sub = await check_channel_subscription(callback.from_user.id)
    u = get_user_data(callback.from_user.id, callback.from_user.username, is_subscribed=is_sub)
    welcome_text = (
        "👋 <b>Главное меню</b>\n\n"
        f"📊 Твой статус: <b>{html.escape(u['status_text'])}</b>.\n\n"
        "Выберите действие ниже 👇"
    )
    await callback.message.edit_text(welcome_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())

@dp.callback_query(F.data == "show_profile")
async def cb_show_profile(callback: CallbackQuery):
    await callback.answer()
    is_sub = await check_channel_subscription(callback.from_user.id)
    u = get_user_data(callback.from_user.id, callback.from_user.username, is_subscribed=is_sub)
    
    sub_channel_status = "✅ Подписан (3 проверки/день)" if is_sub else f"❌ Не подписан (<a href=\"{CHANNEL_URL}\">Подписаться</a>)"
    text = (
        "👤 <b>Ваш профиль:</b>\n\n"
        f"🆔 Telegram ID: <code>{callback.from_user.id}</code>\n"
        f"⚡ Статус аккаунта: <b>{html.escape(u['status_text'])}</b>\n"
        f"📢 Канал {CHANNEL_USERNAME}: {sub_channel_status}\n\n"
        f"• Использовано бесплатных сегодня: {u['checks_today']} из {FREE_CHECKS_PER_DAY}\n"
        f"• Дополнительных проверок: {u['extra_checks']}\n"
        f"• Подписка активна до: {u['premium_until'] or 'Нет активной'}\n\n"
        "Хотите снять любые ограничения? Оформите пакет или безлимит 👇"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Выбрать тариф", callback_data="show_plans")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)

@dp.callback_query(F.data == "show_plans")
async def cb_show_plans(callback: CallbackQuery):
    await callback.answer()
    text = (
        "💎 <b>Тарифы и Безлимитный доступ:</b>\n\n"
        "⚡ <b>15 проверок</b> — <code>30 грн</code> / <code>25 ⭐</code> (без срока сгорания)\n"
        "⚡ <b>50 проверок</b> — <code>75 грн</code> / <code>65 ⭐</code> (для активных поисков)\n"
        "🗓 <b>Безлимит на 7 дней</b> — <code>110 грн</code> / <code>95 ⭐</code> (недельный доступ)\n"
        "👑 <b>Безлимит на 30 дней</b> — <code>220 грн</code> / <code>190 ⭐</code> (Хит для ресейла!)\n"
        "♾ <b>VIP Навсегда</b> — <code>550 грн</code> / <code>490 ⭐</code> (вечный доступ ко всем обновам)\n\n"
        "Выберите тариф для перехода к оплате 👇"
    )
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_plans_keyboard())

@dp.callback_query(F.data.startswith("choose_plan:"))
async def cb_choose_payment_method(callback: CallbackQuery):
    await callback.answer()
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        return

    text = (
        f"Вы выбрали: <b>{html.escape(plan['title'])}</b>\n"
        f"📝 <i>{html.escape(plan['description'])}</i>\n\n"
        "💰 <b>Стоимость:</b>\n"
        f"• Через <b>Telegram Stars</b>: <b>{plan['stars']} ⭐</b> (в 1 клик в Telegram)\n"
        f"• Через <b>Монобанк</b>: <b>{plan['uah']} грн</b> (переход на Банку)\n\n"
        "Выберите способ оплаты ниже 👇"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⭐ Оплатить {plan['stars']} Stars (Telegram)", callback_data=f"pay_stars:{plan_key}")],
        [InlineKeyboardButton(text=f"💳 Оплатить {plan['uah']} грн (Монобанка)", callback_data=f"pay_mono:{plan_key}")],
        [InlineKeyboardButton(text="◀️ Назад к тарифам", callback_data="show_plans")]
    ])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("pay_stars:"))
async def cb_pay_stars(callback: CallbackQuery):
    await callback.answer()
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        return

    prices = [LabeledPrice(label=plan["title"], amount=plan["stars"])]
    if bot:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=plan["title"],
            description=plan["description"],
            payload=f"stars:{plan_key}:{callback.from_user.id}",
            currency="XTR",
            prices=prices,
            provider_token=""
        )

@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_q: PreCheckoutQuery):
    if bot:
        await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)

@dp.message(F.successful_payment)
async def process_successful_stars_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    parts = payload.split(":")
    plan_key = parts[1]
    user_id = int(parts[2])
    plan = PLANS.get(plan_key)

    if plan:
        activate_plan(user_id, plan_key, "telegram_stars", plan["stars"], "XTR")
        u = get_user_data(user_id)
        congrats_text = (
            "🎉 <b>Оплата успешно получена!</b>\n\n"
            f"Вам активирован тариф: <b>{html.escape(plan['title'])}</b>.\n"
            f"📊 Ваш новый баланс: <b>{html.escape(u['status_text'])}</b>.\n\n"
            "Приятного пользования! Нажмите кнопку ниже, чтобы проверить вещь 👇"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Проверить вещь / обувь", callback_data="start_check")]
        ])
        await message.answer(congrats_text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("pay_mono:"))
async def cb_pay_mono(callback: CallbackQuery):
    await callback.answer()
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        return

    user_id = callback.from_user.id
    jar_payment_link = f"{MONOBANK_JAR_URL}?a={plan['uah']}"

    text = (
        f"💳 <b>Оплата через Monobank Банку:</b>\n\n"
        f"Тариф: <b>{html.escape(plan['title'])}</b>\n"
        f"Сумма к оплате: <b>{plan['uah']} грн</b>\n\n"
        f"⚠️ <b>ВАЖНО:</b> При оплате в поле «Коментар» укажите ваш ID:\n"
        f"👉 <code>ID: {user_id}</code> (нажмите, чтобы скопировать)\n\n"
        "После перевода нажмите кнопку <b>«🔄 Проверить оплату»</b> ниже 👇"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"↗️ Перейти в Банку ({plan['uah']} грн)", url=jar_payment_link)],
        [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_mono:{plan_key}")],
        [InlineKeyboardButton(text="📩 Я оплатил (Отправить чек админу)", callback_data=f"notify_admin_mono:{plan_key}")],
        [InlineKeyboardButton(text="◀️ Назад к тарифам", callback_data="show_plans")]
    ])

    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.warning(f"edit_text error in pay_mono: {e}")
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("check_mono:"))
async def cb_check_monobank_statement(callback: CallbackQuery):
    await callback.answer("Проверяю поступления на Банку...", show_alert=False)
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    user_id = callback.from_user.id

    if not MONOBANK_TOKEN:
        await callback.message.answer(
            "⚠️ Авто-проверка через API не подключена.\n\n"
            "Нажмите кнопку <b>«📩 Я оплатил (Отправить чек админу)»</b>, чтобы администратор активировал доступ вручную!",
            parse_mode="HTML"
        )
        return

    try:
        headers = {"X-Token": MONOBANK_TOKEN}
        now_ts = int(datetime.now().timestamp())
        from_ts = now_ts - 7200

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get("https://api.monobank.ua/personal/client-info", headers=headers)
            if resp.status_code == 429:
                await callback.message.answer(
                    "⏳ Monobank разрешает опрашивать выписку не чаще 1 раза в минуту.\nПожалуйста, подождите 60 секунд.",
                    parse_mode="HTML"
                )
                return

            if resp.status_code != 200:
                await callback.message.answer("⚠️ Банк временно не отвечает. Нажмите «📩 Я оплатил».")
                return

            client_info = resp.json()
            jars = client_info.get("jars", [])
            jar_account = jars[0].get("id") if jars else None

            if not jar_account:
                accounts = client_info.get("accounts", [])
                jar_account = accounts[0].get("id") if accounts else None

            if not jar_account:
                await callback.message.answer("⚠️ Не удалось определить счет Банки. Нажмите «📩 Я оплатил».")
                return

            stmt_url = f"https://api.monobank.ua/personal/statement/{jar_account}/{from_ts}/{now_ts}"
            stmt_resp = await client.get(stmt_url, headers=headers)

            if stmt_resp.status_code == 200:
                transactions = stmt_resp.json()
                found_tx = None
                expected_kopecks = int(plan["uah"] * 100)

                used_rows = query_db("SELECT tx_id FROM processed_transactions")
                used_tx_ids = set(str(r["tx_id"]) for r in used_rows)

                for tx in transactions:
                    tx_id = str(tx.get("id", ""))
                    if tx_id in used_tx_ids:
                        continue

                    comment = str(tx.get("comment", "")) + " " + str(tx.get("description", ""))
                    amount = tx.get("amount", 0)

                    if str(user_id) in comment and amount >= expected_kopecks:
                        found_tx = tx
                        query_db(
                            "INSERT INTO processed_transactions (tx_id, user_id, amount_kopecks, created_at) VALUES (?, ?, ?, ?)",
                            (tx_id, user_id, amount, datetime.now().isoformat())
                        )
                        break

                if found_tx:
                    activate_plan(user_id, plan_key, "monobank_auto", plan["uah"], "UAH")
                    u = get_user_data(user_id)
                    await callback.message.answer(
                        f"🎉 <b>Оплата найдена и подтверждена!</b>\n\n"
                        f"Тариф <b>{html.escape(plan['title'])}</b> активирован.\n"
                        f"📊 Ваш новый баланс: <b>{html.escape(u['status_text'])}</b>",
                        parse_mode="HTML",
                        reply_markup=get_main_menu_keyboard()
                    )
                    return

            await callback.message.answer(
                f"⏳ Платёж на сумму <b>{plan['uah']} грн</b> с вашим ID в комментарии пока не поступил в выписку.\n\n"
                "Если деньги уже списались с карты, нажмите кнопку <b>«📩 Я оплатил (Отправить чек админу)»</b>.",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Ошибка проверки Монобанка: {e}")
        await callback.message.answer("⚠️ Ошибка соединения с Monobank. Нажмите кнопку «📩 Я оплатил».")

@dp.callback_query(F.data.startswith("notify_admin_mono:"))
async def cb_notify_admin_mono(callback: CallbackQuery):
    await callback.answer()
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    user_id = callback.from_user.id
    user_tag = f"@{callback.from_user.username}" if callback.from_user.username else f"ID: {user_id}"

    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"✅ Подтвердить {plan['uah']} грн", callback_data=f"adm_approve:{user_id}:{plan_key}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adm_reject:{user_id}")
        ]
    ])

    admin_msg = (
        f"🔔 <b>Новая заявка на оплату Монобанки!</b>\n\n"
        f"Пользователь: {html.escape(user_tag)} (<code>{user_id}</code>)\n"
        f"Тариф: <b>{html.escape(plan['title'])}</b>\n"
        f"⚠️ <b>Требуемая сумма: {plan['uah']} грн</b> (НЕ подтверждайте, если пришла 1 грн!)\n\n"
        "Проверьте выписку в приложении Monobank:"
    )

    try:
        if bot:
            await bot.send_message(ADMIN_USER_ID, admin_msg, parse_mode="HTML", reply_markup=admin_kb)
        await callback.message.answer(
            "✅ <b>Запрос отправлен администратору!</b>\n"
            "После проверки бот мгновенно начислит вам тариф и пришлет сообщение.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить админа: {e}")
        await callback.message.answer("Заявка зафиксирована. Администратор проверит выписку.")

@dp.callback_query(F.data.startswith("adm_approve:"))
async def cb_admin_approve(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    parts = callback.data.split(":")
    target_user_id = int(parts[1])
    plan_key = parts[2]
    plan = PLANS.get(plan_key)

    if plan:
        activate_plan(target_user_id, plan_key, "monobank_manual", plan["uah"], "UAH")
        u = get_user_data(target_user_id)
        if bot:
            try:
                await bot.send_message(
                    target_user_id,
                    f"🎉 <b>Ваша оплата подтверждена!</b>\n\n"
                    f"Тариф: <b>{html.escape(plan['title'])}</b> успешно начислен.\n"
                    f"📊 Ваш статус: <b>{html.escape(u['status_text'])}</b>.\n\n"
                    "Приятных проверок вещей!",
                    parse_mode="HTML",
                    reply_markup=get_main_menu_keyboard()
                )
            except Exception:
                pass
        await callback.message.edit_text(f"✅ Успешно! Пользователю <code>{target_user_id}</code> выдан тариф {plan['title']} ({plan['uah']} грн).", parse_mode="HTML")

@dp.callback_query(F.data.startswith("adm_reject:"))
async def cb_admin_reject(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return
    target_user_id = int(callback.data.split(":")[1])
    if bot:
        try:
            await bot.send_message(target_user_id, "❌ Платеж на указанную сумму не был найден в выписке Банки.", parse_mode="HTML")
        except Exception:
            pass
    await callback.message.edit_text(f"❌ Заявка пользователя <code>{target_user_id}</code> отклонена.", parse_mode="HTML")

def get_admin_promo_keyboard():
    buttons = []
    for plan_key, plan in PLANS.items():
        buttons.append([InlineKeyboardButton(text=f"🎟 {plan['title']}", callback_data=f"gen_promo:{plan_key}")])
    buttons.append([InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def generate_random_promo_code() -> str:
    chars = string.ascii_uppercase + string.digits
    part1 = "".join(secrets.choice(chars) for _ in range(4))
    part2 = "".join(secrets.choice(chars) for _ in range(4))
    return f"CRIM-{part1}-{part2}"

async def apply_promo_code_logic(user_id: int, raw_code: str) -> tuple[bool, str]:
    clean_code = raw_code.strip().upper()
    rows = query_db("SELECT * FROM promo_codes WHERE code = ?", (clean_code,))

    if not rows:
        blogger_rows = query_db("SELECT * FROM bloggers WHERE promo_code = ?", (clean_code,))
        if not blogger_rows:
            return False, "❌ Промокод не существует или введен неверно."

        blogger = blogger_rows[0]
        blogger_id = blogger["id"]
        blogger_tag = blogger.get("tag", "Блогер")
        plan_key = blogger.get("plan_id")
        plan = PLANS.get(plan_key)
        if not plan:
            return False, "❌ Ошибка: связанный тариф не найден."

        already_used = query_db(
            "SELECT id FROM blogger_promo_uses WHERE blogger_id = ? AND user_id = ?",
            (blogger_id, user_id)
        )
        if already_used:
            return False, "⚠️ Вы уже активировали бонусный промокод от этого блогера ранее."

        now_iso = datetime.now().isoformat()
        query_db(
            "INSERT INTO blogger_promo_uses (blogger_id, user_id, used_at) VALUES (?, ?, ?)",
            (blogger_id, user_id, now_iso)
        )
        query_db(
            "UPDATE bloggers SET total_referrals = total_referrals + 1 WHERE id = ?",
            (blogger_id,)
        )
        query_db(
            "UPDATE users SET referred_by_blogger = ? WHERE user_id = ?",
            (blogger_tag, user_id)
        )

        activate_plan(user_id, plan_key, "blogger_promo", 0.0, "BLOGGER")
        u = get_user_data(user_id)

        if bot:
            asyncio.create_task(
                bot.send_message(
                    ADMIN_USER_ID,
                    f"📢 <b>Новый реферал от блогера!</b>\n\n"
                    f"👤 Блогер: <b>{html.escape(str(blogger_tag))}</b>\n"
                    f"🆔 Пользователь: <code>{user_id}</code>\n"
                    f"📦 Выдан бонус: <b>{html.escape(plan['title'])}</b>\n\n"
                    f"Теперь при любых покупках этого пользователя блогер будет получать 20%!",
                    parse_mode="HTML"
                )
            )

        success_text = (
            f"🎉 <b>Промокод от {html.escape(str(blogger_tag))} активирован!</b>\n\n"
            f"Вам начислен тариф: <b>{html.escape(plan['title'])}</b>.\n"
            f"📊 Ваш баланс: <b>{html.escape(u['status_text'])}</b>.\n\n"
            "Приятного пользования!"
        )
        return True, success_text

    promo = rows[0]
    if int(promo.get("is_used") or 0) == 1:
        return False, "⚠️ Этот промокод уже был активирован ранее."

    expires_at_str = promo.get("expires_at")
    if expires_at_str:
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            if datetime.now() > expires_at:
                return False, "⏳ Срок действия этого промокода (7 дней) истёк."
        except Exception:
            pass

    plan_key = promo.get("plan_id")
    plan = PLANS.get(plan_key)
    if not plan:
        return False, "❌ Ошибка: связанный тариф не найден."

    now_iso = datetime.now().isoformat()
    query_db(
        "UPDATE promo_codes SET is_used = 1, used_by = ?, used_at = ? WHERE code = ?",
        (user_id, now_iso, clean_code)
    )
    activate_plan(user_id, plan_key, "promo_code", 0.0, "PROMO")
    u = get_user_data(user_id)
    success_text = (
        "🎉 <b>Промокод успешно активирован!</b>\n\n"
        f"Вам начислен тариф: <b>{html.escape(plan['title'])}</b>.\n"
        f"📊 Ваш баланс: <b>{html.escape(u['status_text'])}</b>.\n\n"
        "Приятного пользования!"
    )
    return True, success_text

def generate_blogger_promo_code(blogger_prefix: str) -> str:
    clean_prefix = re.sub(r"[^A-Za-z0-9]", "", blogger_prefix).upper()[:5]
    if not clean_prefix:
        clean_prefix = "BLOG"
    chars = string.ascii_uppercase + string.digits
    random_part = "".join(secrets.choice(chars) for _ in range(4))
    return f"{clean_prefix}-{random_part}"

def format_blogger_stats_text(header: str = "📊 <b>Партнёрская статистика блогеров (20%):</b>\n") -> str:
    """Форматирует полную сводку блогеров и начислений для отправки в каналы или админу."""
    bloggers = query_db("SELECT * FROM bloggers ORDER BY id DESC")
    if not bloggers:
        return f"{header}\n<i>Блогеры пока не зарегистрированы в системе.</i>"

    text_lines = [header]
    total_refs = 0
    total_uah = 0.0
    total_stars = 0
    today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")

    for idx, b in enumerate(bloggers, 1):
        plan = PLANS.get(b.get("plan_id"), {})
        plan_title = plan.get("title", "Бонус")
        tag = html.escape(str(b.get("tag", "Блогер")))
        code = b.get("promo_code", "НЕТ")
        refs = int(b.get("total_referrals") or 0)
        uah = float(b.get("earnings_uah") or 0.0)
        stars = int(b.get("earnings_stars") or 0)
        paid_uah = float(b.get("total_paid_uah") or 0.0)
        paid_stars = int(b.get("total_paid_stars") or 0)

        base_rate = float(b.get("commission_rate") or 20.0)
        fine_pct = float(b.get("fine_percent") or 0.0)
        fine_until = b.get("fine_until")

        if fine_until and fine_until >= today_str and fine_pct > 0:
            effective_rate = max(0.0, base_rate - fine_pct)
            fine_info = f"\n• ⚠️ <b>Штраф: -{fine_pct:.0f}%</b> на 1 мес. (до {fine_until}, ставка: <b>{effective_rate:.0f}%</b>)"
        elif base_rate != 20.0:
            fine_info = f"\n• ⚙️ <b>Ставка: {base_rate:.0f}%</b>"
        else:
            fine_info = "\n• 💎 Ставка: <b>20%</b>"

        payout_info = ""
        if paid_uah > 0 or paid_stars > 0:
            payout_info = f"\n• ✅ Всего выплачено ранее: {paid_uah:.2f} грн | {paid_stars} ⭐"

        total_refs += refs
        total_uah += uah
        total_stars += stars

        text_lines.append(
            f"<b>{idx}. {tag}</b>\n"
            f"• Промокод: <code>{code}</code>\n"
            f"• Привлечено: <b>{refs} чел.</b>\n"
            f"• Бонус зрителям: {plan_title}"
            f"{fine_info}\n"
            f"• 💰 <b>К выплате (накоплено): {uah:.2f} грн</b> | <b>{stars} ⭐</b>"
            f"{payout_info}\n"
            "───────────────"
        )

    text_lines.append(
        f"\n📈 <b>Итого по всем блогерам:</b>\n"
        f"• Всего рефералов: <b>{total_refs} чел.</b>\n"
        f"• Суммарно к выплате: <b>{total_uah:.2f} грн</b> | <b>{total_stars} ⭐</b>\n\n"
        f"🕒 <i>Сформировано: {datetime.now(KYIV_TZ).strftime('%d.%m.%Y %H:%M')} (Киев)</i>"
    )
    return "\n".join(text_lines)

def get_admin_blogger_plans_keyboard():
    buttons = []
    for plan_key, plan in PLANS.items():
        buttons.append([InlineKeyboardButton(text=f"🎁 {plan['title']}", callback_data=f"blog_plan:{plan_key}")])
    buttons.append([InlineKeyboardButton(text="📊 Список блогеров и статистика", callback_data="blog_stats")])
    buttons.append([
        InlineKeyboardButton(text="⚖️ Назначить штраф (%)", callback_data="blog_fine_start"),
        InlineKeyboardButton(text="💸 Выплата (Обнуление)", callback_data="blog_payout_start")
    ])
    buttons.append([InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("promoblog"))
async def cmd_promoblog(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        await message.answer("⛔ Данная команда доступна только главному администратору.")
        return

    await state.clear()
    text = (
        "🤝 <b>Панель работы с блогерами и инфлюенсерами</b>\n\n"
        "Здесь вы можете создать партнерский промокод для блогера:\n"
        "• Промокод многоразовый — каждый зритель блогера сможет ввести его 1 раз\n"
        "• Зритель получает выбранный бонус (например, 7 дней безлимита)\n"
        "• Зритель <b>навсегда закрепляется</b> за этим блогером\n"
        "• Блогеру автоматически начисляется <b>20%</b> (с учётом штрафов/бонусов) со всех покупок\n\n"
        "Выберите действие ниже 👇"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_admin_blogger_plans_keyboard())

@dp.callback_query(F.data.startswith("blog_plan:"))
async def cb_select_blogger_plan(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        return

    await state.update_data(selected_blog_plan=plan_key)
    await state.set_state(BloggerPromoFSM.waiting_for_blogger_tag)

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_main")]
    ])

    await callback.message.edit_text(
        f"📝 <b>Регистрация блогера</b>\n\n"
        f"Выбранный бонус для зрителей: <b>{html.escape(plan['title'])}</b>\n\n"
        "Отправьте в ответном сообщении <b>никнейм, имя или канал блогера</b>:\n"
        "<i>Например: @resale_bro, Vlad Resale или TikTok_Artem</i>",
        parse_mode="HTML",
        reply_markup=cancel_kb
    )

@dp.message(StateFilter(BloggerPromoFSM.waiting_for_blogger_tag), F.text)
async def process_blogger_tag_input(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return

    blogger_tag = message.text.strip()
    data = await state.get_data()
    plan_key = data.get("selected_blog_plan", "sub_7d")
    plan = PLANS.get(plan_key)
    await state.clear()

    existing = query_db("SELECT * FROM bloggers WHERE tag = ?", (blogger_tag,))
    if existing:
        b = existing[0]
        await message.answer(
            f"⚠️ Блогер <b>{html.escape(blogger_tag)}</b> уже зарегистрирован ранее!\n\n"
            f"Его промокод: <code>{b['promo_code']}</code>\n"
            f"Используйте команду <code>/promoblog</code> для просмотра статистики.",
            parse_mode="HTML",
            reply_markup=get_admin_blogger_plans_keyboard()
        )
        return

    promo_code = generate_blogger_promo_code(blogger_tag)
    now_iso = datetime.now().isoformat()

    query_db(
        """INSERT INTO bloggers (tag, promo_code, plan_id, created_at, earnings_uah, earnings_stars, total_referrals)
           VALUES (?, ?, ?, ?, 0.0, 0, 0)""",
        (blogger_tag, promo_code, plan_key, now_iso)
    )

    reply_text = (
        "✅ <b>Блогер успешно зарегистрирован!</b>\n\n"
        f"👤 Блогер: <b>{html.escape(blogger_tag)}</b>\n"
        f"🎟 Промокод для видео: <code>{promo_code}</code> (нажмите, чтобы скопировать)\n"
        f"🎁 Подарок для аудитории: <b>{html.escape(plan['title'])}</b>\n"
        f"💸 Комиссия блогеру: <b>20%</b> со всех платежей его рефералов\n\n"
        "Передайте этот промокод блогеру. Когда его зрители будут покупать тарифы, "
        "бот будет автоматически присылать вам уведомления и подсчитывать баланс блогера."
    )
    await message.answer(reply_text, parse_mode="HTML", reply_markup=get_admin_blogger_plans_keyboard())

@dp.message(Command("dellblog"))
async def cmd_dellblog(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        await message.answer("⛔ Данная команда доступна только главному администратору.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "⚠️ <b>Укажите ник или промокод блогера для удаления!</b>\n\n"
            "Пример использования:\n"
            "<code>/dellblog @resale_bro</code> или <code>/dellblog RESALE-7A1B</code>",
            parse_mode="HTML"
        )
        return

    raw_tag = parts[1].strip()
    tag_with_at = raw_tag if raw_tag.startswith("@") else f"@{raw_tag}"
    tag_without_at = raw_tag.lstrip("@")

    found = query_db(
        "SELECT * FROM bloggers WHERE tag = ? OR tag = ? OR promo_code = ?",
        (tag_with_at, tag_without_at, raw_tag.upper())
    )

    if not found:
        await message.answer(
            f"❌ Блогер с ником или промокодом <b>{html.escape(raw_tag)}</b> не найден в базе данных.",
            parse_mode="HTML"
        )
        return

    blogger = found[0]
    b_id = blogger["id"]
    b_tag = blogger.get("tag", raw_tag)
    b_code = blogger.get("promo_code", "НЕТ")

    query_db("DELETE FROM bloggers WHERE id = ?", (b_id,))
    query_db("DELETE FROM blogger_promo_uses WHERE blogger_id = ?", (b_id,))

    await message.answer(
        f"🗑 <b>Блогер успешно удален!</b>\n\n"
        f"👤 Никнейм: <b>{html.escape(str(b_tag))}</b>\n"
        f"🎟 Промокод <code>{b_code}</code> отключен.\n\n"
        f"Проверить актуальный список: <code>/promoblog</code>",
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "blog_fine_start")
async def cb_blog_fine_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    await state.set_state(BloggerFineFSM.waiting_for_blogger_ident)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_blog_menu")]
    ])
    await callback.message.edit_text(
        "⚖️ <b>Управление штрафами и ставками блогеров</b>\n\n"
        "Отправьте в чат <b>@username, имя или промокод блогера</b>, которому хотите изменить процент или выдать штраф на 1 месяц:\n"
        "<i>Например: @resale_bro или RESALE-7A1B</i>",
        parse_mode="HTML",
        reply_markup=kb
    )

@dp.message(StateFilter(BloggerFineFSM.waiting_for_blogger_ident), F.text)
async def process_blogger_fine_ident(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return

    raw_val = message.text.strip()
    tag_with_at = raw_val if raw_val.startswith("@") else f"@{raw_val}"
    tag_without_at = raw_val.lstrip("@")

    found = query_db(
        "SELECT * FROM bloggers WHERE tag = ? OR tag = ? OR promo_code = ?",
        (tag_with_at, tag_without_at, raw_val.upper())
    )

    if not found:
        await message.answer(
            f"❌ Блогер с ником или промокодом <b>{html.escape(raw_val)}</b> не найден.\n"
            "Попробуйте ввести заново или проверьте список через <code>/promoblog</code>:",
            parse_mode="HTML"
        )
        return

    blogger = found[0]
    await state.update_data(fine_blogger_id=blogger["id"], fine_blogger_tag=blogger["tag"])
    await state.set_state(BloggerFineFSM.waiting_for_fine_value)

    today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
    cur_fine = float(blogger.get("fine_percent") or 0.0)
    fine_until = blogger.get("fine_until")
    fine_status = f"Активен штраф -{cur_fine:.0f}% до {fine_until}" if (fine_until and fine_until >= today_str and cur_fine > 0) else "Штрафов нет"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_blog_menu")]
    ])

    await message.answer(
        f"👤 Выбран блогер: <b>{html.escape(blogger['tag'])}</b>\n"
        f"Текущий статус: <i>{fine_status}</i> (базовая ставка: {blogger.get('commission_rate', 20.0)}%)\n\n"
        "Введите величину изменения ставки на <b>1 месяц</b>:\n"
        "• <code>-10</code> — штраф 10% на месяц (будет получать 10% вместо 20%)\n"
        "• <code>+5</code> — бонусная надбавка 5% (будет получать 25%)\n"
        "• <code>0</code> — полностью снять все штрафы и вернуть стандартные 20%",
        parse_mode="HTML",
        reply_markup=kb
    )

@dp.message(StateFilter(BloggerFineFSM.waiting_for_fine_value), F.text)
async def process_blogger_fine_value(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return

    data = await state.get_data()
    blogger_id = data.get("fine_blogger_id")
    blogger_tag = data.get("fine_blogger_tag")
    await state.clear()

    text_val = message.text.strip().replace("%", "")
    try:
        val = float(text_val)
    except ValueError:
        await message.answer("⚠️ Введите число (например <code>-10</code>, <code>+5</code> или <code>0</code>). Попробуйте заново через меню.")
        return

    now = datetime.now(KYIV_TZ)
    month_later_str = (now + timedelta(days=30)).strftime("%Y-%m-%d")

    if val < 0:
        fine_percent = abs(val)
        query_db(
            "UPDATE bloggers SET fine_percent = ?, fine_until = ? WHERE id = ?",
            (fine_percent, month_later_str, blogger_id)
        )
        msg = (
            f"⚖️ <b>Штраф успешно назначен!</b>\n\n"
            f"👤 Блогер: <b>{html.escape(str(blogger_tag))}</b>\n"
            f"📉 Штраф: <b>-{fine_percent:.0f}%</b> от комиссии\n"
            f"⏳ Срок действия: <b>1 месяц</b> (до {month_later_str})\n"
            f"📊 Итоговая ставка на период штрафа: <b>{max(0.0, 20.0 - fine_percent):.0f}%</b>"
        )
    elif val == 0:
        query_db(
            "UPDATE bloggers SET fine_percent = 0.0, fine_until = NULL, commission_rate = 20.0 WHERE id = ?",
            (blogger_id,)
        )
        msg = (
            f"✅ <b>Штрафы сняты!</b>\n\n"
            f"👤 Блогер: <b>{html.escape(str(blogger_tag))}</b>\n"
            "Ставка комиссии возвращена на стандартные <b>20%</b>."
        )
    else:
        new_rate = 20.0 + val
        query_db(
            "UPDATE bloggers SET commission_rate = ?, fine_percent = 0.0, fine_until = NULL WHERE id = ?",
            (new_rate, blogger_id)
        )
        msg = (
            f"🚀 <b>Ставка повышена!</b>\n\n"
            f"👤 Блогер: <b>{html.escape(str(blogger_tag))}</b>\n"
            f"📈 Новая ставка: <b>{new_rate:.0f}%</b>"
        )

    await message.answer(msg, parse_mode="HTML", reply_markup=get_admin_blogger_plans_keyboard())

@dp.callback_query(F.data == "blog_payout_start")
async def cb_blog_payout_start(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    bloggers = query_db("SELECT * FROM bloggers ORDER BY id DESC")
    if not bloggers:
        await callback.message.edit_text("ℹ️ В системе пока нет зарегистрированных блогеров.", reply_markup=get_admin_blogger_plans_keyboard())
        return

    buttons = []
    for b in bloggers:
        uah = float(b.get("earnings_uah") or 0.0)
        stars = int(b.get("earnings_stars") or 0)
        btn_text = f"💸 {b['tag']} — {uah:.2f} грн | {stars} ⭐"
        buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"payout_ask:{b['id']}")])

    buttons.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_blog_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await callback.message.edit_text(
        "💸 <b>Фиксация выплаты и обнуление баланса</b>\n\n"
        "Выберите блогера, которому вы перевели деньги, чтобы подтвердить выплату и обнулить накопленный счетчик 👇",
        parse_mode="HTML",
        reply_markup=kb
    )

@dp.callback_query(F.data.startswith("payout_ask:"))
async def cb_payout_ask(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    b_id = int(callback.data.split(":")[1])
    found = query_db("SELECT * FROM bloggers WHERE id = ?", (b_id,))
    if not found:
        await callback.message.answer("❌ Блогер не найден.")
        return

    blogger = found[0]
    uah = float(blogger.get("earnings_uah") or 0.0)
    stars = int(blogger.get("earnings_stars") or 0)

    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Подтвердить выплату ({uah:.2f} грн / {stars} ⭐)", callback_data=f"payout_confirm:{b_id}")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="blog_payout_start")]
    ])

    await callback.message.edit_text(
        f"⚠️ <b>Подтверждение выплаты блогеру:</b>\n\n"
        f"👤 Блогер: <b>{html.escape(blogger['tag'])}</b>\n"
        f"💰 Накопленная сумма: <b>{uah:.2f} грн</b> | <b>{stars} ⭐</b>\n\n"
        "После нажатия кнопки баланс блогера <b>обнулится до 0</b>, а выплаченная сумма запишется в историю общих выплат блогеру.",
        parse_mode="HTML",
        reply_markup=confirm_kb
    )

@dp.callback_query(F.data.startswith("payout_confirm:"))
async def cb_payout_confirm(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    b_id = int(callback.data.split(":")[1])
    found = query_db("SELECT * FROM bloggers WHERE id = ?", (b_id,))
    if not found:
        await callback.message.answer("❌ Блогер не найден.")
        return

    blogger = found[0]
    cur_uah = float(blogger.get("earnings_uah") or 0.0)
    cur_stars = int(blogger.get("earnings_stars") or 0)
    total_paid_uah = float(blogger.get("total_paid_uah") or 0.0) + cur_uah
    total_paid_stars = int(blogger.get("total_paid_stars") or 0) + cur_stars

    query_db(
        """UPDATE bloggers 
           SET earnings_uah = 0.0, 
               earnings_stars = 0, 
               total_paid_uah = ?, 
               total_paid_stars = ? 
           WHERE id = ?""",
        (total_paid_uah, total_paid_stars, b_id)
    )

    success_msg = (
        f"🎉 <b>Выплата зафиксирована, баланс обнулен!</b>\n\n"
        f"👤 Блогер: <b>{html.escape(blogger['tag'])}</b>\n"
        f"💵 Выплачено в этот раз: <b>{cur_uah:.2f} грн</b> | <b>{cur_stars} ⭐</b>\n"
        f"📈 Всего выплачено за всё время: <b>{total_paid_uah:.2f} грн</b> | <b>{total_paid_stars} ⭐</b>\n\n"
        f"Текущий баланс блогера: <b>0.00 грн</b> | <b>0 ⭐</b>"
    )

    await callback.message.edit_text(success_msg, parse_mode="HTML", reply_markup=get_admin_blogger_plans_keyboard())

@dp.callback_query(F.data == "blog_stats")
async def cb_show_blogger_stats(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    text = format_blogger_stats_text()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить список", callback_data="blog_stats")],
        [InlineKeyboardButton(text="➕ Зарегистрировать еще блогера", callback_data="back_to_blog_menu")],
        [InlineKeyboardButton(text="◀️ В главное меню", callback_data="back_to_main")]
    ])

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "back_to_blog_menu")
async def cb_back_to_blog_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await cmd_promoblog(callback.message, state)

@dp.message(Command("point"))
async def cmd_point(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        await message.answer("⛔ Данная команда доступна только главному администратору.")
        return

    parts = message.text.split(maxsplit=1)
    target_chat = None
    target_title = None

    if len(parts) > 1 and parts[1].strip():
        target_chat = parts[1].strip()
    elif message.chat.type in ("group", "supergroup", "channel"):
        target_chat = str(message.chat.id)
        target_title = message.chat.title or "Группа"
    else:
        await message.answer(
            "⚠️ <b>Укажите канал или группу для отчётов!</b>\n\n"
            "Примеры использования:\n"
            "• <code>/point @my_channel</code> — привязать публичный канал или группу\n"
            "• <code>/point -1001234567890</code> — привязать по ID\n"
            "• Или напишите <code>/point</code> прямо внутри группы, куда добавлен бот.",
            parse_mode="HTML"
        )
        return

    if not bot:
        await message.answer("⚠️ Ошибка: Экземпляр бота не инициализирован.")
        return

    report_text = format_blogger_stats_text("📊 <b>Активация точки отчётов /point</b>\n\nСтатистика блогеров на данный момент:\n")
    try:
        sent_msg = await bot.send_message(target_chat, report_text, parse_mode="HTML")
        final_chat_id = str(sent_msg.chat.id)
        if not target_title:
            target_title = sent_msg.chat.title or sent_msg.chat.username or str(target_chat)

        now_iso = datetime.now().isoformat()
        query_db("DELETE FROM report_points WHERE chat_id = ?", (final_chat_id,))
        query_db(
            "INSERT INTO report_points (chat_id, title, created_at) VALUES (?, ?, ?)",
            (final_chat_id, target_title, now_iso)
        )

        await message.answer(
            f"✅ <b>Точка отчётов успешно активирована!</b>\n\n"
            f"📍 Канал / Чат: <b>{html.escape(target_title)}</b> (<code>{final_chat_id}</code>)\n"
            f"📤 Тестовый отчёт со статистикой блогеров уже отправлен туда.\n"
            f"⏰ <b>Авто-рассылка:</b> каждый вечер ровно в <b>22:00</b> по киевскому времени бот будет присылать туда свежую статистику.\n\n"
            f"Для отключения используйте: <code>/dellpoint</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка активации точки {target_chat}: {e}")
        await message.answer(
            f"❌ <b>Не удалось отправить отчёт в {html.escape(str(target_chat))}!</b>\n\n"
            f"Причина: <code>{html.escape(str(e))}</code>\n\n"
            "Убедитесь, что:\n"
            "1. Бот добавлен в эту группу/канал как администратор с правами публикации.\n"
            "2. Указан верный @юзернейм или ID чата.",
            parse_mode="HTML"
        )

@dp.message(Command("dellpoint"))
async def cmd_dellpoint(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        await message.answer("⛔ Данная команда доступна только главному администратору.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].strip():
        arg = parts[1].strip()
        query_db("DELETE FROM report_points WHERE chat_id = ? OR title = ?", (arg, arg))
        await message.answer(f"🗑 Точка отчётов <b>{html.escape(arg)}</b> отключена.", parse_mode="HTML")
        return

    if message.chat.type in ("group", "supergroup", "channel"):
        cid = str(message.chat.id)
        query_db("DELETE FROM report_points WHERE chat_id = ?", (cid,))
        await message.answer("🗑 Эта группа отключена от вечерней рассылки отчётов.", parse_mode="HTML")
        return

    points = query_db("SELECT * FROM report_points")
    if not points:
        await message.answer("ℹ️ Активных точек отчётов не найдено.")
        return

    query_db("DELETE FROM report_points")
    await message.answer("🗑 Все точки отчётов (вечерняя рассылка в 22:00) успешно отключены.", parse_mode="HTML")

async def daily_point_scheduler():
    """Надежный интервальный планировщик: проверяет время каждые 30 секунд и гарантированно шлет отчет в 22:00."""
    logger.info("Запущен планировщик вечерних отчётов (интервал 30 сек, окно 22:00 Киев).")
    last_reported_date = ""
    while True:
        try:
            now = datetime.now(KYIV_TZ)
            today_str = now.strftime("%Y-%m-%d")

            if now.hour == 22 and 0 <= now.minute <= 15 and last_reported_date != today_str:
                points = query_db("SELECT * FROM report_points")
                if points and bot:
                    logger.info(f"Начало отправки вечерних отчётов за {today_str}...")
                    report_text = format_blogger_stats_text("📊 <b>Ежедневный отчёт по блогерам (22:00 Киев):</b>\n")
                    for p in points:
                        cid = p.get("chat_id")
                        if not cid:
                            continue
                        try:
                            await bot.send_message(cid, report_text, parse_mode="HTML")
                            logger.info(f"Вечерний отчёт успешно доставлен в {cid} ({p.get('title')})")
                        except Exception as post_err:
                            logger.warning(f"Не удалось отправить вечерний отчёт в {cid}: {post_err}")
                last_reported_date = today_str

            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception as loop_err:
            logger.error(f"Ошибка в daily_point_scheduler: {loop_err}")
            await asyncio.sleep(30)

def get_info_stats_text() -> str:
    """Формирует отчет по пользователям за все время и за текущий день."""
    today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
    today_formatted = datetime.now(KYIV_TZ).strftime("%d.%m.%Y")

    total_users_rows = query_db("SELECT COUNT(*) as cnt FROM users")
    total_users = total_users_rows[0].get("cnt", 0) if total_users_rows else 0

    log_users_rows = query_db(
        "SELECT COUNT(DISTINCT user_id) as cnt FROM usage_logs WHERE action_date = ?",
        (today_str,)
    )
    logged_today = log_users_rows[0].get("cnt", 0) if log_users_rows else 0

    user_today_rows = query_db(
        "SELECT COUNT(DISTINCT user_id) as cnt FROM users WHERE last_check_date = ? AND checks_today > 0",
        (today_str,)
    )
    users_today = user_today_rows[0].get("cnt", 0) if user_today_rows else 0

    active_today = max(logged_today, users_today)

    total_checks_rows = query_db(
        "SELECT COUNT(*) as cnt FROM usage_logs WHERE action_date = ?",
        (today_str,)
    )
    checks_count_today = total_checks_rows[0].get("cnt", 0) if total_checks_rows else 0

    pay_rows = query_db(
        "SELECT currency, SUM(amount) as s FROM payments WHERE status = 'success' GROUP BY currency"
    )
    total_uah = 0.0
    total_stars = 0
    for r in pay_rows:
        curr = str(r.get("currency") or "").upper()
        amt = float(r.get("s") or 0.0)
        if curr == "UAH":
            total_uah = amt
        elif curr == "XTR":
            total_stars = int(amt)

    checks_line = f"• Проверок вещей сделано сегодня: <b>{checks_count_today} шт.</b>\n" if checks_count_today > 0 else ""

    return (
        "📊 <b>Статистика бота (/info)</b>\n\n"
        f"📅 Дата: <b>{today_formatted}</b> (Киев)\n\n"
        "👥 <b>Пользователи:</b>\n"
        f"• Всего зарегистрировано: <b>{total_users} чел.</b>\n"
        f"• Воспользовались ботом сегодня: <b>{active_today} чел.</b>\n"
        f"{checks_line}\n"
        "💰 <b>Касса за всё время:</b>\n"
        f"• 💳 Монобанк: <b>{total_uah:.2f} грн</b>\n"
        f"• ⭐ Telegram Stars: <b>{total_stars} ⭐</b>\n\n"
        f"🕒 <i>Обновлено: {datetime.now(KYIV_TZ).strftime('%H:%M:%S')}</i>"
    )

def get_info_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_info")],
        [InlineKeyboardButton(text="◀️ В главное меню", callback_data="back_to_main")]
    ])

@dp.message(Command("info"))
async def cmd_info(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        await message.answer("⛔ Данная команда доступна только главному администратору.")
        return

    text = get_info_stats_text()
    await message.answer(text, parse_mode="HTML", reply_markup=get_info_keyboard())

@dp.callback_query(F.data == "refresh_info")
async def cb_refresh_info(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("⛔ Доступно только администратору.", show_alert=True)
        return

    await callback.answer("Обновляю данные...")
    text = get_info_stats_text()
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_info_keyboard())
    except Exception:
        pass

@dp.message(Command("promo"))
async def cmd_promo(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            ok, response_text = await apply_promo_code_logic(message.from_user.id, parts[1])
            await message.answer(response_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())
            return
        await message.answer(
            "⛔ Команда создания промокодов доступна только администратору.\n\n"
            "Чтобы активировать промокод, нажмите кнопку <b>«🎟 Ввести промокод»</b> в главном меню или отправьте <code>/code ВАШ_КОД</code>.",
            parse_mode="HTML"
        )
        return

    text = (
        "👑 <b>Генератор промокодов (Панель Администратора)</b>\n\n"
        "Выберите тариф из списка ниже, на который вы хотите выпустить промокод.\n"
        "Каждый промокод создается со сроком действия <b>7 дней</b> и сохраняется в облачную базу данных Turso."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_admin_promo_keyboard())

@dp.callback_query(F.data.startswith("gen_promo:"))
async def cb_generate_promo(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        return

    code = generate_random_promo_code()
    now = datetime.now()
    expires_at = now + timedelta(days=7)
    expires_str = expires_at.strftime("%d.%m.%Y %H:%M")

    query_db(
        "INSERT INTO promo_codes (code, plan_id, created_at, expires_at, is_used) VALUES (?, ?, ?, ?, 0)",
        (code, plan_key, now.isoformat(), expires_at.isoformat())
    )

    reply_text = (
        "🎟 <b>Промокод успешно создан!</b>\n\n"
        f"📦 Тариф: <b>{html.escape(plan['title'])}</b>\n"
        f"🔑 Код: <code>{code}</code> (нажмите, чтобы скопировать)\n"
        f"⏳ Срок действия: <b>7 дней</b> (до {expires_str})\n\n"
        "Передайте этот код пользователю. Он сможет применить его через кнопку «🎟 Ввести промокод» или команду <code>/code</code>."
    )
    await callback.message.edit_text(reply_text, parse_mode="HTML", reply_markup=get_admin_promo_keyboard())

@dp.callback_query(F.data == "enter_promo")
async def cb_enter_promo(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(PromoInputFSM.waiting_for_promo_code)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_main")]
    ])
    await callback.message.answer(
        "🎟 <b>Активация промокода</b>\n\n"
        "Отправьте ваш промокод ответным сообщением в чат:",
        parse_mode="HTML",
        reply_markup=kb
    )

@dp.message(Command("code"))
async def cmd_code(message: Message, state: FSMContext):
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        await state.clear()
        ok, res_text = await apply_promo_code_logic(message.from_user.id, parts[1])
        await message.answer(res_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())
    else:
        await state.set_state(PromoInputFSM.waiting_for_promo_code)
        await message.answer("🎟 Отправьте ваш промокод ответным сообщением:", parse_mode="HTML")

@dp.message(StateFilter(PromoInputFSM.waiting_for_promo_code), F.text)
async def process_promo_input(message: Message, state: FSMContext):
    await state.clear()
    ok, res_text = await apply_promo_code_logic(message.from_user.id, message.text)
    await message.answer(res_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())

@dp.callback_query(F.data == "start_check")
async def cb_start_check(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user_id = callback.from_user.id

    can_proceed, reason = await check_can_proceed(user_id)
    if not can_proceed:
        if reason == "need_sub":
            text = (
                "📢 <b>Подпишитесь на канал для бесплатных проверок!</b>\n\n"
                "Вы исчерпали стартовые проверки. Чтобы получать <b>3 бесплатные проверки каждый день</b>, подпишитесь на наш канал:\n"
                f"👉 <a href=\"{CHANNEL_URL}\"><b>{CHANNEL_USERNAME}</b></a>\n\n"
                "После подписки нажмите <b>«✅ Я подписался»</b> 👇"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_URL)],
                [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_channel_sub")],
                [InlineKeyboardButton(text="💎 Снять лимит (Тарифы)", callback_data="show_plans")],
                [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")]
            ])
            await callback.message.answer(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
            return

        text = (
            "⚠️ <b>Лимит проверок исчерпан!</b>\n\n"
            f"Вы использовали все {FREE_CHECKS_PER_DAY} бесплатные проверки на сегодня.\n"
            "Они автоматически обновятся завтра, либо вы можете снять лимит прямо сейчас 👇"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Снять лимит (Тарифы)", callback_data="show_plans")],
            [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")]
        ])
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
        return

    await state.set_state(ClothingCheckFSM.waiting_for_main_photo)
    await callback.message.answer(
        "📸 <b>Шаг 1 из 3: Общий вид</b>\n\n"
        "Пришлите фотографию <b>вещи или пары обуви целиком</b>.",
        parse_mode="HTML"
    )

@dp.message(StateFilter(ClothingCheckFSM.waiting_for_main_photo), F.photo)
async def process_main_photo(message: Message, state: FSMContext):
    # Фоновая загрузка и сжатие первого фото прямо во время шага 1
    if bot:
        p1_bytes = await fetch_and_prep_bytes(bot, message.photo[-1].file_id)
        await state.update_data(photo_1_bytes=p1_bytes)

    await state.set_state(ClothingCheckFSM.waiting_for_neck_tag)
    await message.answer(
        "🏷 <b>Шаг 2 из 3: Главная бирка / Логотип</b>\n\n"
        "Сфотографируйте <b>бирку на воротнике/горловине</b> (для одежды) либо <b>язычок / внешний логотип</b> (для обуви).",
        parse_mode="HTML"
    )

@dp.message(StateFilter(ClothingCheckFSM.waiting_for_neck_tag), F.photo)
async def process_neck_tag_photo(message: Message, state: FSMContext):
    # Фоновая загрузка и сжатие второго фото прямо во время шага 2
    if bot:
        p2_bytes = await fetch_and_prep_bytes(bot, message.photo[-1].file_id)
        await state.update_data(photo_2_bytes=p2_bytes)

    await state.set_state(ClothingCheckFSM.waiting_for_care_tag)
    await message.answer(
        "🧵 <b>Шаг 3 из 3: Сервисный ярлык / Размерная бирка</b>\n\n"
        "Отправьте <b>внутреннюю бирку с составом и артикулом</b> (wash tag) либо <b>размерную бирку кроссовок со style-code</b>.",
        parse_mode="HTML"
    )

@dp.message(StateFilter(ClothingCheckFSM.waiting_for_care_tag), F.photo)
async def process_care_tag_photo(message: Message, state: FSMContext):
    user_data = await state.get_data()
    await state.clear()
    status_msg = await message.answer(
        "🔍 <b>Анализирую вещь через AI...</b>\n"
        "Считываю артикулы, проверяю оригинальность и сверяю цены на рынке.",
        parse_mode="HTML"
    )

    try:
        if not bot:
            raise RuntimeError("Telegram Bot instance not ready")

        # Первые два фото уже пережаты и лежат в памяти; готовим только третье
        p3_bytes = await fetch_and_prep_bytes(bot, message.photo[-1].file_id)
        p1_bytes = user_data.get("photo_1_bytes")
        p2_bytes = user_data.get("photo_2_bytes")

        if not p1_bytes or not p2_bytes:
            raise ValueError("Не удалось получить предыдущие фото. Пожалуйста, начните проверку заново.")

        image_parts = [
            genai_types.Part.from_bytes(data=p1_bytes, mime_type="image/jpeg"),
            genai_types.Part.from_bytes(data=p2_bytes, mime_type="image/jpeg"),
            genai_types.Part.from_bytes(data=p3_bytes, mime_type="image/jpeg")
        ]

        data = await analyze_with_gemini_fallback(image_parts)
        await decrement_check(message.from_user.id)
        is_sub = await check_channel_subscription(message.from_user.id)
        u = get_user_data(message.from_user.id, is_subscribed=is_sub)

        brand = html.escape(str(data.get("brand") or "Не определен"))
        item_name = html.escape(str(data.get("item_name") or "Вещь / Обувь"))
        tier = html.escape(str(data.get("category_tier") or "Масс-маркет"))
        era = html.escape(str(data.get("era_or_year") or "Не указан"))
        verdict = html.escape(str(data.get("authenticity_verdict") or "Проверено"))

        local_q = data.get("search_query_local") or f"{brand} {item_name}"
        global_q = data.get("search_query_global") or f"{brand} {item_name}"
        links = generate_marketplace_links(str(local_q), str(global_q))

        raw_reasons = data.get("legit_reasons", [])
        if isinstance(raw_reasons, list):
            reasons_list = [str(r).strip() for r in raw_reasons if r]
        elif isinstance(raw_reasons, str) and raw_reasons.strip():
            reasons_list = [raw_reasons.strip()]
        else:
            reasons_list = []

        reasons_formatted = "\n".join([f"  • {html.escape(r)}" for r in reasons_list]) if reasons_list else "  • Детали и фурнитура соответствуют стандартам бренда"

        # Безопасное приведение числовых значений (защита от падения TypeError)
        score = safe_int(data.get("authenticity_score"), 75)
        score_emoji = "🟢" if score >= 75 else ("🟡" if score >= 45 else "🔴")

        price_uah_min = safe_int(data.get("price_uah_min"), 300)
        price_uah_max = safe_int(data.get("price_uah_max"), 600)
        price_usd_min = safe_int(data.get("price_usd_min"), 10)
        price_usd_max = safe_int(data.get("price_usd_max"), 20)

        # Сохраняем сырые данные и сформированную карточку с юзернеймом автора
        USER_CHECK_DATA[message.from_user.id] = data
        USER_SALES_CARDS[message.from_user.id] = build_sales_card_text(data, message.from_user.username)

        result_message = (
            f"🏷 <b>Бренд:</b> {brand}\n"
            f"👕 <b>Модель:</b> {item_name}\n"
            f"📦 <b>Сегмент:</b> {tier}\n"
            f"📅 <b>Период:</b> {era}\n\n"
            f"{score_emoji} <b>Легит-чек:</b> {verdict} ({score}%)\n"
            f"<b>Обоснование:</b>\n{reasons_formatted}\n\n"
            f"💰 <b>Реальная вторичка (Шафа / OLX):</b>\n"
            f"👉 <b>{price_uah_min} – {price_uah_max} грн</b> (~${price_usd_min} – ${price_usd_max})\n\n"
            f"📊 Остаток проверок: <b>{html.escape(u['status_text'])}</b>"
        )

        keyboard_buttons = [
            [
                InlineKeyboardButton(text="📋 Карточка для продажи (Текст)", callback_data=f"show_card:{message.from_user.id}")
            ],
            [
                InlineKeyboardButton(text="🇺🇦 Шафа (Shafa.ua)", url=links["shafa_ua"]),
                InlineKeyboardButton(text="🇺🇦 OLX Поиск", url=links["olx_ua"])
            ],
            [
                InlineKeyboardButton(text="💵 eBay (Проданные)", url=links["ebay_sold"]),
                InlineKeyboardButton(text="🛍 eBay (В продаже)", url=links["ebay_active"])
            ]
        ]
        if "масс-маркет" not in tier.lower():
            keyboard_buttons.append([InlineKeyboardButton(text="🔥 Grailed Маркет", url=links["grailed"])])

        keyboard_buttons.append([InlineKeyboardButton(text="🔄 Проверить еще вещь", callback_data="start_check")])
        keyboard_buttons.append([InlineKeyboardButton(text="💎 Продлить / Купить тариф", callback_data="show_plans")])

        kb = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        await status_msg.delete()
        await message.answer(result_message, parse_mode="HTML", reply_markup=kb)

    except (json.JSONDecodeError, ValueError) as json_err:
        logger.warning(f"Ошибка парсинга ответа: {json_err}")
        try:
            await status_msg.edit_text(
                "🔍 <b>Не удалось четко распознать бирку или текст.</b>\n\n"
                "Сделайте фото ярлыка ближе, с хорошим освещением и в фокусе, чтобы цифры и штрихкод были разборчивы.",
                parse_mode="HTML"
            )
        except Exception:
            pass
    except Exception as exc:
        err_msg = str(exc)
        logger.error(f"Ошибка при обработке запроса: {exc}", exc_info=True)
        if "503" in err_msg or "UNAVAILABLE" in err_msg or "high demand" in err_msg:
            err_text = "⏳ Серверы Google AI сейчас испытывают пиковую мировую нагрузку. Пожалуйста, подождите 15-20 секунд и нажмите «🔍 Проверить вещь» снова."
        elif "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
            err_text = "⏳ Превышен лимит запросов к AI в минуту. Пожалуйста, подождите 30 секунд и нажмите «🔍 Проверить вещь» снова."
        elif "404" in err_msg or "NOT_FOUND" in err_msg:
            err_text = f"⚠️ Модель AI временно недоступна для ключа: {html.escape(err_msg[:80])}."
        else:
            err_text = f"❌ Ошибка обработки: {html.escape(err_msg[:100])}"

        try:
            await status_msg.edit_text(err_text, parse_mode="HTML")
        except Exception:
            pass

@dp.callback_query(F.data.startswith("show_card:"))
async def cb_show_sales_card(callback: CallbackQuery):
    await callback.answer()
    target_user_id = int(callback.data.split(":")[1])
    
    # Берем данные проверки
    check_data = USER_CHECK_DATA.get(target_user_id) or USER_CHECK_DATA.get(callback.from_user.id)
    
    # Юзернейм берем у того, кто запросил карточку прямо сейчас (или из профиля)
    req_username = callback.from_user.username
    
    if check_data:
        card_text = build_sales_card_text(check_data, req_username)
    else:
        card_text = USER_SALES_CARDS.get(target_user_id) or USER_SALES_CARDS.get(callback.from_user.id)

    if not card_text:
        await callback.message.answer(
            "⚠️ Карточка не найдена или устарела. Запустите проверку вещи заново через кнопку «🔍 Проверить вещь».",
            parse_mode="HTML"
        )
        return

    reply_text = (
        "📋 <b>Готова картка для продажу</b>\n"
        "<i>(Натисніть на текст нижче в сірому полі, щоб скопіювати його в 1 клік):</i>\n\n"
        f"<code>{html.escape(card_text)}</code>"
    )

    close_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Сховати картку", callback_data="close_sales_card")]
    ])

    await callback.message.answer(reply_text, parse_mode="HTML", reply_markup=close_kb)

@dp.callback_query(F.data == "close_sales_card")
async def cb_close_sales_card(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass

async def handle_health_check(request):
    return web.Response(text="Bot is running 24/7 with Turso Cloud DB!", status=200)

async def start_background_web():
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Веб-сервер активен на порту {port}")

async def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.error("КРИТИЧЕСКАЯ ОШИБКА: Не задан TELEGRAM_BOT_TOKEN!")
        return

    init_db()
    await start_background_web()
    asyncio.create_task(daily_point_scheduler())
    logger.info("Database initialized. Starting Telegram bot polling...")
    if bot:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
