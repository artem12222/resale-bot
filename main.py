import asyncio
from datetime import date, datetime, timedelta
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
from PIL import Image

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

# Облачные учетные данные базы Turso (libSQL)
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
            is_lifetime INTEGER DEFAULT 0
        )
    """)

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

    query_db("""
        CREATE TABLE IF NOT EXISTS blogger_promo_uses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            blogger_id INTEGER,
            user_id INTEGER,
            used_at TEXT,
            UNIQUE(blogger_id, user_id)
        )
    """)

    # Добавляем колонку реферера в таблицу пользователей, если ее еще нет
    try:
        query_db("ALTER TABLE users ADD COLUMN referred_by_blogger TEXT DEFAULT NULL")
    except Exception:
        pass

    logger.info("База данных инициализирована (Turso Cloud / SQLite)")

def get_user_data(user_id: int, username: Optional[str] = None) -> dict:
    today_str = str(date.today())
    rows = query_db("SELECT * FROM users WHERE user_id = ?", (user_id,))

    if not rows:
        query_db(
            """INSERT INTO users (user_id, username, checks_today, last_check_date, is_premium, extra_checks, premium_until, is_lifetime) 
               VALUES (?, ?, 0, ?, 0, 0, NULL, 0)""",
            (user_id, username, today_str)
        )
        return {
            "user_id": user_id,
            "checks_today": 0,
            "extra_checks": 0,
            "premium_until": None,
            "is_lifetime": 0,
            "status_text": f"{FREE_CHECKS_PER_DAY} бесплатных на сегодня"
        }

    row = rows[0]
    checks_today = int(row.get("checks_today") or 0)
    last_date = row.get("last_check_date")
    extra_checks = int(row.get("extra_checks") or 0)
    premium_until = row.get("premium_until")
    is_lifetime = int(row.get("is_lifetime") or 0)

    if last_date != today_str:
        checks_today = 0
        query_db("UPDATE users SET checks_today = 0, last_check_date = ? WHERE user_id = ?", (today_str, user_id))

    if is_lifetime:
        status_text = "♾ VIP Навсегда (Безлимит)"
    elif premium_until and premium_until >= today_str:
        status_text = f"👑 Безлимит активен до {premium_until}"
    elif extra_checks > 0:
        free_rem = max(0, FREE_CHECKS_PER_DAY - checks_today)
        status_text = f"{free_rem} беспл. + {extra_checks} из пакета"
    else:
        free_rem = max(0, FREE_CHECKS_PER_DAY - checks_today)
        status_text = f"{free_rem} из {FREE_CHECKS_PER_DAY} бесплатных"

    return {
        "user_id": user_id,
        "checks_today": checks_today,
        "extra_checks": extra_checks,
        "premium_until": premium_until,
        "is_lifetime": is_lifetime,
        "status_text": status_text
    }

def check_can_proceed(user_id: int) -> bool:
    u = get_user_data(user_id)
    today_str = str(date.today())
    if u["is_lifetime"]:
        return True
    if u["premium_until"] and u["premium_until"] >= today_str:
        return True
    if u["checks_today"] < FREE_CHECKS_PER_DAY:
        return True
    if u["extra_checks"] > 0:
        return True
    return False

def decrement_check(user_id: int):
    u = get_user_data(user_id)
    today_str = str(date.today())
    if u["is_lifetime"]:
        return
    if u["premium_until"] and u["premium_until"] >= today_str:
        return

    if u["checks_today"] < FREE_CHECKS_PER_DAY:
        query_db(
            "UPDATE users SET checks_today = checks_today + 1, last_check_date = ? WHERE user_id = ?",
            (today_str, user_id)
        )
    elif u["extra_checks"] > 0:
        query_db(
            "UPDATE users SET extra_checks = extra_checks - 1 WHERE user_id = ?",
            (user_id,)
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
                if currency == "UAH":
                    commission = round(amount * 0.20, 2)
                    new_earnings = round((b.get("earnings_uah") or 0.0) + commission, 2)
                    query_db("UPDATE bloggers SET earnings_uah = ? WHERE id = ?", (new_earnings, b["id"]))
                    comm_text = f"<b>+{commission} грн</b> (20% от {amount} грн)"
                elif currency == "XTR":
                    commission_stars = int(amount * 0.20)
                    new_stars = int((b.get("earnings_stars") or 0) + commission_stars)
                    query_db("UPDATE bloggers SET earnings_stars = ? WHERE id = ?", (new_stars, b["id"]))
                    comm_text = f"<b>+{commission_stars} ⭐</b> (20% от {amount} ⭐)"
                else:
                    comm_text = f"<b>20%</b> от {amount} {currency}"

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
                if "flash" in name and "image" not in name and "live" not in name and "tts" not in name:
                    discovered.append(name)
            logger.info(f"Обнаружены доступные модели Google AI: {discovered}")
        except Exception as e:
            logger.warning(f"Не удалось получить список моделей через API: {e}")

    preferred = ["gemini-2.5-flash", "gemini-3.7-flash", "gemini-3.5-flash", "gemini-3.5-flash-lite", "gemini-2.5-flash-lite"]
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

ANALYSIS_PROMPT = """
Ты — профессиональный эксперт по ресейлу, оценке и легит-чеку ЛЮБОЙ одежды, обуви (кроссовок) и аксессуаров.
Тебе отправлены 3 фотографии одной вещи:
1) Общий план вещи (одежда, кроссовки, куртка, сумка).
2) Главная бирка / логотип / бирка на воротнике (для одежды) ИЛИ язычок / внешний брендинг (для обуви).
3) Внутренний сервисный ярлык (состав, фабричный артикул, wash tag) ИЛИ размерный ярлык кроссовок со style-code и штрихкодом.

ТВОЯ ЗАДАЧА:
1. ОПРЕДЕЛИТЬ БРЕНД И МОДЕЛЬ:
   - Внимательно прочитай текст, артикулы, цифры, штрихкоды и логотипы на всех бирках (даже если фото перевернуто или под углом).
   - Определи категорию: Масс-маркет / Стритвир и Ворквир / Спортивный бренд / Премиум и Люкс / Винтаж.

2. ПРОВЕСТИ ЛЕГИТ-ЧЕК:
   - Оцени оригинальность: шрифты, ровность строчек, наличие фабричных кодов (RN, CA, style-code, Certilogo, QR-коды, штрихкоды).
   - Для масс-маркета (Zara, Pull&Bear, Bershka, H&M, Uniqlo, Mango и др.): если бирки фабричные — оригинальность 99-100% (масс-маркет не подделывают).
   - Для кроссовок (Nike, adidas, New Balance, Jordan, ASICS): проверь соответствие style-code и формат размерной сетки.
   - Сформулируй четкие причины вердикта (legit_reasons).

3. ОЦЕНИТЬ РЕАЛЬНУЮ РЫНОЧНУЮ СТОИМОСТЬ (ВТОРИЧКА УКРАИНЫ И МИР):
   - Оцени адекватную вилку цен для продажи б/у вещи в хорошем состоянии:
     * price_uah_min / price_uah_max (в гривнах для Shafa.ua и OLX).
     * price_usd_min / price_usd_max (в долларах для eBay и Grailed).

4. СФОРМИРОВАТЬ ТОЧНЫЕ ПОИСКОВЫЕ ЗАПРОСЫ (2-3 СЛОВА):
   - search_query_local: бренд + тип вещи на русском/украинском (например: "Nike кроссовки мужские", "Carhartt куртка").
   - search_query_global: бренд + линейка/модель латиницей (например: "Nike Dunk Low", "Carhartt Detroit jacket").

КРИТИЧЕСКИЕ ТРЕБОВАНИЯ:
- Верни ИСКЛЮЧИТЕЛЬНО валидный JSON без оберток markdown (без ```json).
- Внутри строковых значений НЕ используй двойные кавычки (заменяй их на одинарные).

Структура JSON:
{
  "brand": "Точное название бренда",
  "category_tier": "Категория вещи",
  "item_name": "Название модели или тип вещи",
  "era_or_year": "Примерные годы выпуска",
  "authenticity_verdict": "100% Оригинал / Оригинал / Сомнительно / Подделка",
  "authenticity_score": 95,
  "legit_reasons": [
    "Первая конкретная причина вердикта по бирке/швам",
    "Вторая причина по артикулу/материалам"
  ],
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

def prepare_image_part_sync(file_bytes: bytes) -> genai_types.Part:
    with Image.open(io.BytesIO(file_bytes)) as img:
        img = img.convert("RGB")
        img.thumbnail((900, 900), Image.Resampling.BILINEAR)
        out_buf = io.BytesIO()
        img.save(out_buf, format="JPEG", quality=75, optimize=False)
        return genai_types.Part.from_bytes(data=out_buf.getvalue(), mime_type="image/jpeg")

async def fetch_and_prep_photo(bot_instance: Bot, file_id: str) -> genai_types.Part:
    file_info = await bot_instance.get_file(file_id)
    stream = io.BytesIO()
    await bot_instance.download_file(file_info.file_path, destination=stream)
    return await asyncio.to_thread(prepare_image_part_sync, stream.getvalue())

def extract_clean_json(text: str) -> dict:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    start_idx = cleaned.find("{")
    end_idx = cleaned.rfind("}")
    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
        cleaned = cleaned[start_idx:end_idx + 1]

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    try:
        fixed = re.sub(r",\s*([}\]])", r"\1", cleaned)
        fixed = re.sub(r'(:\s*")([^"]*)"([^"]*)(")', r'\1\2\'\3\4', fixed)
        return json.loads(fixed)
    except Exception:
        logger.warning(f"Не удалось распарсить JSON: {cleaned[:200]}")
        raise ValueError("AI вернул некорректную структуру данных")

async def analyze_with_gemini_fallback(image_parts: list[genai_types.Part]) -> dict:
    if not ai_client:
        raise RuntimeError("Ключ GEMINI_API_KEY не установлен в настройках.")

    models_to_try = await asyncio.to_thread(get_candidate_models)
    last_error = None

    for model_name in models_to_try[:3]:
        try:
            logger.info(f"Отправка запроса к модели {model_name}...")
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    ai_client.models.generate_content,
                    model=model_name,
                    contents=[*image_parts, ANALYSIS_PROMPT],
                    config=genai_types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1
                    )
                ),
                timeout=45.0
            )

            raw_text = None
            if response and response.text:
                raw_text = response.text
            elif response and response.candidates and len(response.candidates) > 0:
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
    u = get_user_data(message.from_user.id, message.from_user.username)
    name = html.escape(message.from_user.first_name or "Пользователь")
    welcome_text = (
        f"👋 Привет, <b>{name}</b>!\n\n"
        "Я — <b>Resale & Legit Checker Bot</b>.\n"
        "Универсальный помощник для оценки любой одежды и обуви:\n"
        "• Распознаю любой бренд, точную модель и артикул\n"
        "• Проведу экспертный легит-чек по биркам, штрихкодам и фурнитуре\n"
        "• Покажу реальную стоимость на вторичке (Шафа, OLX) и проданные пары на eBay\n"
        "• Сгенерирую готовые поисковые ссылки на маркетплейсы\n\n"
        f"📊 Твой статус: <b>{html.escape(u['status_text'])}</b>."
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())

@dp.callback_query(F.data == "back_to_main")
async def cb_back_to_main(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    u = get_user_data(callback.from_user.id, callback.from_user.username)
    welcome_text = (
        "👋 <b>Главное меню</b>\n\n"
        f"📊 Твой статус: <b>{html.escape(u['status_text'])}</b>.\n\n"
        "Выберите действие ниже 👇"
    )
    await callback.message.edit_text(welcome_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())

@dp.callback_query(F.data == "show_profile")
async def cb_show_profile(callback: CallbackQuery):
    await callback.answer()
    u = get_user_data(callback.from_user.id, callback.from_user.username)
    text = (
        "👤 <b>Ваш профиль:</b>\n\n"
        f"🆔 Telegram ID: <code>{callback.from_user.id}</code>\n"
        f"⚡ Статус аккаунта: <b>{html.escape(u['status_text'])}</b>\n\n"
        f"• Использовано бесплатных сегодня: {u['checks_today']} из {FREE_CHECKS_PER_DAY}\n"
        f"• Дополнительных проверок: {u['extra_checks']}\n"
        f"• Подписка активна до: {u['premium_until'] or 'Нет активной'}\n\n"
        "Хотите снять любые ограничения? Оформите пакет или безлимит 👇"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Выбрать тариф", callback_data="show_plans")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

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
        # Проверяем, не является ли промокод кодом от блогера
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

        # Проверяем, не активировал ли этот пользователь промокод данного блогера ранее
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
        # Навсегда закрепляем пользователя за блогером
        query_db(
            "UPDATE users SET referred_by_blogger = ? WHERE user_id = ?",
            (blogger_tag, user_id)
        )

        activate_plan(user_id, plan_key, "blogger_promo", 0.0, "BLOGGER")
        u = get_user_data(user_id)

        # Уведомляем админа о новом привлеченном пользователе
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

def generate_random_promo_code() -> str:
    chars = string.ascii_uppercase + string.digits
    part1 = "".join(secrets.choice(chars) for _ in range(4))
    part2 = "".join(secrets.choice(chars) for _ in range(4))
    return f"CRIM-{part1}-{part2}"

def generate_blogger_promo_code(blogger_prefix: str) -> str:
    clean_prefix = re.sub(r"[^A-Za-z0-9]", "", blogger_prefix).upper()[:5]
    if not clean_prefix:
        clean_prefix = "BLOG"
    chars = string.ascii_uppercase + string.digits
    random_part = "".join(secrets.choice(chars) for _ in range(4))
    return f"{clean_prefix}-{random_part}"

def get_admin_blogger_plans_keyboard():
    buttons = []
    for plan_key, plan in PLANS.items():
        buttons.append([InlineKeyboardButton(text=f"🎁 {plan['title']}", callback_data=f"blog_plan:{plan_key}")])
    buttons.append([InlineKeyboardButton(text="📊 Список блогеров и статистика (20%)", callback_data="blog_stats")])
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
        "• Блогеру автоматически начисляется <b>20%</b> со всех будущих покупок его зрителей\n\n"
        "Выберите тариф-бонус, который получит аудитория блогера 👇"
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

    # Проверяем, не зарегистрирован ли блогер с таким ником
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

    # Генерируем уникальный промокод для блогера
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

@dp.callback_query(F.data == "blog_stats")
async def cb_show_blogger_stats(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id != ADMIN_USER_ID:
        return

    bloggers = query_db("SELECT * FROM bloggers ORDER BY id DESC")
    if not bloggers:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад к тарифам", callback_data="back_to_blog_menu")]
        ])
        await callback.message.edit_text(
            "📋 <b>Список блогеров пуст.</b>\n\nВы еще не зарегистрировали ни одного блогера.",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    text_lines = ["📊 <b>Анкеты блогеров и начисления (20%):</b>\n"]
    for idx, b in enumerate(bloggers, 1):
        plan = PLANS.get(b.get("plan_id"), {})
        plan_title = plan.get("title", "Бонус")
        tag = html.escape(str(b.get("tag", "Блогер")))
        code = b.get("promo_code", "НЕТ")
        refs = b.get("total_referrals", 0)
        uah = b.get("earnings_uah", 0.0)
        stars = b.get("earnings_stars", 0)

        text_lines.append(
            f"<b>{idx}. {tag}</b>\n"
            f"• Промокод: <code>{code}</code>\n"
            f"• Привлечено людей: <b>{refs} чел.</b>\n"
            f"• Бонус зрителям: {plan_title}\n"
            f"• 💰 Заработано (20%): <b>{uah:.2f} грн</b> | <b>{stars} ⭐</b>\n"
            "───────────────"
        )

    text_lines.append("\n<i>Вы можете в любой момент перевести блогеру указанную сумму вручную.</i>")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить список", callback_data="blog_stats")],
        [InlineKeyboardButton(text="➕ Зарегистрировать еще блогера", callback_data="back_to_blog_menu")],
        [InlineKeyboardButton(text="◀️ В главное меню", callback_data="back_to_main")]
    ])

    await callback.message.edit_text("\n".join(text_lines), parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "back_to_blog_menu")
async def cb_back_to_blog_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await cmd_promoblog(callback.message, state)

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
    if not check_can_proceed(user_id):
        text = (
            "⚠️ <b>Лимит проверок исчерпан!</b>\n\n"
            f"Вы использовали все {FREE_CHECKS_PER_DAY} бесплатные проверки на сегодня.\n"
            "Чтобы проверить вещь прямо сейчас, выберите пакет проверок или оформите безлимит 👇"
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
    await state.update_data(main_photo=message.photo[-1].file_id)
    await state.set_state(ClothingCheckFSM.waiting_for_neck_tag)
    await message.answer(
        "🏷 <b>Шаг 2 из 3: Главная бирка / Логотип</b>\n\n"
        "Сфотографируйте <b>бирку на воротнике/горловине</b> (для одежды) либо <b>язычок / внешний логотип</b> (для обуви).",
        parse_mode="HTML"
    )

@dp.message(StateFilter(ClothingCheckFSM.waiting_for_neck_tag), F.photo)
async def process_neck_tag_photo(message: Message, state: FSMContext):
    await state.update_data(neck_photo=message.photo[-1].file_id)
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
        image_parts = await asyncio.gather(
            fetch_and_prep_photo(bot, user_data["main_photo"]),
            fetch_and_prep_photo(bot, user_data["neck_photo"]),
            fetch_and_prep_photo(bot, message.photo[-1].file_id)
        )

        data = await analyze_with_gemini_fallback(image_parts)
        decrement_check(message.from_user.id)
        u = get_user_data(message.from_user.id)

        brand = html.escape(str(data.get("brand", "Не определен")))
        item_name = html.escape(str(data.get("item_name", "Вещь / Обувь")))
        tier = html.escape(str(data.get("category_tier", "Масс-маркет")))
        era = html.escape(str(data.get("era_or_year", "Не указан")))
        verdict = html.escape(str(data.get("authenticity_verdict", "Проверено")))

        local_q = data.get("search_query_local") or f"{brand} {item_name}"
        global_q = data.get("search_query_global") or f"{brand} {item_name}"
        links = generate_marketplace_links(local_q, global_q)

        reasons_list = data.get("legit_reasons", [])
        reasons_formatted = "\n".join([f"  • {html.escape(str(r))}" for r in reasons_list]) if reasons_list else "  • Детали и фурнитура соответствуют стандартам бренда"

        score = data.get("authenticity_score", 50)
        score_emoji = "🟢" if score >= 75 else ("🟡" if score >= 45 else "🔴")

        price_uah_min = data.get("price_uah_min", 0)
        price_uah_max = data.get("price_uah_max", 0)
        price_usd_min = data.get("price_usd_min", 0)
        price_usd_max = data.get("price_usd_max", 0)

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
        if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
            err_text = "⏳ Превышен лимит запросов к AI в минуту. Пожалуйста, подождите 30 секунд и нажмите «🔍 Проверить вещь» снова."
        elif "404" in err_msg or "NOT_FOUND" in err_msg:
            err_text = f"⚠️ Модель AI временно недоступна для ключа: {html.escape(err_msg[:80])}."
        else:
            err_text = f"❌ Ошибка обработки: {html.escape(err_msg[:100])}"

        try:
            await status_msg.edit_text(err_text, parse_mode="HTML")
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
    logger.info("Database initialized. Starting Telegram bot polling...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
