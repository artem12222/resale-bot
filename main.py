import asyncio
from datetime import date, datetime, timedelta
import html
import io
import json
import logging
import os
import sqlite3
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

# ======================== БЕЗОПАСНЫЕ НАСТРОЙКИ (ИЗ ОКРУЖЕНИЯ) ========================
# Все ключи и токены считываются из настроек сервера (Environment Variables)
# Благодаря этому GitHub не блокирует код при пуше
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Личный Telegram ID администратора (по умолчанию 0)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

# Ссылка на банку Monobank и токен для API-выписок
MONOBANK_JAR_URL = os.getenv("MONOBANK_JAR_URL", "https://send.monobank.ua/")
MONOBANK_TOKEN = os.getenv("MONOBANK_TOKEN", "")

# Число бесплатных проверок в день
FREE_CHECKS_PER_DAY = int(os.getenv("FREE_CHECKS_PER_DAY", "3"))
DB_NAME = "resale_bot.db"

# ======================== ТАРИФНАЯ СЕТКА ========================
PLANS = {
    "pack_15": {
        "title": "⚡ 15 проверок",
        "description": "Пакет из 15 проверок без ограничения по времени",
        "stars": 25,       # ~$0.50
        "uah": 30,
        "type": "checks",
        "amount": 15
    },
    "pack_50": {
        "title": "⚡ 50 проверок",
        "description": "Пакет из 50 проверок для активных поисков",
        "stars": 65,       # ~$1.30
        "uah": 75,
        "type": "checks",
        "amount": 50
    },
    "sub_7d": {
        "title": "🗓 Безлимит на 7 дней",
        "description": "Неограниченные проверки шмота на 1 неделю",
        "stars": 95,       # ~$1.90
        "uah": 110,
        "type": "days",
        "amount": 7
    },
    "sub_30d": {
        "title": "👑 Безлимит на 30 дней",
        "description": "Полный безлимит на месяц (Хит для ресейлеров)",
        "stars": 190,      # ~$3.80
        "uah": 220,
        "type": "days",
        "amount": 30
    },
    "lifetime": {
        "title": "♾ VIP Навсегда",
        "description": "Пожизненный доступ ко всем проверкам и обновлениям",
        "stars": 490,      # ~$9.80
        "uah": 550,
        "type": "lifetime",
        "amount": 999999
    }
}

# Список моделей для автоматического перехода в случае кратковременных 503 ошибок
CANDIDATE_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-3.6-flash",
    "gemini-2.5-pro"
]

# ======================== БАЗА ДАННЫХ ========================
def init_db():
    """Создает и обновляет таблицы базы данных с поддержкой подписок и оплат."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
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
        cursor.execute("PRAGMA table_info(users)")
        cols = [c[1] for c in cursor.fetchall()]
        if "extra_checks" not in cols:
            cursor.execute("ALTER TABLE users ADD COLUMN extra_checks INTEGER DEFAULT 0")
        if "premium_until" not in cols:
            cursor.execute("ALTER TABLE users ADD COLUMN premium_until TEXT DEFAULT NULL")
        if "is_lifetime" not in cols:
            cursor.execute("ALTER TABLE users ADD COLUMN is_lifetime INTEGER DEFAULT 0")

        cursor.execute("""
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
        conn.commit()

def get_user_data(user_id: int, username: Optional[str] = None) -> dict:
    """Возвращает актуальный статус подписки и проверок пользователя."""
    today_str = str(date.today())
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()

        if not row:
            cursor.execute(
                """INSERT INTO users (user_id, username, checks_today, last_check_date, is_premium, extra_checks, premium_until, is_lifetime) 
                   VALUES (?, ?, 0, ?, 0, 0, NULL, 0)""",
                (user_id, username, today_str)
            )
            conn.commit()
            return {
                "user_id": user_id,
                "checks_today": 0,
                "extra_checks": 0,
                "premium_until": None,
                "is_lifetime": 0,
                "status_text": f"{FREE_CHECKS_PER_DAY} бесплатных на сегодня"
            }

        checks_today = row["checks_today"]
        last_date = row["last_check_date"]
        extra_checks = row["extra_checks"]
        premium_until = row["premium_until"]
        is_lifetime = row["is_lifetime"]

        # Сброс лимита в полночь
        if last_date != today_str:
            checks_today = 0
            cursor.execute("UPDATE users SET checks_today = 0, last_check_date = ? WHERE user_id = ?", (today_str, user_id))
            conn.commit()

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
    """Проверяет доступность проверки вещи."""
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
    """Списывает проверку из лимита."""
    u = get_user_data(user_id)
    today_str = str(date.today())

    if u["is_lifetime"]:
        return
    if u["premium_until"] and u["premium_until"] >= today_str:
        return

    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        if u["checks_today"] < FREE_CHECKS_PER_DAY:
            cursor.execute(
                "UPDATE users SET checks_today = checks_today + 1, last_check_date = ? WHERE user_id = ?",
                (today_str, user_id)
            )
        elif u["extra_checks"] > 0:
            cursor.execute(
                "UPDATE users SET extra_checks = extra_checks - 1 WHERE user_id = ?",
                (user_id,)
            )
        conn.commit()

def activate_plan(user_id: int, plan_id: str, method: str, amount: float, currency: str):
    """Активирует тариф пользователю."""
    plan = PLANS.get(plan_id)
    if not plan:
        return

    today = date.today()
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT premium_until, extra_checks, is_lifetime FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        cur_until = row[0] if row else None
        cur_extra = row[1] if row else 0

        if plan["type"] == "lifetime":
            cursor.execute("UPDATE users SET is_lifetime = 1 WHERE user_id = ?", (user_id,))
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
            cursor.execute("UPDATE users SET premium_until = ? WHERE user_id = ?", (new_until, user_id))
        elif plan["type"] == "checks":
            new_checks = cur_extra + plan["amount"]
            cursor.execute("UPDATE users SET extra_checks = ? WHERE user_id = ?", (new_checks, user_id))

        cursor.execute(
            """INSERT INTO payments (user_id, plan_id, amount, currency, method, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'success', ?)""",
            (user_id, plan_id, amount, currency, method, datetime.now().isoformat())
        )
        conn.commit()

# ======================== FSM И АНАЛИЗ GEMINI ========================
class ClothingCheckFSM(StatesGroup):
    waiting_for_main_photo = State()
    waiting_for_neck_tag = State()
    waiting_for_care_tag = State()

try:
    ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
except Exception as err:
    logger.error(f"Ошибка настройки Gemini API: {err}")
    ai_client = None

ANALYSIS_PROMPT = """
Ты — профессиональный эксперт по ресейлу, оценке одежды и легит-чеку.
Тебе отправлены 3 фотографии одной вещи:
1) Общий вид одежды.
2) Горловина / воротник / нашивка бренда.
3) Внутренняя сервисная бирка (wash/care tag) с артикулом, составом и кодами.

ВАЖНЫЕ ПРАВИЛА РАСПОЗНАВАНИЯ И ОЦЕНКИ:
1. КОРРЕКТНЫЙ OCR И ЛИНЕЙКИ:
   - Если бренд Pull & Bear и на принте/бирке надпись STWD — это линейка STWD (Stay White Dope), а НЕ "STAX".
   - Если Zara — различай линейки: Man, Woman, TRF (Trafaluc), Basic, Origins, Studio.
   - Считывай точный артикул (Ref / Art number) с нижней бирки, если он виден.

2. СЕГМЕНТ И РЕАЛЬНЫЕ РЫНОЧНЫЕ ЦЕНЫ (ВТОРИЧКА УКРАИНЫ И СНГ):
   - "Масс-маркет" (Pull & Bear, Zara, Bershka, H&M, Reserved, Cropp, House):
     * Оригинальность: 99-100% оригинал (масс-маркет не подделывают).
     * Футболки/майки б/у: 100 – 250 грн ($2.5 – $6 USD).
     * Рубашки/худи/свитшоты б/у: 250 – 500 грн ($6 – $12 USD).
     * Жилетки/куртки б/у: 400 – 900 грн ($10 – $22 USD).
   - "Винтаж / Ворквир / Стритвир" (Carhartt, Stussy, The North Face, Nike Vintage, Dickies, Levi's):
     * Оценивай по реальным ценам проданных лотов на eBay Sold и Grailed.
   - "Премиум / Люкс" (Stone Island, CP Company, Ralph Lauren, Arc'teryx, Prada):
     * Высокий риск подделок, строгая проверка патчей, Certilogo, штрихкодов и швов.

3. ЛАКОНИЧНЫЕ ПОИСКОВЫЕ ЗАПРОСЫ (МАКСИМУМ 2-3 СЛОВА):
   - search_query_local (для OLX и Шафы): бренд + тип вещи (например: "Pull and Bear футболка", "Zara жилетка").
   - search_query_global (для eBay/Grailed): бренд + модель латиницей (например: "Pull and Bear STWD tee").

Верни СТРОГИЙ JSON без оформления markdown:
{
  "brand": "Точное название бренда",
  "category_tier": "Масс-маркет / Стритвир и Ворквир / Премиум и Люкс",
  "item_name": "Название модели или линейки",
  "era_or_year": "Примерные годы выпуска",
  "authenticity_verdict": "100% Оригинал / Оригинал / Фейк / Сомнительно",
  "authenticity_score": 95,
  "legit_reasons": [
    "Оригинальная фирменная бирка",
    "Швы и сервисный ярлык соответствуют стандартам"
  ],
  "price_uah_min": 150,
  "price_uah_max": 250,
  "price_usd_min": 4,
  "price_usd_max": 7,
  "search_query_local": "Pull and Bear футболка",
  "search_query_global": "Pull and Bear STWD tee"
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

def prepare_image_part(file_stream: io.BytesIO) -> genai_types.Part:
    """Сжимает картинку для ускорения запроса в 10 раз."""
    with Image.open(file_stream) as img:
        img = img.convert("RGB")
        img.thumbnail((1280, 1280), Image.Resampling.LANCZOS)
        out_buf = io.BytesIO()
        img.save(out_buf, format="JPEG", quality=82, optimize=True)
        return genai_types.Part.from_bytes(data=out_buf.getvalue(), mime_type="image/jpeg")

async def analyze_with_gemini_fallback(image_parts: list[genai_types.Part]) -> dict:
    if not ai_client:
        raise RuntimeError("GEMINI_API_KEY не установлен в переменных окружения.")

    last_error = None
    for model_name in CANDIDATE_MODELS:
        for attempt in range(2):
            try:
                logger.info(f"Обращение к {model_name} (попытка {attempt + 1})...")
                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        ai_client.models.generate_content,
                        model=model_name,
                        contents=[*image_parts, ANALYSIS_PROMPT],
                        config=genai_types.GenerateContentConfig(
                            response_mime_type="application/json",
                            temperature=0.2
                        )
                    ),
                    timeout=20.0
                )
                if response and response.text:
                    raw = response.text.strip()
                    if raw.startswith("```"):
                        raw = raw.strip("`")
                        if raw.startswith("json"):
                            raw = raw[4:].strip()
                    return json.loads(raw)
            except Exception as exc:
                err_msg = str(exc)
                logger.warning(f"Сбой модели {model_name}: {err_msg}")
                last_error = exc
                if "404" in err_msg or "NOT_FOUND" in err_msg:
                    break
                await asyncio.sleep(1.0)
    raise last_error or RuntimeError("Все AI-модели временно недоступны.")

# ======================== ИНИЦИАЛИЗАЦИЯ И МЕНЮ ========================
bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
dp = Dispatcher(storage=MemoryStorage())

def get_main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Проверить вещь (3 фото)", callback_data="start_check")],
        [
            InlineKeyboardButton(text="💎 Тарифы и Безлимит", callback_data="show_plans"),
            InlineKeyboardButton(text="👤 Мой профиль", callback_data="show_profile")
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
    u = get_user_data(message.from_user.id, message.from_user.username)
    welcome_text = (
        f"👋 Привет, {message.from_user.first_name}!\n\n"
        "Я — **Resale & Legit Checker Bot**.\n"
        "Помогу оценить шмотку перед покупкой или продажей:\n"
        "• Распознаю бренд, модель и год выпуска\n"
        "• Проведу легит-чек по биркам и фурнитуре\n"
        "• Покажу реальную цену в Украине и проданные лоты на eBay\n"
        "• Сгенерирую готовые ссылки на Shafa.ua, OLX, eBay, Grailed\n\n"
        f"📊 Твой статус: **{u['status_text']}**."
    )
    await message.answer(welcome_text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())

@dp.callback_query(F.data == "back_to_main")
async def cb_back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    u = get_user_data(callback.from_user.id, callback.from_user.username)
    welcome_text = (
        f"👋 Главное меню\n\n"
        f"📊 Твой статус: **{u['status_text']}**.\n\n"
        "Выберите действие ниже 👇"
    )
    await callback.message.edit_text(welcome_text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "show_profile")
async def cb_show_profile(callback: CallbackQuery):
    u = get_user_data(callback.from_user.id, callback.from_user.username)
    text = (
        f"👤 **Ваш профиль:**\n\n"
        f"🆔 Telegram ID: `{callback.from_user.id}`\n"
        f"⚡ Статус аккаунта: **{u['status_text']}**\n\n"
        f"• Использовано бесплатных сегодня: {u['checks_today']} из {FREE_CHECKS_PER_DAY}\n"
        f"• Дополнительных проверок: {u['extra_checks']}\n"
        f"• Подписка активна до: {u['premium_until'] or 'Нет активной'}\n\n"
        "Хотите снять любые ограничения? Оформите пакет или безлимит 👇"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Выбрать тариф", callback_data="show_plans")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()

# ======================== ОПЛАТА: ЗВЕЗДЫ И МОНОБАНК ========================
@dp.callback_query(F.data == "show_plans")
async def cb_show_plans(callback: CallbackQuery):
    text = (
        "💎 **Тарифы и Безлимитный доступ:**\n\n"
        "⚡ **15 проверок** — `30 грн` / `25 ⭐` (без срока сгорания)\n"
        "⚡ **50 проверок** — `75 грн` / `65 ⭐` (для активных поисков)\n"
        "🗓 **Безлимит на 7 дней** — `110 грн` / `95 ⭐` (недельный доступ)\n"
        "👑 **Безлимит на 30 дней** — `220 грн` / `190 ⭐` (Хит для ресейла!)\n"
        "♾ **VIP Навсегда** — `550 грн` / `490 ⭐` (вечный доступ ко всем обновам)\n\n"
        "Выберите тариф для перехода к оплате 👇"
    )
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=get_plans_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("choose_plan:"))
async def cb_choose_payment_method(callback: CallbackQuery):
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        await callback.answer("Тариф не найден.", show_alert=True)
        return

    text = (
        f"Вы выбрали: **{plan['title']}**\n"
        f"📝 {plan['description']}\n\n"
        f"Стоимость:\n"
        f"• Через **Telegram Stars**: **{plan['stars']} ⭐** (в 1 клик прямо в Telegram)\n"
        f"• Через **Монобанк**: **{plan['uah']} грн** (перевод на Банку)\n\n"
        "Выберите способ оплаты:"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⭐ Оплатить {plan['stars']} Stars (Telegram)", callback_data=f"pay_stars:{plan_key}")],
        [InlineKeyboardButton(text=f"💳 Оплатить {plan['uah']} грн (Монобанка)", callback_data=f"pay_mono:{plan_key}")],
        [InlineKeyboardButton(text="◀️ Назад к тарифам", callback_data="show_plans")]
    ])
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("pay_stars:"))
async def cb_pay_stars(callback: CallbackQuery):
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        await callback.answer("Ошибка тарифа.")
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
    await callback.answer()

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
            "🎉 **Оплата успешно получена!**\n\n"
            f"Вам активирован тариф: **{plan['title']}**.\n"
            f"📊 Ваш новый баланс: **{u['status_text']}**.\n\n"
            "Приятного пользования! Нажмите кнопку ниже, чтобы проверить вещь 👇"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Проверить вещь", callback_data="start_check")]
        ])
        await message.answer(congrats_text, parse_mode="Markdown", reply_markup=kb)

@dp.callback_query(F.data.startswith("pay_mono:"))
async def cb_pay_mono(callback: CallbackQuery):
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    if not plan:
        await callback.answer("Ошибка тарифа.")
        return

    user_id = callback.from_user.id
    jar_payment_link = f"{MONOBANK_JAR_URL}?a={plan['uah']}"

    text = (
        f"💳 **Оплата через Monobank Банку:**\n\n"
        f"Тариф: **{plan['title']}**\n"
        f"Сумма к оплате: **{plan['uah']} грн**\n\n"
        f"⚠️ **ВАЖНО:** При оплате в поле «Коментар» ОБЯЗАТЕЛЬНО укажите ваш ID:\n"
        f"👉 `ID: {user_id}` (нажмите, чтобы скопировать)\n\n"
        "После перевода нажмите кнопку **«🔄 Проверить оплату»** ниже 👇"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"↗️ Перейти в Банку ({plan['uah']} грн)", url=jar_payment_link)],
        [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_mono:{plan_key}")],
        [InlineKeyboardButton(text="📩 Я оплатил (Отправить чек админу)", callback_data=f"notify_admin_mono:{plan_key}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="show_plans")]
    ])
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("check_mono:"))
async def cb_check_monobank_statement(callback: CallbackQuery):
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    user_id = callback.from_user.id

    if not MONOBANK_TOKEN:
        await callback.answer(
            "Авто-проверка через API не настроена. Нажмите кнопку «📩 Я оплатил», чтобы уведомить админа!",
            show_alert=True
        )
        return

    await callback.answer("Проверяю поступления на Банку...", show_alert=False)

    try:
        headers = {"X-Token": MONOBANK_TOKEN}
        now_ts = int(datetime.now().timestamp())
        from_ts = now_ts - 7200

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get("https://api.monobank.ua/personal/client-info", headers=headers)
            if resp.status_code != 200:
                await callback.message.answer("⚠️ Временная задержка ответа банка. Нажмите «📩 Я оплатил», чтобы выдать доступ вручную.")
                return

            client_info = resp.json()
            jars = client_info.get("jars", [])
            jar_account = jars[0].get("id") if jars else None

            if not jar_account:
                accounts = client_info.get("accounts", [])
                jar_account = accounts[0].get("id") if accounts else None

            stmt_url = f"https://api.monobank.ua/personal/statement/{jar_account}/{from_ts}/{now_ts}"
            stmt_resp = await client.get(stmt_url, headers=headers)

            if stmt_resp.status_code == 200:
                transactions = stmt_resp.json()
                found = False
                expected_kopecks = plan["uah"] * 100

                for tx in transactions:
                    comment = tx.get("comment", "")
                    amount = tx.get("amount", 0)
                    if str(user_id) in comment and amount >= expected_kopecks:
                        found = True
                        break

                if found:
                    activate_plan(user_id, plan_key, "monobank_auto", plan["uah"], "UAH")
                    u = get_user_data(user_id)
                    await callback.message.answer(
                        f"🎉 **Оплата подтверждена!**\nТариф **{plan['title']}** успешно активирован!\nСтатус: {u['status_text']}",
                        reply_markup=get_main_menu_keyboard()
                    )
                    return

            await callback.message.answer(
                "⏳ Платёж пока не поступил или комментарий с вашим ID не найден в выписке.\n"
                "Если вы уже перевели средства, нажмите кнопку **«📩 Я оплатил (Отправить чек админу)»**."
            )
    except Exception as e:
        logger.error(f"Ошибка проверки Монобанка: {e}")
        await callback.message.answer("Не удалось связаться с банком. Нажмите «📩 Я оплатил», чтобы передать заявку владельцу.")

# ======================== АДМИНИСТРАТОР ========================
@dp.callback_query(F.data.startswith("notify_admin_mono:"))
async def cb_notify_admin_mono(callback: CallbackQuery):
    plan_key = callback.data.split(":")[1]
    plan = PLANS.get(plan_key)
    user_id = callback.from_user.id
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"id {user_id}"

    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить оплату", callback_data=f"adm_approve:{user_id}:{plan_key}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adm_reject:{user_id}")
        ]
    ])

    admin_msg = (
        f"🔔 **Новая оплата на Монобанку!**\n\n"
        f"Пользователь: {username} (`{user_id}`)\n"
        f"Тариф: **{plan['title']}**\n"
        f"Сумма к зачислению: **{plan['uah']} грн**\n\n"
        "Проверьте выписку в приложении Монобанка и подтвердите зачисление:"
    )

    if ADMIN_USER_ID != 0:
        try:
            await bot.send_message(ADMIN_USER_ID, admin_msg, parse_mode="Markdown", reply_markup=admin_kb)
            await callback.message.answer(
                "✅ **Запрос отправлен администратору!**\n"
                "После подтверждения поступления на Банку бот сразу пришлет вам уведомление об активации.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление админу: {e}")
            await callback.message.answer("Заявка зафиксирована. Администратор проверит поступление.")
    else:
        await callback.message.answer("Заявка сохранена в системе.")

    await callback.answer()

@dp.callback_query(F.data.startswith("adm_approve:"))
async def cb_admin_approve(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("Доступ только для администратора.", show_alert=True)
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
                f"🎉 **Ваша оплата подтверждена!**\n\n"
                f"Тариф: **{plan['title']}** успешно начислен.\n"
                f"📊 Ваш статус: **{u['status_text']}**.\n\n"
                "Приятных проверок вещей!",
                parse_mode="Markdown",
                reply_markup=get_main_menu_keyboard()
            )
        except Exception:
            pass

        await callback.message.edit_text(f"✅ Успешно! Пользователю `{target_user_id}` выдан тариф {plan['title']}.")
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_reject:"))
async def cb_admin_reject(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("Только для админа.")
        return
    target_user_id = int(callback.data.split(":")[1])
    try:
        await bot.send_message(
            target_user_id,
            "❌ Платеж не был обнаружен в выписке Банки. Если произошла ошибка, свяжитесь с поддержкой."
        )
    except Exception:
        pass
    await callback.message.edit_text(f"❌ Заявка пользователя `{target_user_id}` отклонена.")
    await callback.answer()

@dp.message(Command("give"))
async def cmd_give_access(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.answer("Формат: `/give <user_id> <plan_key>`\nКлючи: pack_15, pack_50, sub_7d, sub_30d, lifetime", parse_mode="Markdown")
        return
    uid = int(parts[1])
    pkey = parts[2]
    if pkey in PLANS:
        activate_plan(uid, pkey, "admin_grant", 0, "FREE")
        u = get_user_data(uid)
        await message.answer(f"Готово! Пользователю `{uid}` начислен тариф {PLANS[pkey]['title']}. Новый статус: {u['status_text']}")
    else:
        await message.answer("Неверный plan_key.")

# ======================== СБОР И АНАЛИЗ ФОТОГРАФИЙ ========================
@dp.callback_query(F.data == "start_check")
async def cb_start_check(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not check_can_proceed(user_id):
        text = (
            "⚠️ **Лимит проверок исчерпан!**\n\n"
            f"Вы использовали все {FREE_CHECKS_PER_DAY} бесплатные проверки на сегодня.\n"
            "Чтобы проверить вещь прямо сейчас, выберите пакет проверок или оформите безлимит 👇"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Снять лимит (Тарифы)", callback_data="show_plans")],
            [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main")]
        ])
        await callback.message.answer(text, parse_mode="Markdown", reply_markup=kb)
        await callback.answer()
        return

    await state.set_state(ClothingCheckFSM.waiting_for_main_photo)
    await callback.message.answer(
        "📸 **Шаг 1 из 3:**\n"
        "Пришлите фотографию **вещи целиком** (общий план спереди или сзади).",
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.message(StateFilter(ClothingCheckFSM.waiting_for_main_photo), F.photo)
async def process_main_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await state.update_data(main_photo=photo_id)
    await state.set_state(ClothingCheckFSM.waiting_for_neck_tag)
    await message.answer(
        "🏷 **Шаг 2 из 3:**\n"
        "Отлично! Теперь сфотографируйте **бирку на воротнике / горловине** крупным планом при хорошем освещении.",
        parse_mode="Markdown"
    )

@dp.message(StateFilter(ClothingCheckFSM.waiting_for_neck_tag), F.photo)
async def process_neck_tag_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await state.update_data(neck_photo=photo_id)
    await state.set_state(ClothingCheckFSM.waiting_for_care_tag)
    await message.answer(
        "🧵 **Шаг 3 из 3:**\n"
        "Последний шаг: отправьте **нижнюю сервисную бирку** (где указаны состав, артикул, RN-код, стирка).",
        parse_mode="Markdown"
    )

@dp.message(StateFilter(ClothingCheckFSM.waiting_for_care_tag), F.photo)
async def process_care_tag_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    user_data = await state.get_data()
    await state.clear()

    main_photo_id = user_data["main_photo"]
    neck_photo_id = user_data["neck_photo"]
    care_photo_id = photo_id

    status_msg = await message.answer("⏳ Анализирую бирки, швы и артикулы через Gemini AI... Это займет 3–6 секунд.")

    try:
        image_parts = []
        for pid in [main_photo_id, neck_photo_id, care_photo_id]:
            file_info = await bot.get_file(pid)
            file_stream = io.BytesIO()
            await bot.download_file(file_info.file_path, destination=file_stream)
            file_stream.seek(0)
            image_parts.append(prepare_image_part(file_stream))

        data = await analyze_with_gemini_fallback(image_parts)
        decrement_check(message.from_user.id)
        u = get_user_data(message.from_user.id)

        brand = html.escape(str(data.get("brand", "Не указан")))
        item_name = html.escape(str(data.get("item_name", "Вещь")))
        tier = html.escape(str(data.get("category_tier", "Масс-маркет")))
        era = html.escape(str(data.get("era_or_year", "Неизвестно")))
        verdict = html.escape(str(data.get("authenticity_verdict", "Проверено")))

        local_q = data.get("search_query_local") or f"{brand} {item_name}"
        global_q = data.get("search_query_global") or f"{brand} {item_name}"
        links = generate_marketplace_links(local_q, global_q)

        reasons_list = data.get("legit_reasons", [])
        reasons_formatted = "\n".join([f"  • {html.escape(str(r))}" for r in reasons_list]) if reasons_list else "  • Детали соответствуют стандартам бренда"

        score = data.get("authenticity_score", 50)
        score_emoji = "🟢" if score >= 75 else ("🟡" if score >= 45 else "🔴")

        price_uah_min = data.get("price_uah_min", 0)
        price_uah_max = data.get("price_uah_max", 0)
        price_usd_min = data.get("price_usd_min", 0)
        price_usd_max = data.get("price_usd_max", 0)

        result_message = (
            f"🏷 <b>Бренд:</b> {brand}\n"
            f"👕 <b>Модель/Линейка:</b> {item_name}\n"
            f"📦 <b>Сегмент:</b> {tier}\n"
            f"📅 <b>Период:</b> {era}\n\n"
            f"{score_emoji} <b>Легит-чек:</b> {verdict} ({score}%)\n"
            f"<b>Обоснование:</b>\n{reasons_formatted}\n\n"
            f"💰 <b>Реальная вторичка (Шафа / OLX):</b>\n"
            f"👉 <b>{price_uah_min} – {price_uah_max} грн</b> (~${price_usd_min} – ${price_usd_max})\n\n"
            f"📊 Остаток проверок: <b>{u['status_text']}</b>"
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

    except Exception as exc:
        logger.error(f"Ошибка при обработке запроса: {exc}", exc_info=True)
        try:
            await status_msg.edit_text(
                "❌ Сервера Gemini кратковременно перегружены. "
                "Пожалуйста, повторите попытку через минуту по кнопке /start."
            )
        except Exception:
            pass

# ======================== ВЕБ-СЕРВЕР ДЛЯ RENDER И ЗАПУСК ========================
async def handle_health_check(request):
    """Ответ для Render и UptimeRobot, чтобы сервис не засыпал."""
    return web.Response(text="Bot is running 24/7!", status=200)

async def start_background_web():
    """Слушает порт Render (PORT), предотвращая ошибку Port scan timeout."""
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
        logger.error("КРИТИЧЕСКАЯ ОШИБКА: Не задана переменная TELEGRAM_BOT_TOKEN!")
        return

    init_db()
    await start_background_web()
    logger.info("База данных инициализирована. Запуск Telegram бота...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
