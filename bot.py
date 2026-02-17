import logging
import asyncio
import html
import re
import warnings
import urllib.parse
import os
import json
import hashlib
from datetime import datetime
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

from flask import Flask
from jobspy import scrape_jobs
import pandas as pd

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

import database as db

# =========================================================
# LinkedIt By Abdulrahman - Telegram Job Bot (Render-ready)
# Phase 1 Features:
# - User profiles with saved preferences
# - Job favorites (save/view/delete)
# - Job alerts with automatic notifications
# - Expanded search sources (Indeed, LinkedIn, Glassdoor, Google)
# - Caching, concurrent search, pagination
# - Promotion links (Bot, Channel, WhatsApp)
# =========================================================

# --- Caching ---
try:
    from cachetools import TTLCache
except ImportError:
    class TTLCache(dict):
        def __init__(self, maxsize=100, ttl=1800):
            super().__init__()
            self.maxsize = maxsize

# --- Flask Server to keep Render alive / health check ---
flask_app = Flask("")

@flask_app.route("/")
def home():
    return "LinkedIt Bot is running!"

@flask_app.route("/health")
def health():
    stats = db.get_bot_stats()
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "stats": stats,
    }, 200

# --- Bot Settings (ENV only) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WHATSAPP_LINK = os.environ.get("WHATSAPP_LINK", "")
BOT_LINK = os.environ.get("BOT_LINK", "")
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "")

# Alert check interval in seconds (default: 6 hours)
ALERT_INTERVAL = int(os.environ.get("ALERT_INTERVAL", "21600"))

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    flask_app.run(host="0.0.0.0", port=port)

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=FutureWarning)

# --- Cache: 30 minutes, max 200 entries ---
job_cache = TTLCache(maxsize=200, ttl=1800)

# --- Thread pool for concurrent scraping ---
executor = ThreadPoolExecutor(max_workers=4)

# --- Constants ---
RESULTS_PER_PAGE = 5
MAX_RESULTS = 20
MAX_FAVORITES = 50
MAX_ALERTS = 5
HOURS_OLD = 168       # 1 week
SEARCH_TIMEOUT = 90   # seconds

# Supported Countries
COUNTRIES = {
    "qa": {"name": "قطر 🇶🇦", "flag": "🇶🇦", "name_en": "Qatar", "indeed_country": "Qatar", "location": "Qatar"},
    "ae": {"name": "الإمارات 🇦🇪", "flag": "🇦🇪", "name_en": "United Arab Emirates", "indeed_country": "United Arab Emirates", "location": "United Arab Emirates"},
    "sa": {"name": "السعودية 🇸🇦", "flag": "🇸🇦", "name_en": "Saudi Arabia", "indeed_country": "Saudi Arabia", "location": "Saudi Arabia"},
    "bh": {"name": "البحرين 🇧🇭", "flag": "🇧🇭", "name_en": "Bahrain", "indeed_country": "Bahrain", "location": "Bahrain"},
}

# Expanded search sources
SEARCH_SITES = ["indeed", "linkedin", "glassdoor", "google"]

# Job Categories
JOB_CATEGORIES = {
    "eng": {"name": "هندسة 🔧", "query": "engineer"},
    "it": {"name": "تقنية المعلومات 💻", "query": "IT software developer"},
    "acc": {"name": "محاسبة 📊", "query": "accountant"},
    "mkt": {"name": "تسويق 📢", "query": "marketing"},
    "hr": {"name": "موارد بشرية 👥", "query": "human resources"},
    "med": {"name": "طب وصحة 🏥", "query": "medical healthcare"},
    "edu": {"name": "تعليم 📚", "query": "teacher education"},
    "sales": {"name": "مبيعات 🛒", "query": "sales"},
    "admin": {"name": "إدارة 🏢", "query": "admin manager"},
    "fin": {"name": "مالية وبنوك 🏦", "query": "finance banking"},
}


# ========================
# Helper Functions
# ========================

def escape_html(text: str) -> str:
    if not text:
        return ""
    return html.escape(str(text))

def extract_email_from_text(text: str) -> str:
    if not text:
        return ""
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", str(text))
    return emails[0] if emails else ""

def _build_promo_keyboard_rows() -> list:
    """Build promotion button rows dynamically."""
    rows = []
    promo_row = []
    if WHATSAPP_LINK:
        promo_row.append(InlineKeyboardButton("📱 واتساب", url=WHATSAPP_LINK))
    if CHANNEL_LINK:
        promo_row.append(InlineKeyboardButton("📢 قناة الوظائف", url=CHANNEL_LINK))
    if promo_row:
        rows.append(promo_row)
    if BOT_LINK:
        rows.append([InlineKeyboardButton("🤖 شارك البوت مع أصدقائك", url=BOT_LINK)])
    return rows

def _generate_job_id(job: dict) -> str:
    """Generate a short unique ID for a job based on URL."""
    job_url = str(job.get("job_url", ""))
    return hashlib.md5(job_url.encode()).hexdigest()[:8]

def _extract_job_email(job: dict) -> str:
    """Extract email from job data."""
    emails_val = job.get("emails", "")
    email = ""
    if emails_val and str(emails_val) not in ("nan", "", "None", "[]"):
        if isinstance(emails_val, list):
            email = emails_val[0]
        else:
            found = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", str(emails_val))
            email = found[0] if found else ""
    if not email:
        email = extract_email_from_text(str(job.get("description", "")))
    return email

def format_job_message(job, country_name: str, show_save_btn: bool = True) -> tuple[str, str, list]:
    """Format job message. Returns (text, whatsapp_url, keyboard_buttons)."""
    title = escape_html(str(job.get("title", "غير محدد")))
    company = escape_html(str(job.get("company", "غير محدد")))
    if company in ("nan", "None", ""):
        company = "غير محدد"

    location_val = str(job.get("location", ""))
    location_display = country_name
    if location_val and location_val not in ("nan", "", "None"):
        city = location_val.split(",")[0].strip()
        location_display = f"{city}، {country_name}"

    description = str(job.get("description", ""))
    if description and description not in ("nan", "", "None"):
        description = re.sub(r"<[^>]+>", "", description)
        description = re.sub(r"\s+", " ", description).strip()
        description = description[:450] + "..." if len(description) > 450 else description
        description = escape_html(description)
    else:
        description = "لا يوجد وصف متاح حالياً"

    job_url = str(job.get("job_url", ""))
    if job_url in ("nan", "", "None"):
        job_url = ""

    email = _extract_job_email(job)

    site = str(job.get("site", ""))
    source_names = {"indeed": "Indeed", "linkedin": "LinkedIn", "google": "Google Jobs", "glassdoor": "Glassdoor"}
    source_name = source_names.get(site, site)

    # Build job message
    msg = "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"💼 <b>{title} - {location_display}</b>\n"
    msg += f"🏢 {company}\n"
    if source_name:
        msg += f"🌐 المصدر: {escape_html(source_name)}\n"
    msg += f"\n{description}\n"
    if email:
        msg += f"\n📧 <b>التواصل:</b> {escape_html(email)}\n"
    if job_url:
        msg += f"\n🔗 <a href='{job_url}'>رابط التقديم على الوظيفة</a>\n"
    if CHANNEL_LINK:
        msg += f"\n📢 <a href='{CHANNEL_LINK}'>انضم لقناة الوظائف</a>"
    if WHATSAPP_LINK:
        msg += f"\n👉 <a href='{WHATSAPP_LINK}'>تابعنا على واتساب</a>"
    if BOT_LINK:
        msg += f"\n🤖 <a href='{BOT_LINK}'>شارك البوت مع أصدقائك</a>"
    msg += "\n━━━━━━━━━━━━━━━━━━━━━"

    # Build share text
    share_text = f"💼 {title} - {location_display}\n"
    if company != "غير محدد":
        share_text += f"🏢 {company}\n"
    if job_url:
        share_text += f"🔗 التقديم: {job_url}\n"
    if email:
        share_text += f"📧 التواصل: {email}\n"
    if CHANNEL_LINK:
        share_text += f"\n📢 قناة الوظائف: {CHANNEL_LINK}"
    if WHATSAPP_LINK:
        share_text += f"\n📱 واتساب: {WHATSAPP_LINK}"
    if BOT_LINK:
        share_text += f"\n🤖 جرب البوت: {BOT_LINK}"
    whatsapp_url = f"https://api.whatsapp.com/send?text={urllib.parse.quote(share_text)}"

    # Build keyboard buttons
    buttons = []
    btn_row = [InlineKeyboardButton("📤 واتساب", url=whatsapp_url)]
    if show_save_btn and job_url:
        job_id = _generate_job_id(job)
        btn_row.append(InlineKeyboardButton("⭐ حفظ", callback_data=f"savejob_{job_id}"))
    buttons.append(btn_row)

    return msg, whatsapp_url, buttons


# ========================
# Search Logic (with caching + concurrency + expanded sources)
# ========================

def _search_single_country(search_term: str, cc: str) -> list:
    """Scrape jobs for a single country (runs in thread pool)."""
    try:
        jobs = scrape_jobs(
            site_name=SEARCH_SITES,
            search_term=search_term,
            location=COUNTRIES[cc]["location"],
            country_indeed=COUNTRIES[cc]["indeed_country"],
            results_wanted=MAX_RESULTS,
            hours_old=HOURS_OLD,
            verbose=0,
        )
        if jobs is not None and not jobs.empty:
            results = []
            for _, row in jobs.iterrows():
                job_dict = row.to_dict()
                job_dict["_country_name"] = COUNTRIES[cc]["name"]
                job_dict["_email"] = _extract_job_email(job_dict)
                results.append(job_dict)
            return results
    except Exception as e:
        logger.error("Error in %s: %s", cc, e)
    return []


async def search_jobs_logic(search_term: str, country_code: str) -> list:
    """Search with caching and concurrent country scraping."""
    cache_key = f"{search_term.lower().strip()}:{country_code}"

    if cache_key in job_cache:
        logger.info("Cache hit for: %s", cache_key)
        return job_cache[cache_key]

    logger.info("Cache miss for: %s, starting search...", cache_key)
    loop = asyncio.get_event_loop()

    if country_code == "all":
        tasks = [
            loop.run_in_executor(executor, _search_single_country, search_term, cc)
            for cc in COUNTRIES.keys()
        ]
        try:
            results_lists = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=SEARCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("Search timed out for: %s", search_term)
            results_lists = []

        all_jobs = []
        for result in results_lists:
            if isinstance(result, list):
                all_jobs.extend(result)
            elif isinstance(result, Exception):
                logger.error("Search error: %s", result)
    else:
        try:
            all_jobs = await asyncio.wait_for(
                loop.run_in_executor(executor, _search_single_country, search_term, country_code),
                timeout=SEARCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("Search timed out for: %s in %s", search_term, country_code)
            all_jobs = []

    # Remove duplicates by job_url
    seen_urls = set()
    unique_jobs = []
    for job in all_jobs:
        url = str(job.get("job_url", ""))
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_jobs.append(job)

    job_cache[cache_key] = unique_jobs
    return unique_jobs


# ========================
# Bot Handlers - Main Menu
# ========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.get_or_create_user(user.id, user.username or "", user.first_name or "")

    keyboard = [
        [InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")],
        [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")],
        [
            InlineKeyboardButton("⭐ المفضلة", callback_data="my_favorites"),
            InlineKeyboardButton("🔔 تنبيهاتي", callback_data="my_alerts"),
        ],
        [InlineKeyboardButton("👤 ملفي الشخصي", callback_data="my_profile")],
    ]
    keyboard.extend(_build_promo_keyboard_rows())

    await update.message.reply_text(
        f"👋 أهلاً بك <b>{escape_html(user.first_name or 'صديقي')}</b> في بوت <b>LinkedIt By Abdulrahman</b>\n\n"
        "أنا أساعدك في العثور على أحدث الوظائف في دول الخليج (قطر، الإمارات، السعودية، البحرين).\n\n"
        "🔍 <b>بحث</b> - ابحث عن وظيفة بالاسم أو التصنيف\n"
        "⭐ <b>المفضلة</b> - الوظائف التي حفظتها\n"
        "🔔 <b>التنبيهات</b> - احصل على إشعارات بالوظائف الجديدة\n"
        "👤 <b>ملفي</b> - إدارة تفضيلاتك\n\n"
        "اختر من القائمة أدناه للبدء:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _build_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Build the main menu keyboard."""
    keyboard = [
        [InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")],
        [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")],
        [
            InlineKeyboardButton("⭐ المفضلة", callback_data="my_favorites"),
            InlineKeyboardButton("🔔 تنبيهاتي", callback_data="my_alerts"),
        ],
        [InlineKeyboardButton("👤 ملفي الشخصي", callback_data="my_profile")],
    ]
    keyboard.extend(_build_promo_keyboard_rows())
    return InlineKeyboardMarkup(keyboard)


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("🇶🇦 قطر", callback_data="country_qa"),
            InlineKeyboardButton("🇦🇪 الإمارات", callback_data="country_ae"),
        ],
        [
            InlineKeyboardButton("🇸🇦 السعودية", callback_data="country_sa"),
            InlineKeyboardButton("🇧🇭 البحرين", callback_data="country_bh"),
        ],
        [InlineKeyboardButton("🌍 جميع الدول", callback_data="country_all")],
    ]
    await update.message.reply_text(
        "🔍 <b>اختر الدولة للبحث عن وظائف:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 <b>دليل استخدام بوت LinkedIt:</b>\n\n"
        "1️⃣ اضغط على /start للبدء.\n"
        "2️⃣ اختر <b>بحث عن وظيفة</b> ثم اختر الدولة.\n"
        "3️⃣ اكتب المسمى الوظيفي (مثلاً: Accountant أو مهندس).\n"
        "4️⃣ سيقوم البوت بالبحث في Indeed, LinkedIn, Glassdoor, Google.\n\n"
        "<b>الميزات الجديدة:</b>\n"
        "⭐ <b>حفظ الوظائف</b> - اضغط زر ⭐ حفظ لحفظ أي وظيفة.\n"
        "🔔 <b>التنبيهات</b> - أضف تنبيه وسنرسل لك الوظائف الجديدة تلقائياً.\n"
        "👤 <b>ملفك الشخصي</b> - احفظ تفضيلاتك للبحث السريع.\n\n"
        "💡 <i>نصيحة: البحث بالإنجليزية يعطي نتائج أكثر وأدق.</i>\n"
    )
    if CHANNEL_LINK:
        help_text += f"\n📢 <a href='{CHANNEL_LINK}'>انضم لقناة الوظائف</a>"
    if BOT_LINK:
        help_text += f"\n🤖 <a href='{BOT_LINK}'>شارك البوت مع أصدقائك</a>"
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


# ========================
# Callback Handler
# ========================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    try:
        await query.answer()
    except BadRequest:
        return

    data = query.data
    user_id = query.from_user.id

    # Ensure user exists in DB
    db.get_or_create_user(user_id, query.from_user.username or "", query.from_user.first_name or "")

    if data == "noop":
        return

    # --- Main Menu ---
    if data == "search":
        keyboard = [
            [
                InlineKeyboardButton("🇶🇦 قطر", callback_data="country_qa"),
                InlineKeyboardButton("🇦🇪 الإمارات", callback_data="country_ae"),
            ],
            [
                InlineKeyboardButton("🇸🇦 السعودية", callback_data="country_sa"),
                InlineKeyboardButton("🇧🇭 البحرين", callback_data="country_bh"),
            ],
            [InlineKeyboardButton("🌍 جميع الدول", callback_data="country_all")],
            [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")],
        ]
        await query.edit_message_text(
            "🔍 <b>اختر الدولة للبحث:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data == "categories":
        keyboard = [[InlineKeyboardButton(c["name"], callback_data=f"cat_{k}")] for k, c in JOB_CATEGORIES.items()]
        keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])
        await query.edit_message_text(
            "📂 <b>اختر تصنيف الوظائف:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data.startswith("country_"):
        country_code = data.replace("country_", "")
        context.user_data["country"] = country_code
        await query.edit_message_text(
            "✍️ <b>أرسل الآن المسمى الوظيفي الذي تبحث عنه:</b>\n(مثال: مهندس، محاسبة، Sales، Developer)",
            parse_mode=ParseMode.HTML,
        )

    elif data.startswith("cat_"):
        cat_id = data.replace("cat_", "")
        search_term = JOB_CATEGORIES[cat_id]["query"]
        await perform_search(query, context, search_term, "all", is_callback=True)

    elif data == "back_main":
        await query.edit_message_text(
            "👋 أهلاً بك في بوت <b>LinkedIt By Abdulrahman</b>\n\nاختر من القائمة أدناه للبدء:",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_main_menu_keyboard(),
        )

    # --- Save Job ---
    elif data.startswith("savejob_"):
        job_id = data.replace("savejob_", "")
        # Find job in user's current results
        job_to_save = None
        for key, val in context.user_data.items():
            if key.startswith("results_") and isinstance(val, list):
                for job in val:
                    if _generate_job_id(job) == job_id:
                        job_to_save = job
                        break
            if job_to_save:
                break

        if job_to_save:
            if db.count_favorites(user_id) >= MAX_FAVORITES:
                await query.answer("⚠️ وصلت للحد الأقصى من المفضلة (50). احذف بعض الوظائف أولاً.", show_alert=True)
            elif db.save_favorite(user_id, job_to_save):
                await query.answer("⭐ تم حفظ الوظيفة في المفضلة!", show_alert=True)
            else:
                await query.answer("ℹ️ هذه الوظيفة محفوظة مسبقاً.", show_alert=True)
        else:
            await query.answer("⚠️ لم أتمكن من حفظ الوظيفة. حاول البحث مرة أخرى.", show_alert=True)

    # --- Favorites ---
    elif data == "my_favorites":
        await show_favorites(query, user_id)

    elif data.startswith("delfav_"):
        fav_id = int(data.replace("delfav_", ""))
        if db.remove_favorite(user_id, fav_id):
            await query.answer("🗑️ تم حذف الوظيفة من المفضلة.")
            await show_favorites(query, user_id)
        else:
            await query.answer("⚠️ لم أتمكن من حذف الوظيفة.")

    elif data.startswith("viewfav_"):
        fav_id = int(data.replace("viewfav_", ""))
        await show_favorite_detail(query, user_id, fav_id)

    # --- Alerts ---
    elif data == "my_alerts":
        await show_alerts(query, user_id)

    elif data == "add_alert":
        if db.count_alerts(user_id) >= MAX_ALERTS:
            await query.answer(f"⚠️ وصلت للحد الأقصى ({MAX_ALERTS} تنبيهات). احذف تنبيهاً أولاً.", show_alert=True)
        else:
            context.user_data["awaiting_alert_keyword"] = True
            await query.edit_message_text(
                "🔔 <b>إضافة تنبيه جديد</b>\n\n"
                "أرسل الكلمة المفتاحية التي تريد تلقي تنبيهات عنها:\n"
                "(مثال: accountant, مهندس, developer, sales)",
                parse_mode=ParseMode.HTML,
            )

    elif data.startswith("delalert_"):
        alert_id = int(data.replace("delalert_", ""))
        if db.remove_alert(user_id, alert_id):
            await query.answer("🗑️ تم حذف التنبيه.")
            await show_alerts(query, user_id)
        else:
            await query.answer("⚠️ لم أتمكن من حذف التنبيه.")

    elif data.startswith("alertcountry_"):
        country_code = data.replace("alertcountry_", "")
        keyword = context.user_data.get("alert_keyword", "")
        if keyword:
            alert_id = db.add_alert(user_id, keyword, country_code)
            if alert_id == -1:
                await query.edit_message_text(
                    "ℹ️ هذا التنبيه موجود مسبقاً.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع", callback_data="my_alerts")]]),
                )
            else:
                country_name = "جميع الدول" if country_code == "all" else COUNTRIES.get(country_code, {}).get("name", country_code)
                await query.edit_message_text(
                    f"✅ <b>تم إضافة التنبيه بنجاح!</b>\n\n"
                    f"🔑 الكلمة المفتاحية: <b>{escape_html(keyword)}</b>\n"
                    f"🌍 الدولة: <b>{country_name}</b>\n\n"
                    "سيتم إرسال الوظائف الجديدة لك تلقائياً.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔔 تنبيهاتي", callback_data="my_alerts")],
                        [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")],
                    ]),
                )
            context.user_data.pop("alert_keyword", None)

    # --- Profile ---
    elif data == "my_profile":
        await show_profile(query, user_id)

    elif data == "set_pref_countries":
        prefs = db.get_user_preferences(user_id)
        current = prefs.get("preferred_countries", [])
        keyboard = []
        for code, info in COUNTRIES.items():
            check = "✅" if code in current else "⬜"
            keyboard.append([InlineKeyboardButton(f"{check} {info['name']}", callback_data=f"togglecountry_{code}")])
        keyboard.append([InlineKeyboardButton("💾 حفظ", callback_data="my_profile")])
        await query.edit_message_text(
            "🌍 <b>اختر الدول المفضلة:</b>\n(اضغط لتفعيل/إلغاء)",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data.startswith("togglecountry_"):
        cc = data.replace("togglecountry_", "")
        prefs = db.get_user_preferences(user_id)
        current = prefs.get("preferred_countries", [])
        if cc in current:
            current.remove(cc)
        else:
            current.append(cc)
        db.update_user_preferences(user_id, countries=current)
        # Refresh the country selection
        keyboard = []
        for code, info in COUNTRIES.items():
            check = "✅" if code in current else "⬜"
            keyboard.append([InlineKeyboardButton(f"{check} {info['name']}", callback_data=f"togglecountry_{code}")])
        keyboard.append([InlineKeyboardButton("💾 حفظ", callback_data="my_profile")])
        await query.edit_message_text(
            "🌍 <b>اختر الدول المفضلة:</b>\n(اضغط لتفعيل/إلغاء)",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data == "set_pref_keywords":
        context.user_data["awaiting_pref_keywords"] = True
        prefs = db.get_user_preferences(user_id)
        current = prefs.get("preferred_keywords", [])
        current_text = "، ".join(current) if current else "لا يوجد"
        await query.edit_message_text(
            f"🔑 <b>الكلمات المفتاحية المفضلة</b>\n\n"
            f"الحالية: <b>{escape_html(current_text)}</b>\n\n"
            "أرسل كلماتك المفتاحية مفصولة بفاصلة:\n"
            "(مثال: accountant, developer, مهندس)",
            parse_mode=ParseMode.HTML,
        )

    elif data == "quick_search":
        prefs = db.get_user_preferences(user_id)
        keywords = prefs.get("preferred_keywords", [])
        countries = prefs.get("preferred_countries", [])
        if not keywords:
            await query.answer("⚠️ أضف كلمات مفتاحية في ملفك الشخصي أولاً.", show_alert=True)
            return
        country_code = countries[0] if len(countries) == 1 else "all"
        search_term = " ".join(keywords[:3])
        await perform_search(query, context, search_term, country_code, is_callback=True)

    # --- Pagination ---
    elif data.startswith("page_"):
        parts = data.split("_")
        search_id = parts[1]
        page = int(parts[2])
        results = context.user_data.get(f"results_{search_id}", [])
        if results:
            await send_page(query.message.chat_id, context, results, page, search_id)


# ========================
# Favorites Display
# ========================

async def show_favorites(query, user_id: int):
    """Show user's saved favorites."""
    favs = db.get_favorites(user_id)
    if not favs:
        await query.edit_message_text(
            "⭐ <b>المفضلة فارغة</b>\n\n"
            "لم تحفظ أي وظائف بعد.\n"
            "ابحث عن وظيفة واضغط زر ⭐ حفظ لإضافتها هنا.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")],
                [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")],
            ]),
        )
        return

    text = f"⭐ <b>الوظائف المحفوظة ({len(favs)}):</b>\n\n"
    keyboard = []
    for fav in favs[:10]:  # Show max 10
        title = fav.get("job_title", "وظيفة")[:40]
        company = fav.get("company", "")[:20]
        label = f"💼 {title}"
        if company and company not in ("nan", "None", ""):
            label += f" - {company}"
        keyboard.append([
            InlineKeyboardButton(label[:60], callback_data=f"viewfav_{fav['id']}"),
            InlineKeyboardButton("🗑️", callback_data=f"delfav_{fav['id']}"),
        ])

    keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def show_favorite_detail(query, user_id: int, fav_id: int):
    """Show details of a saved favorite job."""
    favs = db.get_favorites(user_id)
    fav = next((f for f in favs if f["id"] == fav_id), None)
    if not fav:
        await query.answer("⚠️ الوظيفة غير موجودة.")
        return

    title = escape_html(fav.get("job_title", "غير محدد"))
    company = escape_html(fav.get("company", "غير محدد"))
    if company in ("nan", "None", ""):
        company = "غير محدد"
    location = escape_html(fav.get("location", ""))
    country = escape_html(fav.get("country_name", ""))
    job_url = fav.get("job_url", "")
    email = fav.get("email", "")
    desc = escape_html(fav.get("description", "")[:300])
    saved_at = fav.get("saved_at", "")

    text = "━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"⭐ <b>{title}</b>\n"
    text += f"🏢 {company}\n"
    if location and location not in ("nan", "None"):
        text += f"📍 {location}\n"
    if country:
        text += f"🌍 {country}\n"
    if desc and desc not in ("nan", "None"):
        text += f"\n{desc}\n"
    if email and email not in ("nan", "None"):
        text += f"\n📧 {email}\n"
    if job_url and job_url not in ("nan", "None"):
        text += f"\n🔗 <a href='{job_url}'>رابط التقديم</a>\n"
    if saved_at:
        text += f"\n📅 تم الحفظ: {saved_at[:10]}\n"
    text += "━━━━━━━━━━━━━━━━━━━━━"

    keyboard = []
    if job_url and job_url not in ("nan", "None"):
        keyboard.append([InlineKeyboardButton("🔗 فتح الرابط", url=job_url)])
    keyboard.append([
        InlineKeyboardButton("🗑️ حذف", callback_data=f"delfav_{fav_id}"),
        InlineKeyboardButton("🔙 رجوع", callback_data="my_favorites"),
    ])

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True,
    )


# ========================
# Alerts Display
# ========================

async def show_alerts(query, user_id: int):
    """Show user's job alerts."""
    alerts = db.get_user_alerts(user_id)

    text = "🔔 <b>تنبيهات الوظائف</b>\n\n"
    if not alerts:
        text += "لا يوجد تنبيهات نشطة.\nأضف تنبيهاً لتصلك الوظائف الجديدة تلقائياً!"
    else:
        text += f"لديك <b>{len(alerts)}</b> تنبيه نشط:\n\n"
        for alert in alerts:
            country = "جميع الدول" if alert["country_code"] == "all" else COUNTRIES.get(alert["country_code"], {}).get("name", alert["country_code"])
            text += f"🔑 <b>{escape_html(alert['keyword'])}</b> - {country}\n"

    keyboard = []
    for alert in alerts:
        keyboard.append([
            InlineKeyboardButton(f"🔑 {alert['keyword']}", callback_data="noop"),
            InlineKeyboardButton("🗑️ حذف", callback_data=f"delalert_{alert['id']}"),
        ])

    if len(alerts) < MAX_ALERTS:
        keyboard.append([InlineKeyboardButton("➕ إضافة تنبيه جديد", callback_data="add_alert")])
    keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ========================
# Profile Display
# ========================

async def show_profile(query, user_id: int):
    """Show user profile and preferences."""
    prefs = db.get_user_preferences(user_id)
    countries = prefs.get("preferred_countries", [])
    keywords = prefs.get("preferred_keywords", [])
    alerts_on = prefs.get("alerts_enabled", False)
    fav_count = db.count_favorites(user_id)
    alert_count = db.count_alerts(user_id)

    countries_text = "، ".join([COUNTRIES[c]["name"] for c in countries if c in COUNTRIES]) if countries else "لم يتم تحديدها"
    keywords_text = "، ".join(keywords) if keywords else "لم يتم تحديدها"

    text = (
        "👤 <b>ملفك الشخصي</b>\n\n"
        f"🌍 <b>الدول المفضلة:</b> {countries_text}\n"
        f"🔑 <b>الكلمات المفتاحية:</b> {escape_html(keywords_text)}\n"
        f"⭐ <b>الوظائف المحفوظة:</b> {fav_count}\n"
        f"🔔 <b>التنبيهات النشطة:</b> {alert_count}\n"
    )

    keyboard = [
        [InlineKeyboardButton("🌍 تعديل الدول المفضلة", callback_data="set_pref_countries")],
        [InlineKeyboardButton("🔑 تعديل الكلمات المفتاحية", callback_data="set_pref_keywords")],
    ]
    if keywords:
        keyboard.append([InlineKeyboardButton("⚡ بحث سريع بتفضيلاتي", callback_data="quick_search")])
    keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ========================
# Message Handler
# ========================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    # Handle alert keyword input
    if context.user_data.get("awaiting_alert_keyword"):
        context.user_data["awaiting_alert_keyword"] = False
        context.user_data["alert_keyword"] = text

        keyboard = [
            [
                InlineKeyboardButton("🇶🇦 قطر", callback_data="alertcountry_qa"),
                InlineKeyboardButton("🇦🇪 الإمارات", callback_data="alertcountry_ae"),
            ],
            [
                InlineKeyboardButton("🇸🇦 السعودية", callback_data="alertcountry_sa"),
                InlineKeyboardButton("🇧🇭 البحرين", callback_data="alertcountry_bh"),
            ],
            [InlineKeyboardButton("🌍 جميع الدول", callback_data="alertcountry_all")],
        ]
        await update.message.reply_text(
            f"🔔 تنبيه جديد: <b>{escape_html(text)}</b>\n\n"
            "اختر الدولة لهذا التنبيه:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    # Handle preference keywords input
    if context.user_data.get("awaiting_pref_keywords"):
        context.user_data["awaiting_pref_keywords"] = False
        keywords = [k.strip() for k in text.split(",") if k.strip()][:10]
        db.update_user_preferences(user_id, keywords=keywords)
        await update.message.reply_text(
            f"✅ تم حفظ الكلمات المفتاحية: <b>{escape_html('، '.join(keywords))}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 ملفي الشخصي", callback_data="my_profile")],
                [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")],
            ]),
        )
        return

    # Default: treat as job search
    search_term = text
    country_code = context.user_data.get("country", "all")
    await perform_search(update, context, search_term, country_code)


# ========================
# Pagination
# ========================

async def send_page(chat_id, context, results, page, search_id):
    """Send one page of results with navigation buttons."""
    start_idx = page * RESULTS_PER_PAGE
    end_idx = min(start_idx + RESULTS_PER_PAGE, len(results))
    total_pages = (len(results) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE

    page_results = results[start_idx:end_idx]

    for job in page_results:
        c_name = job.get("_country_name", "الخليج")
        text, wa_url, buttons = format_job_message(job, c_name)
        markup = InlineKeyboardMarkup(buttons)
        try:
            await context.bot.send_message(
                chat_id,
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.error("Error sending job message: %s", e)

    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"page_{search_id}_{page - 1}"))
    nav_buttons.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
    if end_idx < len(results):
        nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"page_{search_id}_{page + 1}"))

    if total_pages > 1:
        await context.bot.send_message(
            chat_id,
            f"📊 عرض {start_idx + 1}-{end_idx} من {len(results)} وظيفة",
            reply_markup=InlineKeyboardMarkup([nav_buttons]),
        )


async def perform_search(update_or_query, context, search_term, country_code, is_callback=False):
    if is_callback:
        await update_or_query.edit_message_text(
            f"🔍 جاري البحث عن <b>{escape_html(search_term)}</b>... يرجى الانتظار.\n"
            f"🌐 المصادر: Indeed, LinkedIn, Glassdoor, Google",
            parse_mode=ParseMode.HTML,
        )
        chat_id = update_or_query.message.chat_id
    else:
        await update_or_query.message.reply_text(
            f"🔍 جاري البحث عن <b>{escape_html(search_term)}</b>... يرجى الانتظار.\n"
            f"🌐 المصادر: Indeed, LinkedIn, Glassdoor, Google",
            parse_mode=ParseMode.HTML,
        )
        chat_id = update_or_query.message.chat_id

    results = await search_jobs_logic(search_term, country_code)

    if not results:
        await context.bot.send_message(
            chat_id,
            f"😔 لم أجد وظائف حالياً لـ <b>{escape_html(search_term)}</b>. حاول مرة أخرى بمسمى مختلف.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Store results for pagination and save functionality
    search_id = str(abs(hash(f"{search_term}:{country_code}:{datetime.now().timestamp()}")))[-8:]
    context.user_data[f"results_{search_id}"] = results[:MAX_RESULTS]

    await context.bot.send_message(
        chat_id,
        f"✅ تم العثور على <b>{len(results[:MAX_RESULTS])}</b> وظيفة:",
        parse_mode=ParseMode.HTML,
    )

    await send_page(chat_id, context, results[:MAX_RESULTS], 0, search_id)


# ========================
# Job Alerts Scheduler
# ========================

async def check_and_send_alerts(app_context):
    """Periodic task to check alerts and send new jobs to users."""
    logger.info("Running alert check...")
    alerts = db.get_all_active_alerts()
    if not alerts:
        logger.info("No active alerts found.")
        return

    for alert in alerts:
        try:
            user_id = alert["user_id"]
            keyword = alert["keyword"]
            country_code = alert["country_code"]

            results = await search_jobs_logic(keyword, country_code)
            if not results:
                continue

            new_jobs = []
            for job in results[:5]:
                job_url = str(job.get("job_url", ""))
                if job_url and not db.is_job_sent(user_id, job_url):
                    new_jobs.append(job)
                    db.mark_job_sent(user_id, job_url)

            if not new_jobs:
                continue

            # Send notification header
            country_name = "جميع الدول" if country_code == "all" else COUNTRIES.get(country_code, {}).get("name", country_code)
            await app_context.bot.send_message(
                user_id,
                f"🔔 <b>تنبيه وظائف جديدة!</b>\n\n"
                f"🔑 الكلمة: <b>{escape_html(keyword)}</b>\n"
                f"🌍 الدولة: {country_name}\n"
                f"📊 عدد الوظائف الجديدة: {len(new_jobs)}",
                parse_mode=ParseMode.HTML,
            )

            # Send each new job
            for job in new_jobs:
                c_name = job.get("_country_name", "الخليج")
                text, wa_url, buttons = format_job_message(job, c_name, show_save_btn=False)
                markup = InlineKeyboardMarkup([[InlineKeyboardButton("📤 واتساب", url=wa_url)]])
                try:
                    await app_context.bot.send_message(
                        user_id,
                        text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=markup,
                        disable_web_page_preview=True,
                    )
                    await asyncio.sleep(0.5)
                except TelegramError as e:
                    logger.error("Error sending alert to %s: %s", user_id, e)
                    if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                        db.remove_alert(user_id, alert["id"])
                        break

            db.update_alert_sent(alert["id"])

        except Exception as e:
            logger.error("Error processing alert %s: %s", alert.get("id"), e)

    logger.info("Alert check completed.")


# ========================
# Error Handler
# ========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception while handling an update:", exc_info=context.error)


# ========================
# Main
# ========================

def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing. Please set BOT_TOKEN in Render Environment Variables.")
        raise SystemExit(1)

    # Initialize database
    db.init_db()

    # Start Flask in a separate thread (health endpoint)
    Thread(target=run_flask, daemon=True).start()

    application = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Error handler
    application.add_error_handler(error_handler)

    # Schedule alert checks
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(
            check_and_send_alerts,
            interval=ALERT_INTERVAL,
            first=60,  # First check after 1 minute
            name="alert_checker",
        )
        logger.info("Alert scheduler started (interval: %s seconds)", ALERT_INTERVAL)
    else:
        logger.warning("Job queue not available. Alerts will not be sent automatically.")

    logger.info("Bot started (Phase 1 - Full Features)...")

    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
