"""
LinkedIt Bot v3.0 - Gulf Job Search Telegram Bot (Background Worker)
=================================================
Search for jobs across Gulf countries (Qatar, UAE, Saudi Arabia, Bahrain)
using Indeed and LinkedIn as data sources.

Key improvements over v2:
- All Telegram API calls wrapped in safe helpers (no unhandled errors)
- Input validation for all user inputs
- Proper error boundaries around every handler
- Clean logging (no token leaks, no noisy HTTP logs)
- Robust pagination and callback handling
- Admin dashboard with analytics
"""

import os
import re
import asyncio
import hashlib
import logging
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from html import escape as html_escape
from urllib.parse import quote_plus

from cachetools import TTLCache
from jobspy import scrape_jobs
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import database as db

# ========================
# Logging Configuration
# ========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("LinkedIt")

# Suppress noisy HTTP logs (prevents token leaking in logs)
for noisy_logger in ("httpx", "httpcore", "urllib3", "telegram.ext.Updater"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

# ========================
# Configuration from Environment
# ========================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
BOT_LINK = os.environ.get("BOT_LINK", "")
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "")
WHATSAPP_LINK = os.environ.get("WHATSAPP_LINK", "")
ALERT_INTERVAL = int(os.environ.get("ALERT_INTERVAL", "21600"))  # 6 hours

# ========================
# Constants
# ========================
MAX_RESULTS = 15
RESULTS_PER_PAGE = 5
SEARCH_TIMEOUT = 90
HOURS_OLD = 72
MAX_FAVORITES = 50
MAX_ALERTS = 5
CACHE_TTL = 1800  # 30 minutes
SEARCH_SITES = ["indeed", "linkedin"]

# ========================
# Countries Configuration
# ========================
COUNTRIES = {
    "qa": {"name": "🇶🇦 قطر", "location": "Qatar", "indeed_country": "qatar"},
    "ae": {"name": "🇦🇪 الإمارات", "location": "United Arab Emirates", "indeed_country": "united arab emirates"},
    "sa": {"name": "🇸🇦 السعودية", "location": "Saudi Arabia", "indeed_country": "saudi arabia"},
    "bh": {"name": "🇧🇭 البحرين", "location": "Bahrain", "indeed_country": "bahrain"},
}

# ========================
# Job Categories
# ========================
JOB_CATEGORIES = {
    "accounting": {"name": "📊 محاسبة ومالية", "query": "Accountant OR Finance"},
    "engineering": {"name": "⚙️ هندسة", "query": "Engineer"},
    "it": {"name": "💻 تقنية المعلومات", "query": "Software Developer OR IT"},
    "medical": {"name": "🏥 طبي وصحي", "query": "Doctor OR Nurse OR Medical"},
    "sales": {"name": "📈 مبيعات وتسويق", "query": "Sales OR Marketing"},
    "admin": {"name": "🏢 إداري", "query": "Administrative OR Office Manager"},
    "education": {"name": "📚 تعليم", "query": "Teacher OR Education"},
    "hospitality": {"name": "🏨 ضيافة وسياحة", "query": "Hotel OR Restaurant OR Tourism"},
    "construction": {"name": "🏗️ بناء وتشييد", "query": "Construction OR Civil"},
    "hr": {"name": "👥 موارد بشرية", "query": "Human Resources OR HR"},
}

# ========================
# Input Validation
# ========================
GREETINGS = frozenset([
    "السلام عليكم", "مرحبا", "اهلا", "هلا", "صباح", "مساء",
    "شكرا", "الحمد", "بسم الله", "hi", "hello", "hey", "thanks",
    "good morning", "good evening", "شكراً", "مرحباً", "أهلاً",
])

COUNTRY_NAMES = frozenset([
    "قطر", "الامارات", "الإمارات", "السعودية", "البحرين",
    "qatar", "uae", "saudi", "bahrain", "saudi arabia",
])

# ========================
# Runtime Objects
# ========================
job_cache = TTLCache(maxsize=200, ttl=CACHE_TTL)
executor = ThreadPoolExecutor(max_workers=4)


# ========================
# Safe Telegram Helpers
# ========================

def escape_html(text: str) -> str:
    """Safely escape HTML characters."""
    if not text:
        return ""
    return html_escape(str(text))


async def safe_edit_message(query, text: str, **kwargs):
    """Safely edit a message, ignoring 'message not modified' and other errors."""
    try:
        return await query.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        error_msg = str(e).lower()
        if "message is not modified" in error_msg:
            pass  # User clicked same button twice - ignore silently
        elif "message to edit not found" in error_msg:
            pass  # Message was deleted - ignore silently
        elif "query is too old" in error_msg:
            pass  # Callback expired - ignore silently
        else:
            logger.warning("safe_edit_message BadRequest: %s", e)
    except TelegramError as e:
        logger.warning("safe_edit_message TelegramError: %s", e)
    except Exception as e:
        logger.error("safe_edit_message unexpected error: %s", e)
    return None


async def safe_answer_callback(query, text: str = "", show_alert: bool = False):
    """Safely answer a callback query, ignoring expiration errors."""
    try:
        await query.answer(text=text, show_alert=show_alert)
    except (BadRequest, TelegramError):
        pass  # Callback expired or already answered


async def safe_send_message(bot, chat_id: int, text: str, **kwargs):
    """Safely send a message, handling all errors."""
    try:
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except TelegramError as e:
        logger.warning("safe_send_message error to %s: %s", chat_id, e)
    except Exception as e:
        logger.error("safe_send_message unexpected error: %s", e)
    return None


# ========================
# Input Validation
# ========================

def validate_search_input(text: str) -> tuple[bool, str]:
    """
    Validate user search input. Returns (is_valid, error_message).
    """
    if not text or not text.strip():
        return False, "الرجاء إدخال نص البحث."

    text = text.strip()

    # Too short or too long
    if len(text) < 2:
        return False, "نص البحث قصير جداً. أدخل مسمى وظيفي (مثال: Accountant)"
    if len(text) > 60:
        return False, "نص البحث طويل جداً. حاول بكلمات أقل."

    # Only emojis/symbols (no alphanumeric characters)
    if not any(c.isalnum() for c in text):
        return False, ""

    # Greetings
    text_lower = text.lower().strip()
    if any(text_lower.startswith(g) for g in GREETINGS):
        return False, ""

    # Country names only
    if text_lower in COUNTRY_NAMES:
        return False, "هذا اسم دولة وليس مسمى وظيفي. اضغط /start واختر الدولة ثم أدخل المسمى الوظيفي."

    return True, ""


# ========================
# Job Formatting Helpers
# ========================

def _safe_value(val, default: str = "غير محدد") -> str:
    """Get safe string value from job data."""
    if val is None:
        return default
    s = str(val).strip()
    if s.lower() in ("nan", "none", ""):
        return default
    return s


def _extract_job_email(job: dict) -> str:
    """Extract email from job description if available."""
    desc = str(job.get("description", ""))
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", desc)
    return emails[0] if emails else ""


def _generate_job_id(job: dict) -> str:
    """Generate a short unique ID for a job."""
    url = str(job.get("job_url", ""))
    title = str(job.get("title", ""))
    return hashlib.md5(f"{url}{title}".encode()).hexdigest()[:10]


def format_job_message(job: dict, country_name: str, show_save_btn: bool = True) -> tuple:
    """Format a job into a Telegram message with buttons."""
    title = escape_html(_safe_value(job.get("title", ""), "وظيفة"))
    company = escape_html(_safe_value(job.get("company", ""), "غير محدد"))
    location = escape_html(_safe_value(job.get("location", ""), ""))
    source = escape_html(_safe_value(job.get("site", ""), ""))
    job_url = _safe_value(job.get("job_url", ""), "")
    email = job.get("_email", "")
    desc = _safe_value(job.get("description", ""), "")

    # Build message text
    text = "━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"💼 <b>{title}</b>\n"
    text += f"🏢 {company}\n"
    if location and location != "غير محدد":
        text += f"📍 {escape_html(location)}\n"
    text += f"🌍 {escape_html(country_name)}\n"
    if source:
        text += f"🔗 المصدر: {source}\n"
    if desc and desc != "غير محدد":
        short_desc = escape_html(desc[:200])
        text += f"\n{short_desc}...\n"
    if email:
        text += f"\n📧 {escape_html(email)}\n"

    # Promo links
    promo_parts = []
    if CHANNEL_LINK:
        promo_parts.append(f"📢 <a href='{CHANNEL_LINK}'>قناة الوظائف</a>")
    if BOT_LINK:
        promo_parts.append(f"🤖 <a href='{BOT_LINK}'>شارك البوت</a>")
    if promo_parts:
        text += "\n" + " | ".join(promo_parts) + "\n"
    text += "━━━━━━━━━━━━━━━━━━━━━"

    # WhatsApp share URL
    wa_text = f"وظيفة: {_safe_value(job.get('title', ''), 'وظيفة')} في {_safe_value(job.get('company', ''), 'شركة')}"
    if job_url:
        wa_text += f"\nرابط التقديم: {job_url}"
    if BOT_LINK:
        wa_text += f"\n\nابحث عن المزيد: {BOT_LINK}"
    wa_url = f"https://wa.me/?text={quote_plus(wa_text)}"

    # Buttons
    buttons = []
    row1 = []
    if job_url:
        row1.append(InlineKeyboardButton("🔗 التقديم", url=job_url))
    row1.append(InlineKeyboardButton("📤 واتساب", url=wa_url))
    buttons.append(row1)

    if show_save_btn:
        job_id = _generate_job_id(job)
        buttons.append([InlineKeyboardButton("⭐ حفظ في المفضلة", callback_data=f"savejob_{job_id}")])

    return text, wa_url, buttons


def _build_promo_keyboard_rows() -> list:
    """Build promotional keyboard rows for main menu."""
    rows = []
    promo_row = []
    if BOT_LINK:
        promo_row.append(InlineKeyboardButton("🤖 شارك البوت", url=BOT_LINK))
    if CHANNEL_LINK:
        promo_row.append(InlineKeyboardButton("📢 قناة الوظائف", url=CHANNEL_LINK))
    if promo_row:
        rows.append(promo_row)
    if WHATSAPP_LINK:
        rows.append([InlineKeyboardButton("📱 واتساب", url=WHATSAPP_LINK)])
    return rows


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


def _build_country_keyboard(prefix: str = "country") -> list:
    """Build country selection keyboard rows."""
    return [
        [
            InlineKeyboardButton("🇶🇦 قطر", callback_data=f"{prefix}_qa"),
            InlineKeyboardButton("🇦🇪 الإمارات", callback_data=f"{prefix}_ae"),
        ],
        [
            InlineKeyboardButton("🇸🇦 السعودية", callback_data=f"{prefix}_sa"),
            InlineKeyboardButton("🇧🇭 البحرين", callback_data=f"{prefix}_bh"),
        ],
        [InlineKeyboardButton("🌍 جميع الدول", callback_data=f"{prefix}_all")],
    ]


# ========================
# Job Search Engine
# ========================

def _search_single_country(search_term: str, cc: str) -> list:
    """Scrape jobs for a single country (runs in thread pool)."""
    # Random delay to reduce rate limiting
    time.sleep(random.uniform(0.5, 2.0))
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
        logger.error("Search error in %s: %s", cc, e)
    return []


async def search_jobs_logic(search_term: str, country_code: str) -> list:
    """Search with caching and concurrent country scraping."""
    cache_key = f"{search_term.lower().strip()}:{country_code}"

    if cache_key in job_cache:
        logger.info("Cache hit: %s", cache_key)
        return job_cache[cache_key]

    logger.info("Searching: %s", cache_key)
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
            logger.warning("Search timed out: %s", search_term)
            results_lists = []

        all_jobs = []
        for result in results_lists:
            if isinstance(result, list):
                all_jobs.extend(result)
            elif isinstance(result, Exception):
                logger.error("Search task error: %s", result)
    else:
        try:
            all_jobs = await asyncio.wait_for(
                loop.run_in_executor(executor, _search_single_country, search_term, country_code),
                timeout=SEARCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("Search timed out: %s in %s", search_term, country_code)
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
# Bot Handlers - Commands
# ========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    try:
        user = update.effective_user
        db.get_or_create_user(user.id, user.username or "", user.first_name or "")

        await update.message.reply_text(
            f"👋 أهلاً بك <b>{escape_html(user.first_name or 'صديقي')}</b> في بوت <b>LinkedIt</b>\n\n"
            "أنا أساعدك في العثور على أحدث الوظائف في دول الخليج "
            "(قطر، الإمارات، السعودية، البحرين).\n\n"
            "🔍 <b>بحث</b> - ابحث عن وظيفة بالاسم أو التصنيف\n"
            "⭐ <b>المفضلة</b> - الوظائف التي حفظتها\n"
            "🔔 <b>التنبيهات</b> - إشعارات بالوظائف الجديدة\n"
            "👤 <b>ملفي</b> - إدارة تفضيلاتك\n\n"
            "اختر من القائمة أدناه للبدء:",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_main_menu_keyboard(),
        )
    except Exception as e:
        logger.error("Error in start: %s", e)


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /search command."""
    try:
        keyboard = _build_country_keyboard("country")
        await update.message.reply_text(
            "🔍 <b>اختر الدولة للبحث عن وظائف:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error("Error in search_command: %s", e)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    try:
        help_text = (
            "📖 <b>دليل استخدام بوت LinkedIt:</b>\n\n"
            "1️⃣ اضغط /start للبدء.\n"
            "2️⃣ اختر <b>بحث عن وظيفة</b> ثم اختر الدولة.\n"
            "3️⃣ اكتب المسمى الوظيفي (مثلاً: Accountant أو مهندس).\n"
            "4️⃣ سيقوم البوت بالبحث في Indeed و LinkedIn.\n\n"
            "<b>الميزات:</b>\n"
            "⭐ <b>حفظ الوظائف</b> - اضغط زر ⭐ حفظ لحفظ أي وظيفة.\n"
            "🔔 <b>التنبيهات</b> - أضف تنبيه وسنرسل لك الوظائف الجديدة تلقائياً.\n"
            "👤 <b>ملفك الشخصي</b> - احفظ تفضيلاتك للبحث السريع.\n\n"
            "💡 <i>نصيحة: البحث بالإنجليزية يعطي نتائج أكثر وأدق.</i>\n"
        )
        if CHANNEL_LINK:
            help_text += f"\n📢 <a href='{CHANNEL_LINK}'>انضم لقناة الوظائف</a>"
        if BOT_LINK:
            help_text += f"\n🤖 <a href='{BOT_LINK}'>شارك البوت مع أصدقائك</a>"

        await update.message.reply_text(
            help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
    except Exception as e:
        logger.error("Error in help_command: %s", e)


# ========================
# Pagination
# ========================

async def send_page(bot, chat_id: int, context, results: list, page: int, search_id: str):
    """Send one page of results with navigation buttons."""
    start_idx = page * RESULTS_PER_PAGE
    end_idx = min(start_idx + RESULTS_PER_PAGE, len(results))
    total_pages = (len(results) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE

    page_results = results[start_idx:end_idx]

    for job in page_results:
        c_name = job.get("_country_name", "الخليج")
        text, wa_url, buttons = format_job_message(job, c_name)
        await safe_send_message(
            bot, chat_id, text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True,
        )
        await asyncio.sleep(0.3)

    # Navigation buttons
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"page_{search_id}_{page - 1}"))
        nav_buttons.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
        if end_idx < len(results):
            nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"page_{search_id}_{page + 1}"))

        await safe_send_message(
            bot, chat_id,
            f"📊 عرض {start_idx + 1}-{end_idx} من {len(results)} وظيفة",
            reply_markup=InlineKeyboardMarkup([nav_buttons]),
        )


# ========================
# Perform Search
# ========================

async def perform_search(update_or_query, context, search_term: str, country_code: str, is_callback: bool = False):
    """Execute job search and send results."""
    try:
        # Send "searching" message
        if is_callback:
            await safe_edit_message(
                update_or_query,
                f"🔍 جاري البحث عن <b>{escape_html(search_term)}</b>... يرجى الانتظار.\n"
                f"🌐 المصادر: Indeed, LinkedIn",
                parse_mode=ParseMode.HTML,
            )
            chat_id = update_or_query.message.chat_id
            user_id = update_or_query.from_user.id if update_or_query.from_user else 0
        else:
            await update_or_query.message.reply_text(
                f"🔍 جاري البحث عن <b>{escape_html(search_term)}</b>... يرجى الانتظار.\n"
                f"🌐 المصادر: Indeed, LinkedIn",
                parse_mode=ParseMode.HTML,
            )
            chat_id = update_or_query.message.chat_id
            user_id = update_or_query.effective_user.id if update_or_query.effective_user else 0

        # Execute search
        results = await search_jobs_logic(search_term, country_code)

        # Log search for analytics
        try:
            db.log_search(user_id, search_term, country_code, len(results) if results else 0)
        except Exception as e:
            logger.error("Error logging search: %s", e)

        if not results:
            await safe_send_message(
                context.bot, chat_id,
                f"😔 لم أجد وظائف حالياً لـ <b>{escape_html(search_term)}</b>.\n"
                "حاول مرة أخرى بمسمى مختلف أو بالإنجليزية.",
                parse_mode=ParseMode.HTML,
            )
            return

        # Store results for pagination and save functionality
        search_id = hashlib.md5(f"{search_term}:{country_code}:{time.time()}".encode()).hexdigest()[:8]
        trimmed_results = results[:MAX_RESULTS]
        context.user_data[f"results_{search_id}"] = trimmed_results

        await safe_send_message(
            context.bot, chat_id,
            f"✅ تم العثور على <b>{len(trimmed_results)}</b> وظيفة:",
            parse_mode=ParseMode.HTML,
        )

        await send_page(context.bot, chat_id, context, trimmed_results, 0, search_id)

    except Exception as e:
        logger.error("Error in perform_search: %s", e)


# ========================
# Display Helpers
# ========================

async def show_favorites(query, user_id: int):
    """Show user's saved favorites."""
    try:
        favs = db.get_favorites(user_id)
        if not favs:
            await safe_edit_message(
                query,
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
        for fav in favs[:10]:
            title = (fav.get("job_title", "وظيفة") or "وظيفة")[:40]
            company = (fav.get("company", "") or "")[:20]
            label = f"💼 {title}"
            if company and company not in ("nan", "None", ""):
                label += f" - {company}"
            keyboard.append([
                InlineKeyboardButton(label[:60], callback_data=f"viewfav_{fav['id']}"),
                InlineKeyboardButton("🗑️", callback_data=f"delfav_{fav['id']}"),
            ])
        keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])

        await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error("Error showing favorites: %s", e)


async def show_favorite_detail(query, user_id: int, fav_id: int):
    """Show details of a saved favorite job."""
    try:
        favs = db.get_favorites(user_id)
        fav = next((f for f in favs if f["id"] == fav_id), None)
        if not fav:
            await safe_answer_callback(query, "⚠️ الوظيفة غير موجودة.")
            return

        title = escape_html(_safe_value(fav.get("job_title", ""), "غير محدد"))
        company = escape_html(_safe_value(fav.get("company", ""), "غير محدد"))
        location = escape_html(_safe_value(fav.get("location", ""), ""))
        country = escape_html(_safe_value(fav.get("country_name", ""), ""))
        job_url = _safe_value(fav.get("job_url", ""), "")
        email = _safe_value(fav.get("email", ""), "")
        desc = escape_html(_safe_value(fav.get("description", ""), "")[:300])
        saved_at = fav.get("saved_at", "")

        text = "━━━━━━━━━━━━━━━━━━━━━\n"
        text += f"⭐ <b>{title}</b>\n"
        text += f"🏢 {company}\n"
        if location:
            text += f"📍 {location}\n"
        if country:
            text += f"🌍 {country}\n"
        if desc:
            text += f"\n{desc}\n"
        if email:
            text += f"\n📧 {email}\n"
        if job_url:
            text += f"\n🔗 <a href='{job_url}'>رابط التقديم</a>\n"
        if saved_at:
            text += f"\n📅 تم الحفظ: {saved_at[:10]}\n"
        text += "━━━━━━━━━━━━━━━━━━━━━"

        keyboard = []
        if job_url:
            keyboard.append([InlineKeyboardButton("🔗 فتح الرابط", url=job_url)])
        keyboard.append([
            InlineKeyboardButton("🗑️ حذف", callback_data=f"delfav_{fav_id}"),
            InlineKeyboardButton("🔙 رجوع", callback_data="my_favorites"),
        ])

        await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                reply_markup=InlineKeyboardMarkup(keyboard),
                                disable_web_page_preview=True)
    except Exception as e:
        logger.error("Error showing favorite detail: %s", e)


async def show_alerts(query, user_id: int):
    """Show user's job alerts."""
    try:
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

        await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error("Error showing alerts: %s", e)


async def show_profile(query, user_id: int):
    """Show user profile and preferences."""
    try:
        prefs = db.get_user_preferences(user_id)
        keywords = prefs.get("preferred_keywords", [])
        countries = prefs.get("preferred_countries", [])
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

        await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error("Error showing profile: %s", e)


# ========================
# Callback Handler
# ========================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all inline keyboard button presses."""
    query = update.callback_query
    if not query:
        return

    # Always answer callback to remove loading indicator
    await safe_answer_callback(query)

    data = query.data
    if not data:
        return

    user_id = query.from_user.id

    # Ensure user exists in DB
    db.get_or_create_user(user_id, query.from_user.username or "", query.from_user.first_name or "")

    try:
        if data == "noop":
            return

        # --- Admin Dashboard ---
        if data.startswith("admin_"):
            handled = await handle_admin_callback(query, data, user_id, context)
            if handled:
                return

        # --- Main Menu ---
        if data == "search":
            keyboard = _build_country_keyboard("country")
            keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])
            await safe_edit_message(query, "🔍 <b>اختر الدولة للبحث:</b>",
                                    parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup(keyboard))

        elif data == "categories":
            keyboard = [[InlineKeyboardButton(c["name"], callback_data=f"cat_{k}")] for k, c in JOB_CATEGORIES.items()]
            keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])
            await safe_edit_message(query, "📂 <b>اختر تصنيف الوظائف:</b>",
                                    parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup(keyboard))

        elif data.startswith("country_"):
            country_code = data.replace("country_", "")
            context.user_data["country"] = country_code
            await safe_edit_message(query,
                "✍️ <b>أرسل الآن المسمى الوظيفي الذي تبحث عنه:</b>\n"
                "(مثال: مهندس، محاسبة، Sales، Developer)",
                parse_mode=ParseMode.HTML)

        elif data.startswith("cat_"):
            cat_id = data.replace("cat_", "")
            if cat_id in JOB_CATEGORIES:
                search_term = JOB_CATEGORIES[cat_id]["query"]
                await perform_search(query, context, search_term, "all", is_callback=True)

        elif data == "back_main":
            await safe_edit_message(query,
                "👋 أهلاً بك في بوت <b>LinkedIt</b>\n\nاختر من القائمة أدناه للبدء:",
                parse_mode=ParseMode.HTML,
                reply_markup=_build_main_menu_keyboard())

        # --- Save Job ---
        elif data.startswith("savejob_"):
            job_id = data.replace("savejob_", "")
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
                    await safe_answer_callback(query, f"⚠️ وصلت للحد الأقصى ({MAX_FAVORITES}). احذف بعض الوظائف أولاً.", True)
                elif db.save_favorite(user_id, job_to_save):
                    await safe_answer_callback(query, "⭐ تم حفظ الوظيفة في المفضلة!", True)
                else:
                    await safe_answer_callback(query, "ℹ️ هذه الوظيفة محفوظة مسبقاً.", True)
            else:
                await safe_answer_callback(query, "⚠️ لم أتمكن من حفظ الوظيفة. حاول البحث مرة أخرى.", True)

        # --- Favorites ---
        elif data == "my_favorites":
            await show_favorites(query, user_id)

        elif data.startswith("delfav_"):
            fav_id = int(data.replace("delfav_", ""))
            if db.remove_favorite(user_id, fav_id):
                await safe_answer_callback(query, "🗑️ تم حذف الوظيفة من المفضلة.")
                await show_favorites(query, user_id)
            else:
                await safe_answer_callback(query, "⚠️ لم أتمكن من حذف الوظيفة.")

        elif data.startswith("viewfav_"):
            fav_id = int(data.replace("viewfav_", ""))
            await show_favorite_detail(query, user_id, fav_id)

        # --- Alerts ---
        elif data == "my_alerts":
            await show_alerts(query, user_id)

        elif data == "add_alert":
            if db.count_alerts(user_id) >= MAX_ALERTS:
                await safe_answer_callback(query, f"⚠️ وصلت للحد الأقصى ({MAX_ALERTS} تنبيهات). احذف تنبيهاً أولاً.", True)
            else:
                context.user_data["awaiting_alert_keyword"] = True
                await safe_edit_message(query,
                    "🔔 <b>إضافة تنبيه جديد</b>\n\n"
                    "أرسل الكلمة المفتاحية التي تريد تلقي تنبيهات عنها:\n"
                    "(مثال: accountant, مهندس, developer, sales)",
                    parse_mode=ParseMode.HTML)

        elif data.startswith("delalert_"):
            alert_id = int(data.replace("delalert_", ""))
            if db.remove_alert(user_id, alert_id):
                await safe_answer_callback(query, "🗑️ تم حذف التنبيه.")
                await show_alerts(query, user_id)
            else:
                await safe_answer_callback(query, "⚠️ لم أتمكن من حذف التنبيه.")

        elif data.startswith("alertcountry_"):
            country_code = data.replace("alertcountry_", "")
            keyword = context.user_data.get("alert_keyword", "")
            if keyword:
                alert_id = db.add_alert(user_id, keyword, country_code)
                if alert_id == -1:
                    await safe_edit_message(query,
                        "ℹ️ هذا التنبيه موجود مسبقاً.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع", callback_data="my_alerts")]]))
                else:
                    country_name = "جميع الدول" if country_code == "all" else COUNTRIES.get(country_code, {}).get("name", country_code)
                    await safe_edit_message(query,
                        f"✅ <b>تم إضافة التنبيه بنجاح!</b>\n\n"
                        f"🔑 الكلمة المفتاحية: <b>{escape_html(keyword)}</b>\n"
                        f"🌍 الدولة: <b>{country_name}</b>\n\n"
                        "سيتم إرسال الوظائف الجديدة لك تلقائياً.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔔 تنبيهاتي", callback_data="my_alerts")],
                            [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")],
                        ]))
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
            await safe_edit_message(query,
                "🌍 <b>اختر الدول المفضلة:</b>\n(اضغط لتفعيل/إلغاء)",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(keyboard))

        elif data.startswith("togglecountry_"):
            cc = data.replace("togglecountry_", "")
            prefs = db.get_user_preferences(user_id)
            current = prefs.get("preferred_countries", [])
            if cc in current:
                current.remove(cc)
            else:
                current.append(cc)
            db.update_user_preferences(user_id, countries=current)
            keyboard = []
            for code, info in COUNTRIES.items():
                check = "✅" if code in current else "⬜"
                keyboard.append([InlineKeyboardButton(f"{check} {info['name']}", callback_data=f"togglecountry_{code}")])
            keyboard.append([InlineKeyboardButton("💾 حفظ", callback_data="my_profile")])
            await safe_edit_message(query,
                "🌍 <b>اختر الدول المفضلة:</b>\n(اضغط لتفعيل/إلغاء)",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(keyboard))

        elif data == "set_pref_keywords":
            context.user_data["awaiting_pref_keywords"] = True
            prefs = db.get_user_preferences(user_id)
            current = prefs.get("preferred_keywords", [])
            current_text = "، ".join(current) if current else "لا يوجد"
            await safe_edit_message(query,
                f"🔑 <b>الكلمات المفتاحية المفضلة</b>\n\n"
                f"الحالية: <b>{escape_html(current_text)}</b>\n\n"
                "أرسل كلماتك المفتاحية مفصولة بفاصلة:\n"
                "(مثال: accountant, developer, مهندس)",
                parse_mode=ParseMode.HTML)

        elif data == "quick_search":
            prefs = db.get_user_preferences(user_id)
            keywords = prefs.get("preferred_keywords", [])
            countries = prefs.get("preferred_countries", [])
            if not keywords:
                await safe_answer_callback(query, "⚠️ أضف كلمات مفتاحية في ملفك الشخصي أولاً.", True)
                return
            country_code = countries[0] if len(countries) == 1 else "all"
            search_term = " ".join(keywords[:3])
            await perform_search(query, context, search_term, country_code, is_callback=True)

        # --- Pagination ---
        elif data.startswith("page_"):
            parts = data.split("_")
            if len(parts) >= 3:
                search_id = parts[1]
                page = int(parts[2])
                results = context.user_data.get(f"results_{search_id}", [])
                if results:
                    await send_page(context.bot, query.message.chat_id, context, results, page, search_id)

    except Exception as e:
        logger.error("Error in handle_callback (data=%s): %s", data, e)


# ========================
# Message Handler
# ========================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all text messages."""
    try:
        user_id = update.effective_user.id
        text = (update.message.text or "").strip()

        if not text:
            return

        # Ensure user exists
        db.get_or_create_user(user_id, update.effective_user.username or "", update.effective_user.first_name or "")

        # Handle alert keyword input
        if context.user_data.get("awaiting_alert_keyword"):
            context.user_data["awaiting_alert_keyword"] = False
            context.user_data["alert_keyword"] = text
            keyboard = _build_country_keyboard("alertcountry")
            await update.message.reply_text(
                f"🔔 تنبيه جديد: <b>{escape_html(text)}</b>\n\nاختر الدولة لهذا التنبيه:",
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

        # Handle broadcast message from admin
        if context.user_data.get("awaiting_broadcast") and _is_admin(user_id):
            if text == "/cancel":
                context.user_data.pop("awaiting_broadcast", None)
                context.user_data.pop("broadcast_message", None)
                await update.message.reply_text(
                    "❌ تم إلغاء الرسالة الجماعية.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لوحة التحكم", callback_data="admin_menu")]]),
                )
                return

            context.user_data["broadcast_message"] = text
            total_users = db.get_bot_stats()["total_users"]
            await update.message.reply_text(
                f"📢 <b>معاينة الرسالة الجماعية:</b>\n\n"
                f"{text}\n\n━━━━━━━━━━━━━━━━━━━━━\n"
                f"👥 سيتم الإرسال إلى: <b>{total_users}</b> مستخدم\n\nهل تريد الإرسال؟",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ إرسال", callback_data="admin_confirm_broadcast")],
                    [InlineKeyboardButton("❌ إلغاء", callback_data="admin_cancel_broadcast")],
                ]),
            )
            return

        # Default: treat as job search with validation
        is_valid, error_msg = validate_search_input(text)
        if not is_valid:
            guide_msg = error_msg if error_msg else (
                "👋 أهلاً بك! للبحث عن وظيفة، يرجى إرسال <b>المسمى الوظيفي</b> مباشرة.\n\n"
                "مثال: <code>Accountant</code> أو <code>مهندس</code> أو <code>Sales Manager</code>\n\n"
                "أو اضغط /start لعرض القائمة الرئيسية."
            )
            await update.message.reply_text(guide_msg, parse_mode=ParseMode.HTML)
            return

        country_code = context.user_data.get("country", "all")
        await perform_search(update, context, text, country_code)

    except Exception as e:
        logger.error("Error in handle_message: %s", e)


# ========================
# Job Alerts Scheduler
# ========================

async def check_and_send_alerts(app_context):
    """Periodic task to check alerts and send new jobs to users."""
    logger.info("Running alert check...")
    try:
        alerts = db.get_all_active_alerts()
        if not alerts:
            logger.info("No active alerts.")
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

                country_name = "جميع الدول" if country_code == "all" else COUNTRIES.get(country_code, {}).get("name", country_code)
                await safe_send_message(
                    app_context.bot, user_id,
                    f"🔔 <b>تنبيه وظائف جديدة!</b>\n\n"
                    f"🔑 الكلمة: <b>{escape_html(keyword)}</b>\n"
                    f"🌍 الدولة: {country_name}\n"
                    f"📊 عدد الوظائف الجديدة: {len(new_jobs)}",
                    parse_mode=ParseMode.HTML,
                )

                for job in new_jobs:
                    c_name = job.get("_country_name", "الخليج")
                    text, wa_url, buttons = format_job_message(job, c_name, show_save_btn=False)
                    result = await safe_send_message(
                        app_context.bot, user_id, text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📤 واتساب", url=wa_url)]]),
                        disable_web_page_preview=True,
                    )
                    if result is None:
                        # User blocked bot or deactivated - remove alert
                        db.remove_alert(user_id, alert["id"])
                        break
                    await asyncio.sleep(0.5)

                db.update_alert_sent(alert["id"])

            except Exception as e:
                logger.error("Error processing alert %s: %s", alert.get("id"), e)

    except Exception as e:
        logger.error("Error in alert check: %s", e)
    logger.info("Alert check completed.")


# ========================
# Admin Dashboard
# ========================

def _is_admin(user_id: int) -> bool:
    """Check if user is admin."""
    return ADMIN_ID != 0 and user_id == ADMIN_ID


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin dashboard - main menu."""
    try:
        if not _is_admin(update.effective_user.id):
            await update.message.reply_text("⛔ هذا الأمر متاح للمشرف فقط.")
            return

        keyboard = [
            [InlineKeyboardButton("📊 نظرة عامة", callback_data="admin_overview")],
            [InlineKeyboardButton("🔍 أكثر الوظائف بحثاً", callback_data="admin_top_searches")],
            [InlineKeyboardButton("🌍 الدول الأكثر طلباً", callback_data="admin_top_countries")],
            [InlineKeyboardButton("👥 المستخدمون الأنشط", callback_data="admin_active_users")],
            [InlineKeyboardButton("🆕 أحدث المستخدمين", callback_data="admin_recent_users")],
            [InlineKeyboardButton("📅 إحصائيات يومية", callback_data="admin_daily_stats")],
            [InlineKeyboardButton("⏰ توزيع البحث بالساعة", callback_data="admin_hourly")],
            [InlineKeyboardButton("❌ بحث بدون نتائج", callback_data="admin_zero_results")],
            [InlineKeyboardButton("📢 رسالة جماعية", callback_data="admin_broadcast")],
        ]
        await update.message.reply_text(
            "🛠️ <b>لوحة تحكم المشرف</b>\n\nاختر من القائمة:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error("Error in admin_command: %s", e)


def _build_admin_menu_keyboard() -> InlineKeyboardMarkup:
    """Build admin menu keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 نظرة عامة", callback_data="admin_overview")],
        [InlineKeyboardButton("🔍 أكثر الوظائف بحثاً", callback_data="admin_top_searches")],
        [InlineKeyboardButton("🌍 الدول الأكثر طلباً", callback_data="admin_top_countries")],
        [InlineKeyboardButton("👥 المستخدمون الأنشط", callback_data="admin_active_users")],
        [InlineKeyboardButton("🆕 أحدث المستخدمين", callback_data="admin_recent_users")],
        [InlineKeyboardButton("📅 إحصائيات يومية", callback_data="admin_daily_stats")],
        [InlineKeyboardButton("⏰ توزيع البحث بالساعة", callback_data="admin_hourly")],
        [InlineKeyboardButton("❌ بحث بدون نتائج", callback_data="admin_zero_results")],
        [InlineKeyboardButton("📢 رسالة جماعية", callback_data="admin_broadcast")],
    ])


async def handle_admin_callback(query, data: str, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle all admin dashboard callbacks. Returns True if handled."""
    if not _is_admin(user_id):
        await safe_answer_callback(query, "⛔ غير مصرح.", True)
        return True

    admin_back_btn = [InlineKeyboardButton("🔙 لوحة التحكم", callback_data="admin_menu")]

    try:
        if data == "admin_menu":
            await safe_edit_message(query,
                "🛠️ <b>لوحة تحكم المشرف</b>\n\nاختر من القائمة:",
                parse_mode=ParseMode.HTML,
                reply_markup=_build_admin_menu_keyboard())
            return True

        elif data == "admin_overview":
            stats = db.get_admin_overview()
            text = (
                "📊 <b>نظرة عامة على البوت</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                "<b>👥 المستخدمون:</b>\n"
                f"   الإجمالي: <b>{stats['total_users']}</b>\n"
                f"   اليوم: <b>{stats['users_today']}</b>\n"
                f"   هذا الأسبوع: <b>{stats['users_this_week']}</b>\n\n"
                "<b>🔍 عمليات البحث:</b>\n"
                f"   الإجمالي: <b>{stats['total_searches']}</b>\n"
                f"   اليوم: <b>{stats['searches_today']}</b>\n"
                f"   هذا الأسبوع: <b>{stats['searches_this_week']}</b>\n\n"
                "<b>📋 أخرى:</b>\n"
                f"   ⭐ المفضلة: <b>{stats['total_favorites']}</b>\n"
                f"   🔔 التنبيهات: <b>{stats['active_alerts']}</b>\n"
                f"   📨 وظائف مرسلة: <b>{stats['total_sent_jobs']}</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━"
            )
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_top_searches":
            top = db.get_top_searches(10)
            if not top:
                text = "🔍 <b>أكثر الوظائف بحثاً</b>\n\nلا توجد بيانات بعد."
            else:
                text = "🔍 <b>أكثر 10 وظائف بحثاً:</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                for i, s in enumerate(top, 1):
                    avg_res = int(s['avg_results']) if s['avg_results'] else 0
                    text += f"{i}. <b>{escape_html(s['search_term'])}</b>\n"
                    text += f"   🔢 {s['count']} مرة | 📊 متوسط: {avg_res}\n\n"
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_top_countries":
            top = db.get_top_countries(10)
            if not top:
                text = "🌍 <b>الدول الأكثر طلباً</b>\n\nلا توجد بيانات بعد."
            else:
                text = "🌍 <b>الدول الأكثر طلباً:</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                total = sum(c['count'] for c in top)
                for c in top:
                    cc = c['country_code']
                    name = "جميع الدول 🌍" if cc == "all" else COUNTRIES.get(cc, {}).get("name", cc)
                    pct = round((c['count'] / total) * 100) if total > 0 else 0
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    text += f"{name}\n{bar} {pct}% ({c['count']})\n\n"
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_active_users":
            users = db.get_active_users(10)
            if not users:
                text = "👥 <b>المستخدمون الأنشط</b>\n\nلا توجد بيانات بعد."
            else:
                text = "👥 <b>أنشط 10 مستخدمين:</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                for i, u in enumerate(users, 1):
                    name = u['first_name'] or u['username'] or str(u['user_id'])
                    text += f"{i}. <b>{escape_html(name)}</b>\n"
                    text += f"   🔍 {u['search_count']} بحث | ⭐ {u['fav_count']} مفضلة | 🔔 {u['alert_count']} تنبيه\n\n"
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_recent_users":
            users = db.get_recent_users(10)
            if not users:
                text = "🆕 <b>أحدث المستخدمين</b>\n\nلا توجد مستخدمين بعد."
            else:
                text = "🆕 <b>آخر 10 مستخدمين:</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                for i, u in enumerate(users, 1):
                    name = u['first_name'] or u['username'] or str(u['user_id'])
                    date = u['created_at'][:16] if u['created_at'] else 'غير معروف'
                    text += f"{i}. <b>{escape_html(name)}</b>\n   📅 {date}\n\n"
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_daily_stats":
            days = db.get_daily_stats_history(7)
            if not days:
                text = "📅 <b>إحصائيات يومية</b>\n\nلا توجد بيانات بعد."
            else:
                text = "📅 <b>إحصائيات آخر 7 أيام:</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                for d in days:
                    text += (
                        f"📆 <b>{d['date']}</b>\n"
                        f"   👤 جدد: {d['new_users']} | 🔍 بحث: {d['total_searches']}\n"
                        f"   ⭐ مفضلة: {d['total_favorites']} | 📨 تنبيهات: {d['total_alerts_sent']}\n\n"
                    )
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_hourly":
            hours = db.get_hourly_search_distribution()
            if not hours:
                text = "⏰ <b>توزيع البحث بالساعة</b>\n\nلا توجد بيانات بعد."
            else:
                text = "⏰ <b>توزيع البحث حسب الساعة (UTC):</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                max_count = max(h['count'] for h in hours) if hours else 1
                for h in hours:
                    bar_len = int((h['count'] / max_count) * 15)
                    bar = "█" * bar_len + "░" * (15 - bar_len)
                    text += f"{h['hour']}:00 {bar} {h['count']}\n"
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_zero_results":
            zeros = db.get_zero_result_searches(10)
            if not zeros:
                text = "❌ <b>بحث بدون نتائج</b>\n\nجميع عمليات البحث أعطت نتائج!"
            else:
                text = "❌ <b>أكثر 10 عمليات بحث بدون نتائج:</b>\n━━━━━━━━━━━━━━━━━━━━━\n\n"
                for i, z in enumerate(zeros, 1):
                    cc = z['country_code']
                    country = "جميع الدول" if cc == "all" else COUNTRIES.get(cc, {}).get("name", cc)
                    text += f"{i}. <b>{escape_html(z['search_term'])}</b> ({country})\n   🔢 {z['count']} مرة\n\n"
            await safe_edit_message(query, text, parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([admin_back_btn]))
            return True

        elif data == "admin_broadcast":
            context.user_data["awaiting_broadcast"] = True
            await safe_edit_message(query,
                "📢 <b>إرسال رسالة جماعية</b>\n\n"
                "اكتب الرسالة التي تريد إرسالها لجميع المستخدمين.\n"
                "يمكنك استخدام HTML للتنسيق.\n\nأرسل /cancel للإلغاء.",
                parse_mode=ParseMode.HTML)
            return True

        elif data == "admin_confirm_broadcast":
            msg_text = context.user_data.get("broadcast_message", "")
            if not msg_text:
                await safe_answer_callback(query, "⚠️ لا توجد رسالة للإرسال.", True)
                return True

            user_ids = db.broadcast_get_all_user_ids()
            sent = 0
            failed = 0
            for uid in user_ids:
                result = await safe_send_message(
                    context.bot, uid, msg_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                )
                if result:
                    sent += 1
                else:
                    failed += 1
                await asyncio.sleep(0.1)

            context.user_data.pop("broadcast_message", None)
            context.user_data.pop("awaiting_broadcast", None)

            await safe_edit_message(query,
                f"✅ <b>تم إرسال الرسالة الجماعية!</b>\n\n"
                f"📨 تم الإرسال: <b>{sent}</b>\n"
                f"❌ فشل: <b>{failed}</b>\n"
                f"👥 الإجمالي: <b>{len(user_ids)}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لوحة التحكم", callback_data="admin_menu")]]))
            return True

        elif data == "admin_cancel_broadcast":
            context.user_data.pop("broadcast_message", None)
            context.user_data.pop("awaiting_broadcast", None)
            await safe_answer_callback(query, "تم الإلغاء.")
            await safe_edit_message(query,
                "❌ تم إلغاء الرسالة الجماعية.",
                parse_mode=ParseMode.HTML,
                reply_markup=_build_admin_menu_keyboard())
            return True

    except Exception as e:
        logger.error("Error in admin callback (data=%s): %s", data, e)
        return True

    return False


# ========================
# Error Handler
# ========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Global error handler - catches all unhandled exceptions."""
    logger.error("Unhandled exception: %s", context.error)


# ========================
# Main
# ========================

def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing! Set it in Render Environment Variables.")
        raise SystemExit(1)

    # Initialize database
    db.init_db()

    # Build application
    application = Application.builder().token(BOT_TOKEN).build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_error_handler(error_handler)

    # Schedule alert checks
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(
            check_and_send_alerts,
            interval=ALERT_INTERVAL,
            first=60,
            name="alert_checker",
        )
        logger.info("Alert scheduler started (interval: %ss)", ALERT_INTERVAL)

    logger.info("LinkedIt Bot v3.0 started successfully!")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
