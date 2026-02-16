import logging
import asyncio
import html
import re
import warnings
import urllib.parse
import os
from datetime import datetime
from threading import Thread
from flask import Flask
from jobspy import scrape_jobs
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode

# --- Flask Server to keep Render alive ---
app = Flask('')

@app.route('/')
def home():
    return "LinkedIt Bot is running! 🚀"

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

# --- Bot Settings ---
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8237443289:AAGLMRVjfEnwTOhv192i-o-xUmKeElIlZvU')
WHATSAPP_LINK = os.environ.get('WHATSAPP_LINK', 'https://whatsapp.com/channel/0029Vat1TW960eBmmdCzvA0r')

# Logging setup
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=FutureWarning)

# Supported Countries
COUNTRIES = {
    "qa": {"name": "قطر 🇶🇦", "flag": "🇶🇦", "name_en": "Qatar", "indeed_country": "Qatar", "location": "Qatar"},
    "ae": {"name": "الإمارات 🇦🇪", "flag": "🇦🇪", "name_en": "United Arab Emirates", "indeed_country": "United Arab Emirates", "location": "United Arab Emirates"},
    "sa": {"name": "السعودية 🇸🇦", "flag": "🇸🇦", "name_en": "Saudi Arabia", "indeed_country": "Saudi Arabia", "location": "Saudi Arabia"},
    "bh": {"name": "البحرين 🇧🇭", "flag": "🇧🇭", "name_en": "Bahrain", "indeed_country": "Bahrain", "location": "Bahrain"},
}

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

def escape_html(text: str) -> str:
    if not text: return ""
    return html.escape(str(text))

def extract_email_from_text(text: str) -> str:
    if not text: return ""
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', str(text))
    return emails[0] if emails else ""

def format_job_message(job, country_name: str) -> tuple:
    title = escape_html(str(job.get("title", "غير محدد")))
    company = escape_html(str(job.get("company", "غير محدد")))
    if company in ("nan", "None", ""): company = "غير محدد"
    
    location_val = str(job.get("location", ""))
    location_display = country_name
    if location_val and location_val not in ("nan", "", "None"):
        city = location_val.split(",")[0].strip()
        location_display = f"{city}، {country_name}"

    description = str(job.get("description", ""))
    if description and description not in ("nan", "", "None"):
        description = re.sub(r'<[^>]+>', '', description)
        description = re.sub(r'\s+', ' ', description).strip()
        description = description[:450] + "..." if len(description) > 450 else description
        description = escape_html(description)
    else:
        description = "لا يوجد وصف متاح حالياً"

    job_url = str(job.get("job_url", ""))
    if job_url in ("nan", "", "None"): job_url = ""

    emails_val = job.get("emails", "")
    email = ""
    if emails_val and str(emails_val) not in ("nan", "", "None", "[]"):
        if isinstance(emails_val, list): email = emails_val[0]
        else:
            found = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', str(emails_val))
            email = found[0] if found else ""
    if not email: email = extract_email_from_text(str(job.get("description", "")))

    site = str(job.get("site", ""))
    source_names = {"indeed": "Indeed", "linkedin": "LinkedIn", "google": "Google Jobs"}
    source_name = source_names.get(site, site)

    msg = f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"💼 <b>{title} - {location_display}</b>\n"
    msg += f"🏢 {company}\n"
    if source_name: msg += f"🌐 المصدر: {escape_html(source_name)}\n"
    msg += f"\n{description}\n"
    if email: msg += f"\n📧 <b>التواصل:</b> {escape_html(email)}\n"
    if job_url: msg += f"\n🔗 <a href='{job_url}'>رابط التقديم على الوظيفة</a>\n"
    msg += f"\n👉 <a href='{WHATSAPP_LINK}'>تابعنا على واتساب للمزيد</a>"
    msg += f"\n━━━━━━━━━━━━━━━━━━━━━"

    share_text = f"💼 {title} - {location_display}\n"
    if company != "غير محدد": share_text += f"🏢 {company}\n"
    if job_url: share_text += f"🔗 التقديم: {job_url}\n"
    if email: share_text += f"📧 التواصل: {email}\n"
    share_text += f"\n📱 للمزيد من الوظائف: {WHATSAPP_LINK}"
    whatsapp_url = f"https://api.whatsapp.com/send?text={urllib.parse.quote(share_text)}"
    
    return msg, whatsapp_url

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")],
        [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")],
        [InlineKeyboardButton("📱 تابعنا على واتساب", url=WHATSAPP_LINK)],
    ]
    await update.message.reply_text(
        f"👋 أهلاً بك في بوت <b>LinkedIt By Abdulrahman</b>\n\n"
        "أنا أساعدك في العثور على أحدث الوظائف في دول الخليج (قطر، الإمارات، السعودية، البحرين).\n\n"
        "اختر من القائمة أدناه للبدء:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🇶🇦 قطر", callback_data="country_qa"), InlineKeyboardButton("🇦🇪 الإمارات", callback_data="country_ae")],
        [InlineKeyboardButton("🇸🇦 السعودية", callback_data="country_sa"), InlineKeyboardButton("🇧🇭 البحرين", callback_data="country_bh")],
        [InlineKeyboardButton("🌍 جميع الدول", callback_data="country_all")],
    ]
    await update.message.reply_text("🔍 <b>اختر الدولة للبحث عن وظائف:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 <b>دليل استخدام بوت LinkedIt:</b>\n\n"
        "1️⃣ اضغط على /start للبدء.\n"
        "2️⃣ اختر <b>بحث عن وظيفة</b> ثم اختر الدولة.\n"
        "3️⃣ اكتب المسمى الوظيفي (مثلاً: Accountant أو مهندس).\n"
        "4️⃣ سيقوم البوت بالبحث في Indeed و LinkedIn و Google Jobs.\n\n"
        "💡 <i>نصيحة: البحث بالإنجليزية يعطي نتائج أكثر وأدق.</i>"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

def search_jobs_logic(search_term, country_code):
    all_jobs = []
    if country_code == "all":
        codes = list(COUNTRIES.keys())
    else:
        codes = [country_code]
    
    for cc in codes:
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin"],
                search_term=search_term,
                location=COUNTRIES[cc]["location"],
                country_indeed=COUNTRIES[cc]["indeed_country"],
                results_wanted=15,
                hours_old=336
            )
            if not jobs.empty:
                for _, row in jobs.iterrows():
                    job_dict = row.to_dict()
                    job_dict["_country_name"] = COUNTRIES[cc]["name"]
                    all_jobs.append(job_dict)
        except Exception as e:
            logger.error(f"Error in {cc}: {e}")
    return all_jobs

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "search":
        keyboard = [
            [InlineKeyboardButton("🇶🇦 قطر", callback_data="country_qa"), InlineKeyboardButton("🇦🇪 الإمارات", callback_data="country_ae")],
            [InlineKeyboardButton("🇸🇦 السعودية", callback_data="country_sa"), InlineKeyboardButton("🇧🇭 البحرين", callback_data="country_bh")],
            [InlineKeyboardButton("🌍 جميع الدول", callback_data="country_all")],
            [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")],
        ]
        await query.edit_message_text("🔍 <b>اختر الدولة للبحث:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "categories":
        keyboard = [[InlineKeyboardButton(c["name"], callback_data=f"cat_{k}")] for k, c in JOB_CATEGORIES.items()]
        keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])
        await query.edit_message_text("📂 <b>اختر تصنيف الوظائف:</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("country_"):
        country_code = data.replace("country_", "")
        context.user_data["country"] = country_code
        await query.edit_message_text(f"✍️ <b>أرسل الآن المسمى الوظيفي الذي تبحث عنه:</b>\n(مثال: مهندس، محاسبة، Sales، Developer)", parse_mode=ParseMode.HTML)

    elif data.startswith("cat_"):
        cat_id = data.replace("cat_", "")
        search_term = JOB_CATEGORIES[cat_id]["query"]
        await perform_search(query, context, search_term, "all", is_callback=True)

    elif data == "back_main":
        keyboard = [[InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")], [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")], [InlineKeyboardButton("📱 تابعنا على واتساب", url=WHATSAPP_LINK)]]
        await query.edit_message_text(f"👋 أهلاً بك في بوت <b>LinkedIt By Abdulrahman</b>\n\nاختر من القائمة أدناه للبدء:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    search_term = update.message.text
    country_code = context.user_data.get("country", "all")
    await perform_search(update, context, search_term, country_code)

async def perform_search(update_or_query, context, search_term, country_code, is_callback=False):
    if is_callback:
        msg = await update_or_query.edit_message_text(f"🔍 جاري البحث عن <b>{search_term}</b>... يرجى الانتظار.", parse_mode=ParseMode.HTML)
        chat_id = update_or_query.message.chat_id
    else:
        msg = await update_or_query.message.reply_text(f"🔍 جاري البحث عن <b>{search_term}</b>... يرجى الانتظار.", parse_mode=ParseMode.HTML)
        chat_id = update_or_query.message.chat_id

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, search_jobs_logic, search_term, country_code)

    if not results:
        await context.bot.send_message(chat_id, f"😔 لم أجد وظائف حالياً لـ {search_term}. حاول مرة أخرى بمسمى مختلف.")
        return

    await context.bot.send_message(chat_id, f"✅ تم العثور على {len(results[:15])} وظيفة:")
    for job in results[:15]:
        c_name = job.get("_country_name", "الخليج")
        text, wa_url = format_job_message(job, c_name)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("📤 مشاركة عبر واتساب", url=wa_url)]])
        await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=markup, disable_web_page_preview=True)
        await asyncio.sleep(0.5)

def main():
    Thread(target=run_flask).start()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    logger.info("Bot started...")
    application.run_polling()

if __name__ == '__main__':
    main()
