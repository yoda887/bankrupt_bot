import logging
import os
import requests
import pandas as pd
import datetime
import pytz
import asyncio
import sqlite3
import html
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, 
    ContextTypes, 
    CommandHandler, 
    ConversationHandler, 
    MessageHandler, 
    filters
)
from dotenv import load_dotenv

# --- КОНФИГУРАЦИЯ ---
load_dotenv()
TOKEN = os.getenv('BOT_TOKEN')

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Настройки
ADMIN_CHAT_ID = 889325852
DB_FILE = "bankrupt.db"
COMPANIES_FILE_TXT = "companies.txt"
GLOBAL_START_DATE = datetime.datetime.strptime("01.01.2025", "%d.%m.%Y").date()

# Состояния для ConversationHandler
FIND_WAITING_CODE = 1
ADD_WAITING_CODE = 2
DEL_WAITING_CODE = 3

# --- ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ---

def init_db():
    """Создает сложную структуру БД для многопользовательского режима."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        # 1. Таблица сырых данных (общий реестр)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bankrupts (
                firm_edrpou TEXT,
                firm_name TEXT,
                date TEXT
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_edrpou ON bankrupts (firm_edrpou)")

        # 2. Таблица пользователей (статус подписки)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                is_active INTEGER DEFAULT 1
            )
        """)

        # 3. Таблица подписок (Кто -> За кем следит)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                chat_id INTEGER,
                firm_edrpou TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, firm_edrpou)
            )
        """)

        # 4. Таблица истории уведомлений (Кто -> О чем уже знает)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sent_history (
                chat_id INTEGER,
                firm_edrpou TEXT,
                date TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, firm_edrpou, date)
            )
        """)
        
        # МИГРАЦИЯ
        cursor.execute("INSERT OR IGNORE INTO users (chat_id, is_active) SELECT DISTINCT chat_id, 1 FROM subscriptions")
        conn.commit()

# --- ЯДРО: ОБНОВЛЕНИЕ БАЗЫ (ГЛОБАЛЬНОЕ) ---

def update_database_logic():
    """Скачивает CSV и обновляет общую таблицу bankrupts."""
    logging.info("Начало скачивания базы...")
    
    try:
        api_url = 'https://data.gov.ua/api/3/action/package_show?id=544d4dad-0b6d-4972-b0b8-fb266829770f'
        resp = requests.get(api_url, timeout=10).json()
        if resp.get('success'):
            resource_url = resp['result']['resources'][-1]['url']
        else:
            resource_url = 'https://data.gov.ua/dataset/544d4dad-0b6d-4972-b0b8-fb266829770f/resource/deb76481-a6c8-4a45-ae6c-f02aa87e9f4a/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
    except Exception as e:
        return False, f"Ошибка API: {e}"

    csv_file = "temp_bankrupt.csv"
    try:
        r = requests.get(resource_url, stream=True, timeout=120)
        with open(csv_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        return False, f"Ошибка скачивания: {e}"

    try:
        df = pd.read_csv(csv_file, sep=None, engine="python", on_bad_lines="skip", encoding="utf-8", encoding_errors='replace')
        df.columns = df.columns.str.strip()
        df['firm_edrpou'] = df['firm_edrpou'].astype(str).str.strip()
        df['firm_name'] = df['firm_name'].astype(str).str.strip()
        df['date'] = df['date'].astype(str).str.strip()
        
        with sqlite3.connect(DB_FILE) as conn:
            df.to_sql('bankrupts', conn, if_exists='replace', index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_edrpou ON bankrupts (firm_edrpou)")
            
        logging.info("База обновлена.")
        return True, "База оновлена."
    except Exception as e:
        return False, f"Помилка імпорту: {e}"
    finally:
        if os.path.exists(csv_file): os.remove(csv_file)

# --- ЛОГИКА: ПЕРСОНАЛЬНЫЙ ПОИСК ---

def check_user_subscriptions(chat_id, save_history=True):
    """Проверяет банкротства ТОЛЬКО для конкретного пользователя."""
    if not os.path.exists(DB_FILE): return [], "База пуста."

    new_items = []
    
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        user_codes = cursor.execute(
            "SELECT firm_edrpou FROM subscriptions WHERE chat_id = ?", 
            (chat_id,)
        ).fetchall()
        
        if not user_codes:
            return [], "У вас немає активних підписок. Використайте /addcompany"

        codes_list = [c[0] for c in user_codes]
        
        placeholders = ','.join('?' for _ in codes_list)
        query = f"SELECT firm_edrpou, firm_name, date FROM bankrupts WHERE firm_edrpou IN ({placeholders})"
        cursor.execute(query, codes_list)
        matches = cursor.fetchall()

        for code, name, date_str in matches:
            try:
                date_obj = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                if date_obj <= GLOBAL_START_DATE: continue
            except: continue

            if save_history:
                seen = cursor.execute(
                    "SELECT 1 FROM sent_history WHERE chat_id = ? AND firm_edrpou = ? AND date = ?", 
                    (chat_id, code, date_str)
                ).fetchone()
                if seen: continue 

            new_items.append({
                "code": code,
                "name": name,
                "date": date_str,
                "date_obj": date_obj
            })

        if save_history and new_items:
            history_data = [(chat_id, i['code'], i['date']) for i in new_items]
            cursor.executemany(
                "INSERT OR IGNORE INTO sent_history (chat_id, firm_edrpou, date) VALUES (?, ?, ?)", 
                history_data
            )
            conn.commit()
            
    new_items.sort(key=lambda x: x["date_obj"])
    return new_items, "OK"

# --- УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ И ПОДПИСКАМИ (SQL) ---

#def db_set_user_active(chat_id, is_active=True):
#    with sqlite3.connect(DB_FILE) as conn:
#        conn.execute("""
#            INSERT INTO users (chat_id, is_active) VALUES (?, ?)
#            ON CONFLICT(chat_id) DO UPDATE SET is_active = excluded.is_active
#        """, (chat_id, 1 if is_active else 0))

def db_set_user_active(chat_id, is_active=True):
    """Возвращает True, если это новый пользователь."""
    is_new_user = False
    with sqlite3.connect(DB_FILE) as conn:
        # Проверяем наличие пользователя до вставки
        cursor = conn.execute("SELECT 1 FROM users WHERE chat_id = ?", (chat_id,))
        if not cursor.fetchone():
            is_new_user = True

        conn.execute("""
            INSERT INTO users (chat_id, is_active) VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET is_active = excluded.is_active
        """, (chat_id, 1 if is_active else 0))
    return is_new_user


def db_add_subscription(chat_id, code):
    with sqlite3.connect(DB_FILE) as conn:
        try:
            db_set_user_active(chat_id, True)
            conn.execute("INSERT INTO subscriptions (chat_id, firm_edrpou) VALUES (?, ?)", (chat_id, code))
            return True
        except sqlite3.IntegrityError:
            return False

def db_del_subscription(chat_id, code):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.execute("DELETE FROM subscriptions WHERE chat_id = ? AND firm_edrpou = ?", (chat_id, code))
        return cursor.rowcount > 0

def db_get_user_subscriptions(chat_id):
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("SELECT firm_edrpou FROM subscriptions WHERE chat_id = ?", (chat_id,)).fetchall()
    return [r[0] for r in rows]

def db_get_active_users():
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT DISTINCT u.chat_id 
            FROM users u
            JOIN subscriptions s ON u.chat_id = s.chat_id
            WHERE u.is_active = 1
        """).fetchall()
    return [r[0] for r in rows]

# --- ХЕНДЛЕРЫ ---

#async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
#    db_set_user_active(update.effective_chat.id, True)
#    await update.message.reply_text(
#        "👋 <b>Бот Монітор Банкрутств</b>\n\n"
#        "Я щоденно перевіряю реєстр та повідомляю про нові банкрутства.\n\n"
#        "<b>Команди:</b>\n"
#        "/addcompany — Додати код у список стеження\n"
#        "/delcompany — Видалити зі списку\n"
#        "/mycompanies — Мій список для стеження\n"
#        "/check — Перевірити мій список зараз\n"
#        "/clear_history — Скинути історію переглядів\n"
#        "/find — Пошук по базі банкротів\n"
#        "/stop — Зупинити розсилку",
#        parse_mode='HTML'
#    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Регистрируем пользователя и проверяем, новый ли он
    is_new = db_set_user_active(update.effective_chat.id, True)
    
    # Если новый - отправляем уведомление админу
    if is_new:
        try:
            user = update.effective_user
            username = f"@{user.username}" if user.username else "без юзернейма"
            admin_text = (
                f"👤 <b>Новий користувач бота!</b>\n"
                f"🆔 ID: <code>{user.id}</code>\n"
                f"📝 Имя: {html.escape(user.full_name)}\n"
                f"🔗 Линк: {username}"
            )
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_text, parse_mode='HTML')
        except Exception as e:
            logging.error(f"Не удалось отправить уведомление админу: {e}")

    await update.message.reply_text(
        "👋 <b>Бот Монітор Банкрутств</b>\n\n"
        "Я щоденно перевіряю реєстр та повідомляю про нові банкрутства.\n\n"
        "<b>Команди:</b>\n"
        "/addcompany — Додати код у список стеження\n"
        "/delcompany — Видалити зі списку\n"
        "/mycompanies — Мій список для стеження\n"
        "/check — Перевірити мій список зараз\n"
        "/clear_history — Скинути історію переглядів\n"
        "/find — Пошук по базі банкротів\n"
        "/stop — Зупинити розсилку",
        parse_mode='HTML'
    )

async def import_txt_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Импортирует коды из старого файла companies.txt в БД текущего пользователя."""
    if not os.path.exists(COMPANIES_FILE_TXT):
        await update.message.reply_text("?? Файл companies.txt не найден на сервере.")
        return

    chat_id = update.effective_chat.id
    added_count = 0
    total_found = 0
    
    await update.message.reply_text("? Начинаю импорт из файла...")

    try:
        with open(COMPANIES_FILE_TXT, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip()
                if code and code.isdigit():
                    total_found += 1
                    if db_add_subscription(chat_id, code):
                        added_count += 1
        
        await update.message.reply_text(
            f"? <b>Импорт завершен!</b>\n\n"
            f"?? Найдено кодов: {total_found}\n"
            f"? Добавлено новых: {added_count}\n"
            f"?? Теперь они в вашем списке (/mycompanies).",
            parse_mode='HTML'
        )
    except Exception as e:
        await update.message.reply_text(f"? Ошибка при чтении файла: {e}")
# --- ЛОГИКА ДОБАВЛЕНИЯ (ADD) ---

async def add_company_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 1: Старт добавления."""
    # Поддержка быстрого добавления (/addcompany 123)
    if context.args:
        code = context.args[0].strip()
        await _add_company_logic(update, code)
        return ConversationHandler.END

    await update.message.reply_text(
        "Введіть код (ЄДРПОУ або ІПН) для додавання до списку або для скасування введіть /cancel"
    )
    return ADD_WAITING_CODE

async def add_company_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 2: Обработка ввода кода."""
    code = update.message.text.strip()
    await _add_company_logic(update, code)
    return ConversationHandler.END

async def _add_company_logic(update, code):
    """Общая логика добавления в БД."""
    if not code.isdigit():
        await update.message.reply_text("❌ Код має складатися тільки з цифр.")
        return
    
    if db_add_subscription(update.effective_chat.id, code):
        await update.message.reply_text(f"✅ Код <b>{code}</b> доданий. Розсилка активна.", parse_mode='HTML')
    else:
        await update.message.reply_text(f"ℹ️ Код <b>{code}</b> вже є у вашому списку.", parse_mode='HTML')

# --- ЛОГИКА УДАЛЕНИЯ (DEL) ---

async def del_company_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 1: Старт удаления."""
    # Поддержка быстрого удаления (/delcompany 123)
    if context.args:
        code = context.args[0].strip()
        await _del_company_logic(update, code)
        return ConversationHandler.END

    await update.message.reply_text(
        "🗑 Введіть код (ЄДРПОУ або ІПН) для видалення або для скасування введіть /cancel"
    )
    return DEL_WAITING_CODE

async def del_company_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 2: Обработка ввода кода."""
    code = update.message.text.strip()
    await _del_company_logic(update, code)
    return ConversationHandler.END

async def _del_company_logic(update, code):
    """Общая логика удаления из БД."""
    if db_del_subscription(update.effective_chat.id, code):
        await update.message.reply_text(f"🗑 Код <b>{code}</b> видалений.", parse_mode='HTML')
    else:
        await update.message.reply_text(f"ℹ️ Кода <b>{code}</b> не було у списку.", parse_mode='HTML')

# --- ДРУГИЕ ХЕНДЛЕРЫ ---

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db_set_user_active(update.effective_chat.id, False)
    await update.message.reply_text("🔕 Розсилка відключена.", parse_mode='HTML')

async def my_companies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    codes = db_get_user_subscriptions(update.effective_chat.id)
    if not codes:
        await update.message.reply_text("📭 Ваш список порожній.")
        return
    text = f"📋 <b>Ваш список ({len(codes)} шт):</b>\n" + "\n".join(f"• <code>{c}</code>" for c in codes)
    await update.message.reply_text(text, parse_mode='HTML')

async def clear_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM sent_history WHERE chat_id = ?", (chat_id,))
    await update.message.reply_text("🧹 Ваша історія переглядів очищена.")

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Перевіряю ваш список...")
    items, msg = await asyncio.to_thread(check_user_subscriptions, update.effective_chat.id, save_history=True)
    
    if not items:
        if msg != "OK": await update.message.reply_text(f"ℹ️ {msg}")
        else: await update.message.reply_text("✅ По вашому списку нових банкрутств немає.")
        return

    text = f"🚨 <b>НОВІ ПОДІЇ ({len(items)}):</b>\n\n"
    for i, item in enumerate(items, 1):
        safe_name = html.escape(item['name'])
        text += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {safe_name}\n📅 {item['date']}\n\n"
    
    await update.message.reply_text(text, parse_mode='HTML')

async def manual_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Оновлюю загальну базу...")
    res, msg = await asyncio.to_thread(update_database_logic)
    if res:
        await update.message.reply_text("✅ База оновлена. Перевіряю ваші підписки...")
        await check_command(update, context)
    else:
        await update.message.reply_text(f"❌ {msg}")

# --- ФУНКЦИИ ДЛЯ CONVERSATION HANDLER (/find) ---

async def find_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 1: Пользователь вводит /find, бот просит код."""
    await update.message.reply_text(
        "🔎 Введіть код (ЄДРПОУ або ІПН) для пошуку або для скасування введіть /cancel"
    )
    return FIND_WAITING_CODE

async def find_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 2: Пользователь ввел код, бот ищет и отвечает."""
    code = update.message.text.strip()
    
    # Логика поиска в БД
    def db_search(c):
        if not os.path.exists(DB_FILE): return "База не скачана."
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute("SELECT firm_name, date FROM bankrupts WHERE firm_edrpou = ?", (c,)).fetchall()
        if not rows: return f"✅ По коду {c} нічого не знайдено."
        res = f"🔎 <b>Результати по {c}:</b>\n"
        for n, d in rows: 
            safe_n = html.escape(n)
            res += f"\n- {safe_n} ({d})"
        return res

    await update.message.reply_text("⏳ Шукаю...")
    result = await asyncio.to_thread(db_search, code)
    await update.message.reply_text(result, parse_mode='HTML')
    
    # Завершаем разговор
    return ConversationHandler.END

async def cancel_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общая функция отмены для всех диалогов."""
    await update.message.reply_text("❌ Операція скасована.")
    return ConversationHandler.END

# --- ЕЖЕДНЕВНАЯ ЗАДАЧА (МАССОВАЯ РАССЫЛКА) ---

async def daily_routine(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Start daily routine")
    
    res, msg = await asyncio.to_thread(update_database_logic)
    if not res:
        try:
            await context.bot.send_message(
                ADMIN_CHAT_ID, 
                f"⚠️ <b>Ошибка утреннего обновления!</b>\n{html.escape(msg)}", 
                parse_mode='HTML'
            )
        except: pass
        return

    users = await asyncio.to_thread(db_get_active_users)
    is_monday = (datetime.datetime.now().weekday() == 0)
    
    for chat_id in users:
        try:
            items, _ = await asyncio.to_thread(check_user_subscriptions, chat_id, save_history=True)
            message = None
            if items:
                message = f"🚨 <b>НОВІ БАНКРУТСТВА ({len(items)}):</b>\n\n"
                for i, item in enumerate(items, 1):
                    safe_name = html.escape(item['name'])
                    message += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {safe_name}\n📅 {item['date']}\n\n"
            elif is_monday:
                message = "👋 <b>Понедельник.</b>\nБот работает. По вашему списку компаний новых банкротств нет."
            
            if message:
                await context.bot.send_message(chat_id, message, parse_mode='HTML')
                
        except Exception as e:
            logging.error(f"Error checking for user {chat_id}: {e}")

# --- ЗАПУСК ---

if __name__ == '__main__':
    if not TOKEN: exit("NO TOKEN")
    
    init_db()
    
    app = ApplicationBuilder().token(TOKEN).build()
    
    jq = app.job_queue
    kyiv_tz = pytz.timezone('Europe/Kiev')
    jq.run_daily(daily_routine, time=datetime.time(hour=9, minute=0, tzinfo=kyiv_tz))
    
    # Обычные команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("mycompanies", my_companies))
    app.add_handler(CommandHandler("check", check_command))
    app.add_handler(CommandHandler("update", manual_update))
    app.add_handler(CommandHandler("clear_history", clear_history_command))
     #app.add_handler(CommandHandler("import_txt", import_txt_command)) # <-- Новая команда
    
    # 1. Диалог для поиска (/find)
    find_handler = ConversationHandler(
        entry_points=[CommandHandler('find', find_start)],
        states={
            FIND_WAITING_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, find_answer)]
        },
        fallbacks=[CommandHandler('cancel', cancel_operation)]
    )
    app.add_handler(find_handler)

    # 2. Диалог для добавления (/addcompany)
    add_handler = ConversationHandler(
        entry_points=[CommandHandler('addcompany', add_company_start)],
        states={
            ADD_WAITING_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_company_handle)]
        },
        fallbacks=[CommandHandler('cancel', cancel_operation)]
    )
    app.add_handler(add_handler)

    # 3. Диалог для удаления (/delcompany)
    del_handler = ConversationHandler(
        entry_points=[CommandHandler('delcompany', del_company_start)],
        states={
            DEL_WAITING_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, del_company_handle)]
        },
        fallbacks=[CommandHandler('cancel', cancel_operation)]
    )
    app.add_handler(del_handler)

    print("Multi-user Bot Started...")
    app.run_polling()