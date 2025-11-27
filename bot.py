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
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
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
DB_FILE = "bankrupt.db"
COMPANIES_FILE_TXT = "companies.txt" # Старый файл для импорта
# Глобальная дата отсечения (старые банкротства до этой даты игнорируем)
GLOBAL_START_DATE = datetime.datetime.strptime("01.01.2025", "%d.%m.%Y").date()

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
        
        # МИГРАЦИЯ: Если таблица users была пуста, заполним её существующими подписчиками как активными
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
        return True, "База обновлена."
    except Exception as e:
        return False, f"Ошибка импорта: {e}"
    finally:
        if os.path.exists(csv_file): os.remove(csv_file)

# --- ЛОГИКА: ПЕРСОНАЛЬНЫЙ ПОИСК ---

def check_user_subscriptions(chat_id, save_history=True):
    """
    Проверяет банкротства ТОЛЬКО для конкретного пользователя.
    Возвращает только те записи, которые пользователь еще не видел.
    """
    if not os.path.exists(DB_FILE): return [], "База пуста."

    new_items = []
    
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        # 1. Получаем список кодов, за которыми следит этот юзер
        user_codes = cursor.execute(
            "SELECT firm_edrpou FROM subscriptions WHERE chat_id = ?", 
            (chat_id,)
        ).fetchall()
        
        if not user_codes:
            return [], "У вас нет активных подписок. Используйте /addcompany или /import_txt"

        codes_list = [c[0] for c in user_codes]
        
        # 2. Ищем эти коды в таблице банкротов
        placeholders = ','.join('?' for _ in codes_list)
        query = f"SELECT firm_edrpou, firm_name, date FROM bankrupts WHERE firm_edrpou IN ({placeholders})"
        cursor.execute(query, codes_list)
        matches = cursor.fetchall()

        for code, name, date_str in matches:
            # Фильтр по дате
            try:
                date_obj = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                if date_obj <= GLOBAL_START_DATE: continue
            except: continue

            # Фильтр по ЛИЧНОЙ истории (отправляли ли МЫ ЭТОМУ юзеру ЭТУ запись)
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

        # 3. Записываем в историю, что мы показали эти данные этому юзеру
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

def db_set_user_active(chat_id, is_active=True):
    """Устанавливает статус рассылки для пользователя."""
    with sqlite3.connect(DB_FILE) as conn:
        # UPSERT: Вставляем или обновляем
        conn.execute("""
            INSERT INTO users (chat_id, is_active) VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET is_active = excluded.is_active
        """, (chat_id, 1 if is_active else 0))

def db_add_subscription(chat_id, code):
    with sqlite3.connect(DB_FILE) as conn:
        try:
            # При добавлении компании делаем юзера активным
            db_set_user_active(chat_id, True)
            conn.execute("INSERT INTO subscriptions (chat_id, firm_edrpou) VALUES (?, ?)", (chat_id, code))
            return True
        except sqlite3.IntegrityError:
            return False # Уже есть

def db_del_subscription(chat_id, code):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.execute("DELETE FROM subscriptions WHERE chat_id = ? AND firm_edrpou = ?", (chat_id, code))
        return cursor.rowcount > 0

def db_get_user_subscriptions(chat_id):
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("SELECT firm_edrpou FROM subscriptions WHERE chat_id = ?", (chat_id,)).fetchall()
    return [r[0] for r in rows]

def db_get_active_users():
    """Получает список пользователей, у которых включена рассылка."""
    with sqlite3.connect(DB_FILE) as conn:
        # Берем пользователей, которые есть в таблице users с флагом 1 И имеют хотя бы 1 подписку
        rows = conn.execute("""
            SELECT DISTINCT u.chat_id 
            FROM users u
            JOIN subscriptions s ON u.chat_id = s.chat_id
            WHERE u.is_active = 1
        """).fetchall()
    return [r[0] for r in rows]

# --- ХЕНДЛЕРЫ ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Активируем подписку
    db_set_user_active(update.effective_chat.id, True)
    
    await update.message.reply_text(
        "👋 <b>Персональный Бот Банкротств</b>\n\n"
        "Я ежедневно проверяю реестр и сообщаю только о <b>ваших</b> компаниях.\n\n"
        "<b>Команды:</b>\n"
        "➕ <code>/addcompany 12345678</code> — Добавить в мой список\n"
        "➖ <code>/delcompany 12345678</code> — Удалить из списка\n"
        "📂 <code>/import_txt</code> — Импортировать все из companies.txt\n"
        "📋 <code>/mycompanies</code> — Мой список\n"
        "🔍 <code>/check</code> — Проверить мои компании сейчас\n"
        "🧹 <code>/clear_history</code> — Сбросить мою историю просмотров\n"
        "🔎 <code>/find 12345678</code> — Глобальный поиск по базе\n"
        "🔕 <code>/stop</code> — Приостановить рассылку (список сохранится)",
        parse_mode='HTML'
    )

async def add_company(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: `/addcompany 30991664`", parse_mode='Markdown')
        return
    code = context.args[0].strip()
    if not code.isdigit():
        await update.message.reply_text("❌ Код должен состоять только из цифр.")
        return
    
    if db_add_subscription(update.effective_chat.id, code):
        await update.message.reply_text(f"✅ Код <b>{code}</b> добавлен в ваш список. Рассылка активна.", parse_mode='HTML')
    else:
        await update.message.reply_text(f"ℹ️ Код <b>{code}</b> уже есть в вашем списке.", parse_mode='HTML')

async def import_txt_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Импортирует коды из старого файла companies.txt в БД текущего пользователя."""
    if not os.path.exists(COMPANIES_FILE_TXT):
        await update.message.reply_text("⚠️ Файл companies.txt не найден на сервере.")
        return

    chat_id = update.effective_chat.id
    added_count = 0
    total_found = 0
    
    await update.message.reply_text("⏳ Начинаю импорт из файла...")

    try:
        with open(COMPANIES_FILE_TXT, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip()
                if code and code.isdigit():
                    total_found += 1
                    if db_add_subscription(chat_id, code):
                        added_count += 1
        
        await update.message.reply_text(
            f"✅ <b>Импорт завершен!</b>\n\n"
            f"📂 Найдено кодов: {total_found}\n"
            f"➕ Добавлено новых: {added_count}\n"
            f"📋 Теперь они в вашем списке (/mycompanies).",
            parse_mode='HTML'
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при чтении файла: {e}")

async def del_company(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: `/delcompany 30991664`", parse_mode='Markdown')
        return
    code = context.args[0].strip()
    
    if db_del_subscription(update.effective_chat.id, code):
        await update.message.reply_text(f"🗑 Код <b>{code}</b> удален из вашего списка.", parse_mode='HTML')
    else:
        await update.message.reply_text(f"ℹ️ Кода <b>{code}</b> не было в вашем списке.", parse_mode='HTML')

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отключает рассылку, но сохраняет список."""
    db_set_user_active(update.effective_chat.id, False)
    await update.message.reply_text(
        "🔕 <b>Рассылка отключена.</b>\n"
        "Ваш список компаний сохранен. Вы можете проверять его вручную через /check.\n"
        "Чтобы возобновить рассылку, нажмите /start или добавьте новую компанию.", 
        parse_mode='HTML'
    )

async def my_companies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    codes = db_get_user_subscriptions(update.effective_chat.id)
    if not codes:
        await update.message.reply_text("📭 Ваш список пуст.")
        return
    text = f"📋 <b>Ваш список ({len(codes)} шт):</b>\n" + "\n".join(f"• <code>{c}</code>" for c in codes)
    await update.message.reply_text(text, parse_mode='HTML')

async def clear_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    with sqlite3.connect(DB_FILE) as conn:
        # Удаляем историю только для этого юзера
        conn.execute("DELETE FROM sent_history WHERE chat_id = ?", (chat_id,))
    await update.message.reply_text("🧹 Ваша история просмотров очищена.")

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Проверяю ваш список...")
    items, msg = await asyncio.to_thread(check_user_subscriptions, update.effective_chat.id, save_history=True)
    
    if not items:
        # Если список пуст, это может быть потому что нет подписок
        if msg != "OK": await update.message.reply_text(f"ℹ️ {msg}")
        else: await update.message.reply_text("✅ По вашим компаниям новых банкротств нет.")
        return

    text = f"🚨 <b>НОВЫЕ СОБЫТИЯ ({len(items)}):</b>\n\n"
    for i, item in enumerate(items, 1):
        safe_name = html.escape(item['name'])
        text += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {safe_name}\n📅 {item['date']}\n────────────────\n"
    
    await update.message.reply_text(text, parse_mode='HTML')

async def find_one(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный поиск по базе (без привязки к пользователю)."""
    if not context.args:
        await update.message.reply_text("Пример: `/find 30991664`", parse_mode='Markdown')
        return
    code = context.args[0].strip()
    
    def db_search(c):
        if not os.path.exists(DB_FILE): return "База не скачана."
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute("SELECT firm_name, date FROM bankrupts WHERE firm_edrpou = ?", (c,)).fetchall()
        if not rows: return f"✅ По коду {c} ничего не найдено."
        res = f"🔎 <b>Результаты по {c}:</b>\n"
        for n, d in rows: 
            safe_n = html.escape(n)
            res += f"\n- {safe_n} ({d})"
        return res

    result = await asyncio.to_thread(db_search, code)
    await update.message.reply_text(result, parse_mode='HTML')

async def manual_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновляет базу и запускает проверку для текущего юзера."""
    await update.message.reply_text("⏳ Обновляю общую базу...")
    res, msg = await asyncio.to_thread(update_database_logic)
    if res:
        await update.message.reply_text("✅ База обновлена. Проверяю ваши подписки...")
        await check_command(update, context)
    else:
        await update.message.reply_text(f"❌ {msg}")

# --- ЕЖЕДНЕВНАЯ ЗАДАЧА (МАССОВАЯ РАССЫЛКА) ---

async def daily_routine(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Start daily routine")
    
    # 1. Обновляем общую базу данных
    res, msg = await asyncio.to_thread(update_database_logic)
    if not res:
        logging.error(f"Daily update failed: {msg}")
        return

    # 2. Получаем список АКТИВНЫХ пользователей с подписками
    users = await asyncio.to_thread(db_get_active_users)
    
    is_monday = (datetime.datetime.now().weekday() == 0)
    
    # 3. Проходим по каждому пользователю индивидуально
    for chat_id in users:
        try:
            # Проверяем подписки конкретного юзера
            items, _ = await asyncio.to_thread(check_user_subscriptions, chat_id, save_history=True)
            
            message = None
            if items:
                message = f"🚨 <b>СВЕЖИЕ БАНКРОТСТВА ({len(items)}):</b>\n\n"
                for i, item in enumerate(items, 1):
                    safe_name = html.escape(item['name'])
                    message += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {safe_name}\n📅 {item['date']}\n────────────────\n"
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
    # Ежедневная проверка в 09:00
    jq.run_daily(daily_routine, time=datetime.time(hour=9, minute=0, tzinfo=kyiv_tz))
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addcompany", add_company))
    app.add_handler(CommandHandler("import_txt", import_txt_command)) # <-- Новая команда
    app.add_handler(CommandHandler("delcompany", del_company))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("mycompanies", my_companies))
    app.add_handler(CommandHandler("check", check_command))
    app.add_handler(CommandHandler("find", find_one))
    app.add_handler(CommandHandler("update", manual_update))
    app.add_handler(CommandHandler("clear_history", clear_history_command))

    print("Multi-user Bot Started...")
    app.run_polling()