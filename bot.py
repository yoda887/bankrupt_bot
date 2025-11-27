import logging
import os
import requests
import pandas as pd
import datetime
import pytz
import asyncio
import sqlite3
import html  # <--- ВОТ ЭТА БИБЛИОТЕКА ОБЯЗАТЕЛЬНА ДЛЯ РАБОТЫ escape()
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

# Файлы и настройки
SUBSCRIBERS_FILE = "subscribers.txt"
COMPANIES_FILE = "companies.txt"
DB_FILE = "bankrupt.db"

# Дата отсечения (старые банкротства до этой даты игнорируем)
GLOBAL_START_DATE = datetime.datetime.strptime("01.01.2025", "%d.%m.%Y").date()

# --- РАБОТА С ТЕКСТОВЫМИ ФАЙЛАМИ ---

def get_monitored_codes():
    """Читает список кодов для мониторинга из файла."""
    if not os.path.exists(COMPANIES_FILE): return []
    with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def add_monitored_code(code):
    """Добавляет код предприятия в файл, если его там нет."""
    codes = get_monitored_codes()
    if code not in codes:
        with open(COMPANIES_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{code}\n")
        return True
    return False

def get_subscribers():
    """Читает список ID подписчиков."""
    if not os.path.exists(SUBSCRIBERS_FILE): return set()
    with open(SUBSCRIBERS_FILE, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def manage_subscriber(chat_id, action="add"):
    """Добавляет или удаляет подписчика."""
    subs = get_subscribers()
    chat_id_str = str(chat_id)
    
    if action == "add":
        if chat_id_str not in subs:
            with open(SUBSCRIBERS_FILE, 'a') as f:
                f.write(f"{chat_id_str}\n")
            return True
    elif action == "remove":
        if chat_id_str in subs:
            subs.remove(chat_id_str)
            with open(SUBSCRIBERS_FILE, 'w') as f:
                f.write("\n".join(subs) + "\n")
            return True
    return False

# --- ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ---

def init_db():
    """Создает таблицы в SQLite, если их нет."""
    with sqlite3.connect(DB_FILE) as conn:
        # Таблица для текущих данных из реестра (перезаписывается при обновлении)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bankrupts (
                firm_edrpou TEXT,
                firm_name TEXT,
                date TEXT
            )
        """)
        
        # Таблица истории (что мы уже видели/отправили)
        # Храним уникальную пару (код + дата), чтобы различать разные дела
        conn.execute("""
            CREATE TABLE IF NOT EXISTS history (
                firm_edrpou TEXT,
                date TEXT,
                seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (firm_edrpou, date)
            )
        """)
        
        # Индексы для скорости
        conn.execute("CREATE INDEX IF NOT EXISTS idx_edrpou ON bankrupts (firm_edrpou)")

# --- ЛОГИКА ОБНОВЛЕНИЯ И ПОИСКА ---

def update_database_logic():
    """Скачивает CSV и обновляет таблицу bankrupts."""
    logging.info("Начало обновления базы...")
    
    # 1. Получаем ссылку через API
    try:
        api_url = 'https://data.gov.ua/api/3/action/package_show?id=544d4dad-0b6d-4972-b0b8-fb266829770f'
        resp = requests.get(api_url, timeout=10).json()
        if resp.get('success'):
            resource_url = resp['result']['resources'][-1]['url']
        else:
            resource_url = 'https://data.gov.ua/dataset/544d4dad-0b6d-4972-b0b8-fb266829770f/resource/deb76481-a6c8-4a45-ae6c-f02aa87e9f4a/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
    except Exception as e:
        return False, f"Ошибка API: {e}"

    # 2. Скачиваем файл
    csv_file = "temp_bankrupt.csv"
    try:
        r = requests.get(resource_url, stream=True, timeout=120)
        with open(csv_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        return False, f"Ошибка скачивания: {e}"

    # 3. Читаем и пишем в SQL
    try:
        df = pd.read_csv(csv_file, sep=None, engine="python", on_bad_lines="skip", encoding="utf-8", encoding_errors='replace')
        
        df.columns = df.columns.str.strip()
        # Стандартизация данных
        df['firm_edrpou'] = df['firm_edrpou'].astype(str).str.strip()
        df['firm_name'] = df['firm_name'].astype(str).str.strip()
        df['date'] = df['date'].astype(str).str.strip()
        
        with sqlite3.connect(DB_FILE) as conn:
            # Полная замена таблицы свежими данными
            df.to_sql('bankrupts', conn, if_exists='replace', index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_edrpou ON bankrupts (firm_edrpou)")
            
        logging.info("База успешно обновлена.")
        return True, "База обновлена."
    except Exception as e:
        return False, f"Ошибка импорта: {e}"
    finally:
        if os.path.exists(csv_file): os.remove(csv_file)

def get_bankruptcies(save_to_history=True, ignore_history=False):
    """
    Универсальная функция поиска.
    Возвращает список банкротов из companies.txt, которые есть в базе.
    """
    codes = get_monitored_codes()
    if not codes:
        return [], "Список мониторинга пуст."

    if not os.path.exists(DB_FILE):
        return [], "База данных не найдена. Сначала /update."

    items = []
    
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        placeholders = ','.join('?' for _ in codes)
        query = f"SELECT firm_edrpou, firm_name, date FROM bankrupts WHERE firm_edrpou IN ({placeholders})"
        cursor.execute(query, codes)
        rows = cursor.fetchall()

        for code, name, date_str in rows:
            try:
                date_obj = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                if date_obj <= GLOBAL_START_DATE:
                    continue
            except: continue

            if not ignore_history:
                seen = cursor.execute(
                    "SELECT 1 FROM history WHERE firm_edrpou = ? AND date = ?", 
                    (code, date_str)
                ).fetchone()
                if seen:
                    continue 

            items.append({
                "code": code,
                "name": name,
                "date": date_str,
                "date_obj": date_obj
            })

        items.sort(key=lambda x: x["date_obj"])

        if save_to_history and items:
            data = [(i['code'], i['date']) for i in items]
            cursor.executemany(
                "INSERT OR IGNORE INTO history (firm_edrpou, date) VALUES (?, ?)", 
                data
            )
            conn.commit()

    return items, "OK"

def is_history_empty():
    """Проверяет, пустая ли таблица истории."""
    if not os.path.exists(DB_FILE): return True
    with sqlite3.connect(DB_FILE) as conn:
        count = conn.execute("SELECT count(*) FROM history").fetchone()[0]
    return count == 0

# --- ХЕНДЛЕРЫ КОМАНД ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    added = manage_subscriber(update.effective_chat.id, "add")
    msg = "✅ Вы подписались на рассылку." if added else "ℹ️ Вы уже подписаны."
    
    await update.message.reply_text(
        f"{msg}\n\n"
        "<b>Команды бота:</b>\n"
        "/check — Проверить новых банкротов\n"
        "/find &lt;код&gt; — Найти компанию по коду (в базе)\n" 
        "/addcompany &lt;код&gt; — Добавить компанию в мониторинг\n"
        "/update — Скачать свежую базу\n"
        "/clear_history — Очистить память\n"
        "/stop — Отписаться\n"
        "/menu — Список команд",
        parse_mode='HTML'
    )

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "Доступны следующие команды бота." if added else "ℹ️ Вы уже подписаны."
    
    await update.message.reply_text(
        f"{msg}\n\n"
        "<b>Команды бота:</b>\n"
        "/check — Проверить новых банкротов\n"
        "/find &lt;код&gt; — Найти компанию по коду (в базе)\n" 
        "/addcompany &lt;код&gt; — Добавить компанию в мониторинг\n"
        "/update — Скачать свежую базу\n"
        "/clear_history — Очистить память\n"
        "/stop — Отписаться",
        parse_mode='HTML'
    )

async def add_company_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет компанию в список мониторинга."""
    if not context.args:
        await update.message.reply_text("Укажите код: `/addcompany 12345678`", parse_mode='Markdown')
        return
    
    code = context.args[0].strip()
    
    if not code.isdigit():
        await update.message.reply_text("❌ Код должен состоять только из цифр.")
        return
        
    if add_monitored_code(code):
        await update.message.reply_text(f"✅ Компания с кодом <b>{code}</b> добавлена в список мониторинга.", parse_mode='HTML')
    else:
        await update.message.reply_text(f"ℹ️ Компания с кодом <b>{code}</b> уже есть в списке.", parse_mode='HTML')

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    removed = manage_subscriber(update.effective_chat.id, "remove")
    msg = "🔕 Вы отписались." if removed else "ℹ️ Вы не были подписаны."
    await update.message.reply_text(msg)

async def clear_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if os.path.exists(DB_FILE):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("DELETE FROM history")
            conn.commit()
    await update.message.reply_text("🧹 История очищена. Команда /check теперь покажет полный список за 2025 год.")

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Проверяю...")
    
    first_run = await asyncio.to_thread(is_history_empty)
    items, msg = await asyncio.to_thread(get_bankruptcies, save_to_history=True, ignore_history=False)
    
    if not items:
        await update.message.reply_text("✅ Новых банкротств не найдено.")
        return

    if first_run:
        header = f"📋 <b>ПОЛНЫЙ СПИСОК (Первый запуск, {len(items)} шт):</b>"
    else:
        header = f"🚨 <b>НОВЫЕ БАНКРОТСТВА ({len(items)} шт):</b>"

    text = f"{header}\n\n"
    for index, i in enumerate(items, 1):
        safe_name = html.escape(i['name'])
        text += f"{index}. 🆔 <b>{i['code']}</b>\n🏢 {safe_name}\n📅 {i['date']}\n────────────────\n"
    
    if len(text) > 4000:
        for x in range(0, len(text), 4000):
            await update.message.reply_text(text[x:x+4000], parse_mode='HTML')
    else:
        await update.message.reply_text(text, parse_mode='HTML')

async def find_one(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ищет конкретный код, игнорируя историю."""
    if not context.args:
        await update.message.reply_text("Укажите код: `/find 30991664`", parse_mode='Markdown')
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
    await update.message.reply_text("⏳ Обновляю базу (это может занять 1-2 минуты)...")
    res, msg = await asyncio.to_thread(update_database_logic)
    
    if res:
        await update.message.reply_text("✅ База обновлена. Запускаю проверку...")
        await check_command(update, context)
    else:
        await update.message.reply_text(f"❌ {msg}")

# --- ЕЖЕДНЕВНАЯ ЗАДАЧА ---

async def daily_routine(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Start daily routine")
    
    res, msg = await asyncio.to_thread(update_database_logic)
    if not res:
        logging.error(f"Daily update failed: {msg}")
        return

    items, _ = await asyncio.to_thread(get_bankruptcies, save_to_history=True, ignore_history=False)
    
    is_monday = (datetime.datetime.now().weekday() == 0)
    message = None
    
    if items:
        message = f"🚨 <b>СВЕЖИЕ БАНКРОТСТВА ({len(items)}):</b>\n\n"
        for index, i in enumerate(items, 1):
            safe_name = html.escape(i['name'])
            message += f"{index}. 🆔 <b>{i['code']}</b>\n🏢 {safe_name}\n📅 {i['date']}\n────────────────\n"
    elif is_monday:
        message = "👋 <b>Понедельник.</b>\nБот работает штатно. База обновлена, новых банкротов из вашего списка не найдено."
    
    if message:
        for chat_id in get_subscribers():
            try:
                await context.bot.send_message(chat_id, message, parse_mode='HTML')
            except Exception as e:
                logging.error(f"Send error {chat_id}: {e}")

# --- ЗАПУСК ---

if __name__ == '__main__':
    if not TOKEN:
        print("CRITICAL: BOT_TOKEN not found in .env")
        exit()
    
    init_db()
    
    app = ApplicationBuilder().token(TOKEN).build()
    
    jq = app.job_queue
    kyiv_tz = pytz.timezone('Europe/Kiev')
    jq.run_daily(daily_routine, time=datetime.time(hour=9, minute=0, tzinfo=kyiv_tz))
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("check", check_command))
    app.add_handler(CommandHandler("find", find_one))
    app.add_handler(CommandHandler("addcompany", add_company_command))
    app.add_handler(CommandHandler("update", manual_update))
    app.add_handler(CommandHandler("clear_history", clear_history_command))
     app.add_handler(CommandHandler("menu", menu))

    print("Bot is running...")
    app.run_polling()