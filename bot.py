import logging
import os
import requests
import pandas as pd
import datetime
import pytz
import asyncio
import sqlite3
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

# Файлы
SUBSCRIBERS_FILE = "subscribers.txt"
COMPANIES_FILE = "companies.txt"
DB_FILE = "bankrupt.db"

# --- РАБОТА С ФАЙЛАМИ ---

def get_monitored_codes():
    """Читает список кодов для мониторинга."""
    if not os.path.exists(COMPANIES_FILE): return []
    with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        # Чистим от пробелов и пустых строк
        return [line.strip() for line in f if line.strip()]

def get_subscribers():
    """Читает список подписчиков."""
    if not os.path.exists(SUBSCRIBERS_FILE): return set()
    with open(SUBSCRIBERS_FILE, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def add_subscriber(chat_id):
    """Добавляет подписчика."""
    subs = get_subscribers()
    if str(chat_id) not in subs:
        with open(SUBSCRIBERS_FILE, 'a') as f:
            f.write(f"{chat_id}\n")

# --- ФУНКЦИИ БАЗЫ ДАННЫХ (SQL) ---

def update_database_logic():
    """Скачивает CSV и пересоздает SQL базу. Возвращает True/False и сообщение."""
    logging.info("Начало обновления базы данных...")
    
    # 1. Получение ссылки
    try:
        api_url = 'https://data.gov.ua/api/3/action/package_show?id=544d4dad-0b6d-4972-b0b8-fb266829770f'
        resp = requests.get(api_url, timeout=10).json()
        if resp.get('success'):
            resource_url = resp['result']['resources'][-1]['url']
        else:
            resource_url = 'https://data.gov.ua/dataset/544d4dad-0b6d-4972-b0b8-fb266829770f/resource/deb76481-a6c8-4a45-ae6c-f02aa87e9f4a/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
    except Exception as e:
        return False, f"Ошибка API: {e}"

    # 2. Скачивание
    csv_file = "temp_bankrupt.csv"
    try:
        r = requests.get(resource_url, stream=True, timeout=120)
        with open(csv_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        return False, f"Ошибка скачивания: {e}"

    # 3. Импорт в SQL
    try:
        # Читаем CSV
        df = pd.read_csv(csv_file, sep=None, engine="python", on_bad_lines="skip", encoding="utf-8", encoding_errors='replace')
        
        # Чистим названия
        df.columns = df.columns.str.strip()
        df['firm_edrpou'] = df['firm_edrpou'].astype(str).str.strip()
        df['firm_name'] = df['firm_name'].astype(str).str.strip()
        df['date'] = df['date'].astype(str).str.strip()
        
        # Пишем в SQLite
        with sqlite3.connect(DB_FILE) as conn:
            df.to_sql('bankrupts', conn, if_exists='replace', index=False)
            # Создаем индексы для скорости
            conn.execute("CREATE INDEX IF NOT EXISTS idx_edrpou ON bankrupts (firm_edrpou)")
            
        logging.info("База обновлена.")
        return True, "База данных успешно обновлена."

    except Exception as e:
        return False, f"Ошибка обработки данных: {e}"
    finally:
        if os.path.exists(csv_file):
            os.remove(csv_file)

def check_watchlist_in_db():
    """Проверяет список companies.txt по локальной базе SQL."""
    if not os.path.exists(DB_FILE):
        return "⚠️ База данных пуста. Нажмите /update, чтобы скачать данные."

    codes = get_monitored_codes()
    if not codes:
        return "ℹ️ Список мониторинга (companies.txt) пуст."

    date_threshold = datetime.datetime.strptime("01.01.2025", "%d.%m.%Y").date()
    results = []

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # SQL-магия: формируем запрос с множеством "OR" или "IN"
            placeholders = ','.join('?' for _ in codes)
            query = f"SELECT firm_edrpou, firm_name, date FROM bankrupts WHERE firm_edrpou IN ({placeholders})"
            
            cursor.execute(query, codes)
            rows = cursor.fetchall()

            for code, name, date_str in rows:
                try:
                    date_obj = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                    if date_obj > date_threshold:
                        results.append({
                            "code": code,
                            "name": name,
                            "date": date_str,
                            "date_obj": date_obj
                        })
                except: continue
                
    except Exception as e:
        return f"Ошибка SQL: {e}"

    # Сортировка и вывод
    results.sort(key=lambda x: x["date_obj"])
    
    if not results:
        return "✅ В списке мониторинга банкротов за 2025 год не найдено."

    msg = f"⚠️ <b>НАЙДЕНЫ БАНКРОТЫ ({len(results)}):</b>\n\n"
    for i, entry in enumerate(results, 1):
        msg += f"{i}. <b>{entry['code']}</b>: {entry['name']}\n📅 {entry['date']}\n_____________________\n"
    
    return msg

# --- ХЕНДЛЕРЫ КОМАНД ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_subscriber(update.effective_chat.id)
    await update.message.reply_text(
        "🤖 <b>Бот Мониторинга Банкротов</b>\n\n"
        "Я работаю на базе SQL для высокой скорости.\n\n"
        "<b>Команды:</b>\n"
        "/check — Проверить ВЕСЬ список мониторинга (Мгновенно)\n"
        "/find <code> — Найти конкретную фирму по коду\n"
        "/update — Принудительно скачать свежую базу (1-3 мин)\n"
        "/help — Показать это меню",
        parse_mode='HTML'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/check — Быстрая проверка вашего списка по сохраненной базе.\n"
        "/find 12345678 — Поиск любой компании по коду.\n"
        "/update — Обновить базу данных с сайта data.gov.ua.\n"
        "/start — Подписаться на утреннюю рассылку."
    )

async def check_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Быстрая проверка списка."""
    await update.message.reply_text("🔍 Проверяю список по базе...")
    report = await asyncio.to_thread(check_watchlist_in_db)
    await update.message.reply_text(report, parse_mode='HTML')

async def find_one(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск одной фирмы."""
    if not context.args:
        await update.message.reply_text("Укажите код: `/find 30991664`", parse_mode='Markdown')
        return
    
    code = context.args[0].strip()
    
    def db_query(c):
        if not os.path.exists(DB_FILE): return "База не найдена. Нажмите /update"
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute("SELECT firm_name, date FROM bankrupts WHERE firm_edrpou = ?", (c,)).fetchall()
        if not rows: return f"✅ Код {c}: Банкротств не найдено."
        res = f"⚠️ <b>Код {c}:</b>\n"
        for n, d in rows: res += f"- {n} ({d})\n"
        return res

    result = await asyncio.to_thread(db_query, code)
    await update.message.reply_text(result, parse_mode='HTML')

async def manual_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Полное обновление."""
    await update.message.reply_text("⏳ Скачиваю новый реестр... Ждите.")
    
    # 1. Обновляем базу
    success, msg = await asyncio.to_thread(update_database_logic)
    if not success:
        await update.message.reply_text(f"❌ {msg}")
        return
        
    await update.message.reply_text("✅ База обновлена. Проверяю ваш список...")
    
    # 2. Проверяем список
    report = await asyncio.to_thread(check_watchlist_in_db)
    await update.message.reply_text(report, parse_mode='HTML')

# --- АВТОМАТИЧЕСКАЯ ЗАДАЧА ---

async def daily_task(context: ContextTypes.DEFAULT_TYPE):
    """Запускается каждое утро."""
    success, msg = await asyncio.to_thread(update_database_logic)
    if not success:
        logging.error(f"Update failed: {msg}")
        return # Можно отправить админу сообщение об ошибке

    report = await asyncio.to_thread(check_watchlist_in_db)
    
    # Рассылка
    subs = get_subscribers()
    for chat_id in subs:
        try:
            await context.bot.send_message(chat_id, f"🌅 <b>Утренний отчет:</b>\n{report}", parse_mode='HTML')
        except Exception as e:
            logging.error(f"Send error {chat_id}: {e}")

# --- ЗАПУСК ---

if __name__ == '__main__':
    if not TOKEN: exit("NO TOKEN FOUND")
    
    app = ApplicationBuilder().token(TOKEN).build()
    
    # Ежедневная задача (09:00 Киев)
    job_queue = app.job_queue
    kyiv_tz = pytz.timezone('Europe/Kiev')
    job_queue.run_daily(daily_task, time=datetime.time(hour=9, minute=0, tzinfo=kyiv_tz))
    
    # Регистрация команд
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("check", check_list))   # Старая добрая команда (быстрая)
    app.add_handler(CommandHandler("find", find_one))      # Поиск одного
    app.add_handler(CommandHandler("update", manual_update)) # Полное обновление

    print("Бот запущен!")
    app.run_polling()