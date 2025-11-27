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

# Дата отсечения (старые банкротства игнорируем глобально)
GLOBAL_START_DATE = datetime.datetime.strptime("01.01.2025", "%d.%m.%Y").date()

# --- РАБОТА С ФАЙЛАМИ (TXT) ---

def get_monitored_codes():
    """Читает список кодов для мониторинга."""
    if not os.path.exists(COMPANIES_FILE): return []
    with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def get_subscribers():
    """Читает список подписчиков."""
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

# --- ИНИЦИАЛИЗАЦИЯ БАЗЫ ---

def init_db():
    """Создает таблицы, если их нет."""
    with sqlite3.connect(DB_FILE) as conn:
        # Таблица для свежих данных (перезаписывается при обновлении)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bankrupts (
                firm_edrpou TEXT,
                firm_name TEXT,
                date TEXT
            )
        """)
        # Таблица истории (что мы уже видели/отправили)
        # Храним пару (код, дата), чтобы различать разные дела по одной фирме
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

# --- ЯДРО: ОБНОВЛЕНИЕ И ПОИСК ---

def update_database_logic():
    """Скачивает CSV и обновляет таблицу bankrupts."""
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
        # Загружаем с пропуском ошибок кодировки
        df = pd.read_csv(csv_file, sep=None, engine="python", on_bad_lines="skip", encoding="utf-8", encoding_errors='replace')
        
        # Чистка
        df.columns = df.columns.str.strip()
        df['firm_edrpou'] = df['firm_edrpou'].astype(str).str.strip()
        df['firm_name'] = df['firm_name'].astype(str).str.strip()
        df['date'] = df['date'].astype(str).str.strip()
        
        with sqlite3.connect(DB_FILE) as conn:
            # Полная перезапись таблицы банкротов
            df.to_sql('bankrupts', conn, if_exists='replace', index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_edrpou ON bankrupts (firm_edrpou)")
            
        logging.info("Таблица bankrupts обновлена.")
        return True, "База обновлена."
    except Exception as e:
        return False, f"Ошибка парсинга: {e}"
    finally:
        if os.path.exists(csv_file): os.remove(csv_file)

def get_new_items(save_to_history=True):
    """
    1. Ищет совпадения по списку companies.txt.
    2. Фильтрует по дате > 2025.
    3. Фильтрует по таблице history (исключает увиденные).
    4. Если save_to_history=True, записывает найденное в историю.
    """
    codes = get_monitored_codes()
    if not codes:
        return [], "Список companies.txt пуст."

    if not os.path.exists(DB_FILE):
        return [], "База данных не найдена."

    new_items = []
    
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        # 1. Проверяем, пуста ли история (для первого запуска)
        history_count = cursor.execute("SELECT count(*) FROM history").fetchone()[0]
        history_is_empty = (history_count == 0)

        # 2. Ищем все совпадения по кодам
        placeholders = ','.join('?' for _ in codes)
        query = f"SELECT firm_edrpou, firm_name, date FROM bankrupts WHERE firm_edrpou IN ({placeholders})"
        cursor.execute(query, codes)
        rows = cursor.fetchall()

        for code, name, date_str in rows:
            # Фильтр по дате (парсинг)
            try:
                date_obj = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                if date_obj <= GLOBAL_START_DATE:
                    continue
            except: continue

            # Фильтр по истории
            # Если история НЕ пуста -> проверяем, видели ли мы запись
            if not history_is_empty:
                seen = cursor.execute(
                    "SELECT 1 FROM history WHERE firm_edrpou = ? AND date = ?", 
                    (code, date_str)
                ).fetchone()
                if seen:
                    continue # Уже видели, пропускаем

            # Если дошли сюда - это новая запись (или первый запуск)
            new_items.append({
                "code": code,
                "name": name,
                "date": date_str,
                "date_obj": date_obj
            })

        # Сортировка
        new_items.sort(key=lambda x: x["date_obj"])

        # 3. Сохраняем в историю (если нужно)
        if save_to_history and new_items:
            data_to_insert = [(item['code'], item['date']) for item in new_items]
            cursor.executemany(
                "INSERT OR IGNORE INTO history (firm_edrpou, date) VALUES (?, ?)", 
                data_to_insert
            )
            conn.commit()

    return new_items, "OK"

# --- ХЕНДЛЕРЫ ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if manage_subscriber(update.effective_chat.id, "add"):
        await update.message.reply_text("✅ Вы подписались на ежедневную рассылку.")
    else:
        await update.message.reply_text("ℹ️ Вы уже подписаны.")
    
    await update.message.reply_text(
        "<b>Команды бота:</b>\n"
        "/check — Проверить наличие НОВЫХ банкротств (с учетом истории)\n"
        "/find <code> — Найти фирму по коду (даже старую)\n"
        "/update — Принудительно скачать новую базу\n"
        "/clear_history — Очистить историю просмотров (бот покажет всё заново)\n"
        "/stop — Отписаться от рассылки",
        parse_mode='HTML'
    )

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if manage_subscriber(update.effective_chat.id, "remove"):
        await update.message.reply_text("🔕 Вы отписались от рассылки.")
    else:
        await update.message.reply_text("ℹ️ Вы не были подписаны.")

async def clear_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM history")
        conn.commit()
    await update.message.reply_text("🧹 История просмотров очищена. Следующая проверка покажет всех банкротов за 2025 год.")

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает только новые, как и утренняя рассылка."""
    await update.message.reply_text("🔍 Ищу новые записи...")
    
    # Запускаем в потоке
    items, msg = await asyncio.to_thread(get_new_items, save_to_history=True)
    
    if not items:
        await update.message.reply_text("✅ Новых банкротств не найдено (все просмотрены).")
        return

    text = f"⚠️ <b>НОВЫЕ БАНКРОТЫ ({len(items)}):</b>\n\n"
    for i in items:
        text += f"🏢 <b>{i['name']}</b>\n🆔 {i['code']}\n📅 {i['date']}\n────────────────\n"
    
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
        for n, d in rows: res += f"\n- {n} ({d})"
        return res

    result = await asyncio.to_thread(db_search, code)
    await update.message.reply_text(result, parse_mode='HTML')

async def manual_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Скачиваю базу...")
    res, msg = await asyncio.to_thread(update_database_logic)
    await update.message.reply_text(f"{'✅' if res else '❌'} {msg}")
    if res:
        await check_command(update, context)

# --- ЕЖЕДНЕВНАЯ ЗАДАЧА ---

async def daily_routine(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Start daily routine")
    
    # 1. Обновляем базу (качаем файл)
    res, msg = await asyncio.to_thread(update_database_logic)
    if not res:
        logging.error(f"Daily update failed: {msg}")
        return # Если база не скачалась, лучше промолчать, чем спамить ошибками

    # 2. Ищем НОВЫЕ
    items, _ = await asyncio.to_thread(get_new_items, save_to_history=True)
    
    # 3. Логика отправки
    is_monday = (datetime.datetime.now().weekday() == 0) # 0 = Понедельник
    
    if items:
        # Если есть новые - шлем всегда
        message = f"🚨 <b>СВЕЖИЕ БАНКРОТСТВА ({len(items)}):</b>\n\n"
        for i in items:
            message += f"🏢 <b>{i['name']}</b>\n🆔 {i['code']}\n📅 {i['date']}\n────────────────\n"
    elif is_monday:
        # Если новых нет, но понедельник - шлем пульс
        message = "👋 <b>Понедельник.</b>\nБот работает штатно. База обновлена, новых банкротов из вашего списка не найдено."
    else:
        # Если новых нет и не понедельник - молчим
        return

    # Рассылка
    for chat_id in get_subscribers():
        try:
            await context.bot.send_message(chat_id, message, parse_mode='HTML')
        except Exception as e:
            logging.error(f"Send error {chat_id}: {e}")

# --- ЗАПУСК ---

if __name__ == '__main__':
    if not TOKEN: exit("NO TOKEN")
    
    # Инициализация БД при старте
    init_db()
    
    app = ApplicationBuilder().token(TOKEN).build()
    
    # Планировщик
    jq = app.job_queue
    tz = pytz.timezone('Europe/Kiev')
    # Каждый день в 09:00
    jq.run_daily(daily_routine, time=datetime.time(hour=9, minute=0, tzinfo=tz))
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("check", check_command))
    app.add_handler(CommandHandler("find", find_one))
    app.add_handler(CommandHandler("update", manual_update))
    app.add_handler(CommandHandler("clear_history", clear_history_command))

    print("Smart Bot Started...")
    app.run_polling()