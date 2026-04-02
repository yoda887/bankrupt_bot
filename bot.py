import logging
import os
import requests
import pandas as pd
import datetime
import pytz
import asyncio
import sqlite3
import html
import time

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

# Настройка логирования (ФАЙЛ + КОНСОЛЬ)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'), # Логи в файл
        logging.StreamHandler()          # Логи в консоль
    ]
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

        # 5. Таблиця санкцій
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sanctions (
                sid INTEGER PRIMARY KEY,
                name TEXT,
                status TEXT,
                reg_id TEXT,
                tax_id TEXT
            )
        """)

        # 6. Таблиця історії повідомлень про санкції (щоб не спамити щодня)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sent_history_sanctions (
                chat_id INTEGER,
                firm_edrpou TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, firm_edrpou)
            )
        """)
        
        # МИГРАЦИЯ
        cursor.execute("INSERT OR IGNORE INTO users (chat_id, is_active) SELECT DISTINCT chat_id, 1 FROM subscriptions")
        conn.commit()

# --- ЯДРО: ОБНОВЛЕНИЕ БАЗЫ (ГЛОБАЛЬНОЕ) ---

def update_database_logic():
    """Скачивает CSV и обновляет общую таблицу bankrupts."""
    logging.info("Начало скачивания базы...")

    # 1. Получаем ссылку на файл
    try:
        api_url = 'https://data.gov.ua/api/3/action/package_show?id=544d4dad-0b6d-4972-b0b8-fb266829770f'
        resp = requests.get(api_url, timeout=15).json()
        if resp.get('success'):
            resource_url = resp['result']['resources'][-1]['url']
        else:
            resource_url = 'https://data.gov.ua/dataset/544d4dad-0b6d-4972-b0b8-fb266829770f/resource/deb76481-a6c8-4a45-ae6c-f02aa87e9f4a/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
    except Exception as e:
        return False, f"Ошибка API: {e}"

    csv_file = "temp_bankrupt.csv"
    download_success = False
    last_error = ""

    # 2. Попытка скачивания с повторами (Retries)
    for attempt in range(1, 4): # 3 попытки
        try:
            logging.info(f"⬇️ Попытка скачивания {attempt}/3...")
            with requests.get(resource_url, stream=True, timeout=180) as r:
                r.raise_for_status() # Проверка на ошибки 404, 500 и т.д.
                with open(csv_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            download_success = True
            logging.info("✅ Файл успешно скачан.")
            break # Выход из цикла, если успешно
        except Exception as e:
            last_error = str(e)
            logging.warning(f"⚠️ Ошибка при скачивании (попытка {attempt}): {e}")
            if os.path.exists(csv_file): 
                os.remove(csv_file) # Удаляем битый файл
            time.sleep(10) # Ждем 10 секунд перед следующей попыткой

    if not download_success:
        return False, f"Не удалось скачать файл после 3 попыток. Последняя ошибка: {last_error}"

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

def update_sanctions_logic():
    """Завантажує CSV-файли санкцій через ScraperAPI для обходу Cloudflare."""
    logging.info("Початок оновлення бази санкцій (через ScraperAPI)...")
    
    # ВСТАВТЕ ВАШ КЛЮЧ SCRAPER API СЮДИ:
    SCRAPER_API_KEY = "997574aa931452b215d42368a1bbbb82"
    
    urls = [
        "https://drs.nsdc.gov.ua/registry-api/subjects/export/legal/csv?lang=uk",
        "https://drs.nsdc.gov.ua/registry-api/subjects/export/individual/csv?lang=uk"
    ]
    
    all_data = []
    
    for target_url in urls:
        csv_file = f"temp_sanctions_{urls.index(target_url)}.csv"
        try:
            logging.info(f"Завантажуємо CSV через проксі: {target_url}")
            
            # Відправляємо запит до ScraperAPI, а він вже сам стукає до РНБО
            payload = {
                'api_key': SCRAPER_API_KEY, 
                'url': target_url, 
                'keep_headers': 'true'
            }
            
            response = requests.get('http://api.scraperapi.com', params=payload, stream=True, timeout=300)
            
            if response.status_code == 200:
                with open(csv_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Читаємо завантажений CSV
                df = pd.read_csv(csv_file, sep=',', encoding='utf-8', on_bad_lines="skip", dtype=str)
                
                cols_to_keep = ['sid', 'name', 'status', 'reg_id', 'tax_id']
                existing_cols = [c for c in cols_to_keep if c in df.columns]
                
                df_filtered = df[existing_cols].copy()
                all_data.append(df_filtered)
                logging.info(f"Успішно оброблено файл (рядків: {len(df_filtered)})")
            else:
                logging.error(f"Помилка ScraperAPI! Код: {response.status_code}. Текст: {response.text[:100]}")
                
        except Exception as e:
            logging.error(f"Помилка завантаження {target_url}: {e}")
        finally:
            if os.path.exists(csv_file): 
                os.remove(csv_file)
            
    if not all_data:
        return False, "Не вдалося завантажити санкції. Перевірте ліміти ScraperAPI або логи."
        
    try:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.fillna('', inplace=True)
        
        with sqlite3.connect(DB_FILE) as conn:
            final_df.to_sql('sanctions', conn, if_exists='replace', index=False)
            
        logging.info(f"База санкцій успішно оновлена. Всього записів: {len(final_df)}")
        return True, f"База санкцій оновлена (записів: {len(final_df)})."
    except Exception as e:
        logging.error(f"Помилка збереження санкцій в БД: {e}")
        return False, f"Помилка запису в БД: {e}"

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

def check_user_sanctions(chat_id, save_history=True):
    """Перевіряє базу санкцій по підписках користувача."""
    if not os.path.exists(DB_FILE): return []

    new_sanctions = []
    import re
    
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        user_codes = cursor.execute("SELECT firm_edrpou FROM subscriptions WHERE chat_id = ?", (chat_id,)).fetchall()
        if not user_codes: return []

        for (code,) in user_codes:
            if save_history:
                seen = cursor.execute("SELECT 1 FROM sent_history_sanctions WHERE chat_id = ? AND firm_edrpou = ?", (chat_id, code)).fetchone()
                if seen: continue 
                
            # Пошук у полях reg_id та tax_id
            matches = cursor.execute("""
                SELECT name, status, reg_id, tax_id 
                FROM sanctions 
                WHERE reg_id LIKE ? OR tax_id LIKE ?
            """, (f"%{code}%", f"%{code}%")).fetchall()
            
            for name, status, reg_id, tax_id in matches:
                # Об'єднуємо поля для перевірки і шукаємо код як ОДНЕ ціле число
                reg_str = str(reg_id) + " " + str(tax_id)
                if re.search(r'\b' + re.escape(code) + r'\b', reg_str):
                    new_sanctions.append({
                        "code": code,
                        "name": name,
                        "status": status
                    })
                    
                    if save_history:
                        cursor.execute("INSERT OR IGNORE INTO sent_history_sanctions (chat_id, firm_edrpou) VALUES (?, ?)", (chat_id, code))
                    break # Переходимо до наступного коду

        if save_history and new_sanctions:
            conn.commit()
            
    return new_sanctions

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
    await update.message.reply_text("🔍 Перевіряю ваш список на банкрутства та санкції...")
    chat_id = update.effective_chat.id
    
    b_items, b_msg = await asyncio.to_thread(check_user_subscriptions, chat_id, save_history=True)
    s_items = await asyncio.to_thread(check_user_sanctions, chat_id, save_history=True)
    
    if not b_items and not s_items:
        await update.message.reply_text("✅ По вашому списку нових подій немає.")
        return

    text = ""
    if b_items:
        text += f"🚨 <b>НОВІ БАНКРУТСТВА ({len(b_items)}):</b>\n\n"
        for i, item in enumerate(b_items, 1):
            text += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {html.escape(item['name'])}\n📅 {item['date']}\n\n"
            
    if s_items:
        text += f"🛑 <b>НОВІ САНКЦІЇ РНБО ({len(s_items)}):</b>\n\n"
        for i, item in enumerate(s_items, 1):
            text += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {html.escape(item['name'])}\n⚠️ Статус: {item['status']}\n\n"
    
    await update.message.reply_text(text, parse_mode='HTML')

async def manual_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Оновлюю бази даних (Банкрутства та Санкції)... Це може зайняти хвилину.")
    b_res, b_msg = await asyncio.to_thread(update_database_logic)
    s_res, s_msg = await asyncio.to_thread(update_sanctions_logic)
    
    if b_res or s_res:
        await update.message.reply_text("✅ Бази оновлено. Перевіряю ваші підписки...")
        await check_command(update, context)
    else:
        await update.message.reply_text(f"❌ Помилка оновлення.\nБанкрутства: {b_msg}\nСанкції: {s_msg}")

# --- ФУНКЦИИ ДЛЯ CONVERSATION HANDLER (/find) ---

async def find_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Шаг 1: Пользователь вводит /find, бот просит код."""
    await update.message.reply_text(
        "🔎 Введіть код (ЄДРПОУ або ІПН) для пошуку або для скасування введіть /cancel"
    )
    return FIND_WAITING_CODE

async def find_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    
    def db_search(c):
        res = ""
        if not os.path.exists(DB_FILE): return "База не скачана."
        import re
        with sqlite3.connect(DB_FILE) as conn:
            # 1. Пошук банкрутств
            b_rows = conn.execute("SELECT firm_name, date FROM bankrupts WHERE firm_edrpou = ?", (c,)).fetchall()
            if b_rows:
                res += f"🚨 <b>БАНКРУТСТВО:</b>\n"
                for n, d in b_rows: 
                    res += f"- {html.escape(n)} ({d})\n"
            else:
                res += "✅ В реєстрі банкрутств не знайдено.\n"
                
            # 2. Пошук санкцій
            s_rows = conn.execute("SELECT name, status, reg_id, tax_id FROM sanctions WHERE reg_id LIKE ? OR tax_id LIKE ?", (f"%{c}%", f"%{c}%")).fetchall()
            s_found = False
            for name, status, reg_id, tax_id in s_rows:
                reg_str = str(reg_id) + " " + str(tax_id)
                if re.search(r'\b' + re.escape(c) + r'\b', reg_str):
                    if not s_found:
                        res += f"\n🛑 <b>САНКЦІЇ РНБО:</b>\n"
                        s_found = True
                    res += f"- {html.escape(name)} (Статус: {status})\n"
                    
            if not s_found:
                res += "\n✅ В санкційних списках не знайдено.\n"
                
        return f"🔎 <b>Результати по коду {c}:</b>\n\n" + res

    await update.message.reply_text("⏳ Шукаю в базах банкрутств та санкцій...")
    result = await asyncio.to_thread(db_search, code)
    await update.message.reply_text(result, parse_mode='HTML')
    return ConversationHandler.END

async def cancel_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общая функция отмены для всех диалогов."""
    await update.message.reply_text("❌ Операція скасована.")
    return ConversationHandler.END

# --- ЕЖЕДНЕВНАЯ ЗАДАЧА (МАССОВАЯ РАССЫЛКА) ---

async def daily_routine(context: ContextTypes.DEFAULT_TYPE):
    logging.info("Start daily routine")
    
    # 1. Оновлення баз
    b_res, b_msg = await asyncio.to_thread(update_database_logic)
    if not b_res: logging.error(f"Помилка банкрутств: {b_msg}")
        
    s_res, s_msg = await asyncio.to_thread(update_sanctions_logic)
    if not s_res: logging.error(f"Помилка санкцій: {s_msg}")

    users = await asyncio.to_thread(db_get_active_users)
    is_monday = (datetime.datetime.now().weekday() == 0)
    
    for chat_id in users:
        try:
            b_items, _ = await asyncio.to_thread(check_user_subscriptions, chat_id, save_history=True)
            s_items = await asyncio.to_thread(check_user_sanctions, chat_id, save_history=True)
            
            message = ""
            
            if b_items:
                message += f"🚨 <b>НОВІ БАНКРУТСТВА ({len(b_items)}):</b>\n\n"
                for i, item in enumerate(b_items, 1):
                    message += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {html.escape(item['name'])}\n📅 {item['date']}\n\n"
                    
            if s_items:
                message += f"🛑 <b>НОВІ САНКЦІЇ РНБО ({len(s_items)}):</b>\n\n"
                for i, item in enumerate(s_items, 1):
                    message += f"{i}. 🆔 <b>{item['code']}</b>\n🏢 {html.escape(item['name'])}\n⚠️ Статус: {item['status']}\n\n"

            if not message and is_monday:
                message = "👋 <b>Понеділок.</b>\nБот працює. По вашому списку компаній нових подій немає."
            
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
    app.add_handler(CommandHandler("import_txt", import_txt_command)) # <-- Новая команда
    
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
