import logging
import os
import requests
import pandas as pd
import datetime
import pytz
import asyncio
import json
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv('BOT_TOKEN')

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Файлы данных
SUBSCRIBERS_FILE = "subscribers.txt"
COMPANIES_FILE = "companies.txt"
HISTORY_FILE = "history.json"  # <--- НОВЫЙ ФАЙЛ ДЛЯ ИСТОРИИ

# Константы
DATASET_ID = '544d4dad-0b6d-4972-b0b8-fb266829770f'
BACKUP_URL = 'https://data.gov.ua/dataset/544d4dad-0b6d-4972-b0b8-fb266829770f/resource/deb76481-a6c8-4a45-ae6c-f02aa87e9f4a/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
DAYS_TO_CHECK = 365 

# --- ФУНКЦИИ РАБОТЫ С ИСТОРИЕЙ (НОВОЕ) ---

def load_history():
    """Загружает список уже просмотренных банкротств."""
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка чтения истории: {e}")
        return []

def save_history(history_list):
    """Сохраняет обновленную историю."""
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history_list, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Ошибка записи истории: {e}")

# --- ФУНКЦИИ РАБОТЫ С ДАННЫМИ ---

def get_monitored_codes():
    if not os.path.exists(COMPANIES_FILE):
        return []
    try:
        with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
            codes = [line.strip() for line in f if line.strip()]
        return list(set(codes)) # Убираем дубликаты
    except Exception:
        return []

def get_subscribers():
    if not os.path.exists(SUBSCRIBERS_FILE):
        return set()
    try:
        with open(SUBSCRIBERS_FILE, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    except Exception:
        return set()

def add_subscriber(chat_id):
    subs = get_subscribers()
    if str(chat_id) not in subs:
        with open(SUBSCRIBERS_FILE, 'a') as f:
            f.write(f"{chat_id}\n")
        return True
    return False

def remove_subscriber(chat_id):
    subs = get_subscribers()
    if str(chat_id) in subs:
        subs.remove(str(chat_id))
        with open(SUBSCRIBERS_FILE, 'w') as f:
            f.write('\n'.join(subs) + '\n')
        return True
    return False

def get_resource_url():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        package_url = f'https://data.gov.ua/api/3/action/package_show?id={DATASET_ID}'
        response = requests.get(package_url, headers=headers, timeout=15, verify=False)
        data = response.json()
        if data.get('success'):
            return data['result']['resources'][-1]['url']
    except Exception:
        pass
    return BACKUP_URL

def check_bankruptcy_logic():
    """
    Основная логика: 
    1. Скачивает файл
    2. Ищет совпадения
    3. Фильтрует через history.json (только новые)
    """
    enterprise_codes = get_monitored_codes()
    if not enterprise_codes:
        return "⚠️ Файл companies.txt пуст или не найден."

    # Скачивание
    url = get_resource_url()
    local_filename = "bankruptcy_temp.csv"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=120, verify=False)
        response.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        return f"❌ Не удалось скачать реестр: {str(e)[:100]}"

    # Чтение
    data_df = None
    for enc in ["utf-8", "cp1251", "windows-1251", "latin-1"]:
        try:
            data_df = pd.read_csv(local_filename, sep=None, engine="python", on_bad_lines="skip", encoding=enc, encoding_errors='replace')
            break
        except:
            continue
    
    if data_df is None:
        if os.path.exists(local_filename): os.remove(local_filename)
        return "❌ Не удалось прочитать CSV (проблема с кодировкой)."

    # Очистка и поиск колонок
    data_df.columns = data_df.columns.str.strip()
    
    edrpou_col = next((col for col in data_df.columns if 'код' in col.lower() or 'edrpou' in col.lower()), 'firm_edrpou')
    name_col = next((col for col in data_df.columns if 'назва' in col.lower() or 'name' in col.lower()), data_df.columns[1])
    date_col = next((col for col in data_df.columns if 'дата' in col.lower() or 'date' in col.lower()), None)

    if edrpou_col not in data_df.columns or not date_col:
        if os.path.exists(local_filename): os.remove(local_filename)
        return "❌ Ошибка структуры файла (нет колонок кода или даты)."

    data_df['clean_code'] = data_df[edrpou_col].astype(str).str.strip()
    date_threshold = datetime.date.today() - datetime.timedelta(days=DAYS_TO_CHECK)

    # --- ФИЛЬТРАЦИЯ НОВЫХ ЗАПИСЕЙ ---
    
    seen_history = load_history() # Загружаем старые записи ["код_дата", "код_дата"...]
    history_set = set(seen_history) # Для быстрого поиска
    
    new_results = []
    new_history_entries = []

    for code in enterprise_codes:
        matches = data_df[data_df['clean_code'] == code]
        if not matches.empty:
            row = matches.iloc[0] # Берем последнюю запись
            
            date_val = str(row[date_col]).strip()
            if pd.isna(date_val) or date_val.lower() == 'nan': continue
            
            # Парсинг даты
            try:
                clean_date_str = date_val.split()[0]
                date_obj = datetime.datetime.strptime(clean_date_str, "%d.%m.%Y").date()
            except:
                continue

            # Проверяем дату
            if date_obj > date_threshold:
                # Генерируем уникальный ID для этой записи: "КОД_ДАТА"
                # Это позволит отличать разные дела по одной компании, если даты разные
                unique_id = f"{code}_{clean_date_str}"
                
                # ЕСЛИ ЭТОГО ID НЕТ В ИСТОРИИ -> ЭТО НОВОЕ!
                if unique_id not in history_set:
                    new_results.append({
                        "code": code,
                        "name": str(row[name_col]),
                        "date": clean_date_str,
                        "date_obj": date_obj
                    })
                    new_history_entries.append(unique_id)

    # Удаляем файл
    if os.path.exists(local_filename):
        os.remove(local_filename)

    # Если есть новые данные
    if new_results:
        # 1. Обновляем историю на диске
        seen_history.extend(new_history_entries)
        save_history(seen_history)
        
        # 2. Формируем сообщение
        new_results.sort(key=lambda x: x["date_obj"], reverse=True)
        message = f"🔥 <b>НОВЫЕ БАНКРОТСТВА ({len(new_results)})</b>\n\n"
        for i, entry in enumerate(new_results, 1):
            name = entry['name'][:100] + "..." if len(entry['name']) > 100 else entry['name']
            message += (
                f"{i}. <b>{entry['code']}</b>\n"
                f"🏢 {name}\n"
                f"📅 {entry['date']}\n"
                f"{'-'*20}\n"
            )
        return message
    else:
        return "✅ Новых банкротов не найдено (среди тех, кого вы еще не видели)."

# --- ОБРАБОТЧИКИ ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    add_subscriber(chat_id)
    await update.message.reply_text("🔔 Вы подписаны! Я буду присылать ТОЛЬКО новых банкротов.")

async def manual_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Ищу обновления в реестре...")
    report = await asyncio.to_thread(check_bankruptcy_logic)
    
    # Разбивка длинного сообщения
    if len(report) > 4000:
        for x in range(0, len(report), 4000):
            await update.message.reply_text(report[x:x+4000], parse_mode='HTML')
    else:
        await update.message.reply_text(report, parse_mode='HTML')

async def scheduled_check(context: ContextTypes.DEFAULT_TYPE):
    subs = get_subscribers()
    if not subs: return
    
    logger.info("Авто-проверка...")
    report = await asyncio.to_thread(check_bankruptcy_logic)
    
    # Если ничего нового ("✅ Новых банкротов не найдено..."), в авто-режиме молчим
    # Если хотите получать отчет "все ок" каждый день - уберите условие ниже
    if "✅" in report:
        return 

    for chat_id in subs:
        try:
            if len(report) > 4000:
                for x in range(0, len(report), 4000):
                    await context.bot.send_message(chat_id=chat_id, text=report[x:x+4000], parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id=chat_id, text=report, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка отправки {chat_id}: {e}")

async def cleandup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Скрытая команда очистки истории, если нужно перепроверить всё заново"""
    if os.path.exists(HISTORY_FILE):
        os.remove(HISTORY_FILE)
        await update.message.reply_text("🗑 История просмотров очищена! Следующая проверка покажет ВСЕХ банкротов как новых.")
    else:
        await update.message.reply_text("История уже пуста.")

# --- ЗАПУСК ---

if __name__ == '__main__':
    if not TOKEN:
        print("❌ Ошибка: Не задан BOT_TOKEN")
        exit()

    app = ApplicationBuilder().token(TOKEN).build()
    
    # Планировщик (9:00 Киев)
    jq = app.job_queue
    kyiv_tz = pytz.timezone('Europe/Kiev')
    jq.run_daily(scheduled_check, time=datetime.time(hour=9, minute=0, tzinfo=kyiv_tz))

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check", manual_check))
    app.add_handler(CommandHandler("reset", cleandup)) # Команда сброса "памяти"

    print("Бот запущен (режим: только новые)")
    app.run_polling()
