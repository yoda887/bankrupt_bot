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
HISTORY_FILE = "history.json"

# Константы
DATASET_ID = '544d4dad-0b6d-4972-b0b8-fb266829770f'
BACKUP_URL = 'https://data.gov.ua/dataset/544d4dad-0b6d-4972-b0b8-fb266829770f/resource/deb76481-a6c8-4a45-ae6c-f02aa87e9f4a/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
DAYS_TO_CHECK = 365 

# --- 1. ФУНКЦИИ РАБОТЫ С ИСТОРИЕЙ (ПАМЯТЬ БОТА) ---

def load_history():
    """Загружает список уже просмотренных уникальных ID."""
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def save_history(history_list):
    """Сохраняет обновленный список ID в файл."""
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history_list, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Ошибка записи истории: {e}")

# --- 2. ФУНКЦИИ РАБОТЫ С ПОДПИСЧИКАМИ И КОМПАНИЯМИ ---

def get_monitored_codes():
    if not os.path.exists(COMPANIES_FILE):
        return []
    try:
        with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
            # Читаем, чистим, убираем пустые
            codes = [line.strip() for line in f if line.strip()]
        return list(set(codes)) # Возвращаем уникальные
    except Exception as e:
        logger.error(f"Ошибка чтения companies.txt: {e}")
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

# --- 3. СЕТЕВЫЕ ФУНКЦИИ ---

def get_resource_url():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        package_url = f'https://data.gov.ua/api/3/action/package_show?id={DATASET_ID}'
        response = requests.get(package_url, headers=headers, timeout=15, verify=False)
        data = response.json()
        if data.get('success'):
            resources = data['result']['resources']
            if resources:
                return resources[-1]['url']
    except Exception as e:
        logger.warning(f"API ошибка: {e}")
    return BACKUP_URL

def download_file(url, filename):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        # Сначала пробуем с проверкой SSL
        response = requests.get(url, headers=headers, stream=True, timeout=120, verify=True)
        response.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except requests.exceptions.SSLError:
        # Если ошибка SSL, пробуем без проверки (для госсайтов)
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=120, verify=False)
            response.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            logger.error(f"Ошибка скачивания (без SSL): {e}")
            return False
    except Exception as e:
        logger.error(f"Ошибка скачивания: {e}")
        return False

# --- 4. ОСНОВНАЯ ЛОГИКА ---

def check_bankruptcy_logic():
    enterprise_codes = get_monitored_codes()
    if not enterprise_codes:
        return "⚠️ Файл companies.txt пуст или не найден."

    url = get_resource_url()
    local_filename = "bankruptcy_temp.csv"
    
    if not download_file(url, local_filename):
        return "❌ Не удалось скачать файл реестра."

    # Чтение с подбором кодировки
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

    # Очистка
    data_df.columns = data_df.columns.str.strip()
    
    # Поиск колонок
    edrpou_col = next((col for col in data_df.columns if 'код' in col.lower() or 'edrpou' in col.lower()), 'firm_edrpou')
    name_col = next((col for col in data_df.columns if 'назва' in col.lower() or 'name' in col.lower()), data_df.columns[1])
    date_col = next((col for col in data_df.columns if 'дата' in col.lower() or 'date' in col.lower()), None)

    if edrpou_col not in data_df.columns or not date_col:
        if os.path.exists(local_filename): os.remove(local_filename)
        return f"❌ Ошибка структуры файла. Найдены колонки: {list(data_df.columns)}"

    data_df['clean_code'] = data_df[edrpou_col].astype(str).str.strip()
    date_threshold = datetime.date.today() - datetime.timedelta(days=DAYS_TO_CHECK)

    # Фильтрация через историю
    seen_history = load_history()
    history_set = set(seen_history)
    new_results = []
    new_history_entries = []

    for code in enterprise_codes:
        matches = data_df[data_df['clean_code'] == code]
        if not matches.empty:
            row = matches.iloc[0]
            date_val = str(row[date_col]).strip()
            
            if pd.isna(date_val) or date_val.lower() == 'nan': continue
            
            try:
                clean_date_str = date_val.split()[0]
                date_obj = datetime.datetime.strptime(clean_date_str, "%d.%m.%Y").date()
            except:
                continue

            if date_obj > date_threshold:
                # Уникальный ключ: КОД + ДАТА
                unique_id = f"{code}_{clean_date_str}"
                
                if unique_id not in history_set:
                    new_results.append({
                        "code": code,
                        "name": str(row[name_col]),
                        "date": clean_date_str,
                        "date_obj": date_obj
                    })
                    new_history_entries.append(unique_id)

    if os.path.exists(local_filename):
        os.remove(local_filename)

    if new_results:
        # Сохраняем в историю
        seen_history.extend(new_history_entries)
        save_history(seen_history)
        
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
            message += (
                f"{i}. <b>Код:</b> {entry['code']}\n"
                f"🏢 <b>Компания:</b> {entry['name']}\n"
                f"📅 <b>Дата:</b> {entry['date']}\n"
                f"_____________________\n"
            )
        return message
    else:
        return "✅ Новых банкротов не найдено (среди тех, кого вы еще не видели)."

# --- 5. ОБРАБОТЧИКИ КОМАНД ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if add_subscriber(chat_id):
        await update.message.reply_text("👋 Вы подписаны! Я буду присылать ТОЛЬКО новые обновления.")
    else:
        await update.message.reply_text("Вы уже подписаны.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if remove_subscriber(chat_id):
        await update.message.reply_text("🔕 Вы отписались от рассылки.")
    else:
        await update.message.reply_text("Вы не были подписаны.")

async def manual_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Ищу обновления...")
    report = await asyncio.to_thread(check_bankruptcy_logic)
    
    # Разбивка на части
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
    
    # Если ничего нового, молчим
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

async def reset_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if os.path.exists(HISTORY_FILE):
        os.remove(HISTORY_FILE)
        await update.message.reply_text("🗑 Память очищена. Следующая проверка (/check) покажет ВСЕХ банкротов как новых.")
    else:
        await update.message.reply_text("История уже пуста.")

async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    codes = get_monitored_codes()
    subs = get_subscribers()
    hist = load_history()
    
    msg = (
        f"🔍 <b>Диагностика</b>\n"
        f"🏭 Кодов на мониторинге: <b>{len(codes)}</b>\n"
        f"👥 Подписчиков: <b>{len(subs)}</b>\n"
        f"💾 Записей в истории: <b>{len(hist)}</b>\n"
        f"📄 Файл companies.txt: {'OK' if os.path.exists(COMPANIES_FILE) else 'NET'}"
    )
    await update.message.reply_text(msg, parse_mode='HTML')

async def cleandup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not os.path.exists(COMPANIES_FILE):
        await update.message.reply_text("Файл не найден.")
        return
    
    with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    unique = list(set(lines))
    removed = len(lines) - len(unique)
    
    if removed > 0:
        with open(COMPANIES_FILE, 'w', encoding='utf-8') as f:
            f.write('\n'.join(unique))
        await update.message.reply_text(f"🧹 Удалено {removed} дубликатов.")
    else:
        await update.message.reply_text("✨ Дубликатов нет.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 <b>Справка по командам:</b>\n\n"
        "/start - Подписаться на уведомления\n"
        "/stop - Отписаться\n"
        "/check - Проверить наличие <b>новых</b> банкротов прямо сейчас\n"
        "/reset - Забыть историю (следующая проверка покажет всех заново)\n"
        "/cleandup - Удалить повторяющиеся коды из файла\n"
        "/debug - Техническая информация"
    )
    await update.message.reply_text(msg, parse_mode='HTML')

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

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("check", manual_check))
    app.add_handler(CommandHandler("reset", reset_history))
    app.add_handler(CommandHandler("debug", debug_command))
    app.add_handler(CommandHandler("cleandup", cleandup_command))
    app.add_handler(CommandHandler("help", help_command))

    print("Бот запущен (Full version)")
    app.run_polling()
