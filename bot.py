import logging
import os
import requests
import pandas as pd
import datetime
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv('BOT_TOKEN')

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Файл для хранения ID чатов, куда отправлять рассылку (простая база данных)
SUBSCRIBERS_FILE = "subscribers.txt"
COMPANIES_FILE = "companies.txt"

# --- ФУНКЦИИ РАБОТЫ С ДАННЫМИ ---

def get_monitored_codes():
    """Читает коды предприятий из внешнего файла."""
    if not os.path.exists(COMPANIES_FILE):
        return []
    with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        # Читаем строки, убираем пробелы и пустые строки
        codes = [line.strip() for line in f if line.strip()]
    return codes

def get_subscribers():
    """Читает ID пользователей для рассылки."""
    if not os.path.exists(SUBSCRIBERS_FILE):
        return set()
    with open(SUBSCRIBERS_FILE, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def add_subscriber(chat_id):
    """Добавляет пользователя в рассылку."""
    subs = get_subscribers()
    if str(chat_id) not in subs:
        with open(SUBSCRIBERS_FILE, 'a') as f:
            f.write(f"{chat_id}\n")

def check_bankruptcy_logic():
    """Основная логика проверки (из вашего ноутбука)."""
    enterprise_codes = get_monitored_codes()
    
    if not enterprise_codes:
        return "Список предприятий (companies.txt) пуст или не найден."

    # 1. Получение ссылки
    dataset_id = '544d4dad-0b6d-4972-b0b8-fb266829770f'
    package_show_url = f'https://data.gov.ua/api/3/action/package_show?id={dataset_id}'
    
    try:
        response = requests.get(package_show_url, timeout=10)
        data_json = response.json()
        if data_json.get('success'):
            resource_url = data_json['result']['resources'][-1]['url']
        else:
            resource_url = 'https://data.gov.ua/dataset/544d4dad-0b6d-4972-b0b8-fb266829770f/resource/deb76481-a6c8-4a45-ae6c-f02aa87e9f4a/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
    except Exception as e:
        logging.error(f"Ошибка получения метаданных: {e}")
        return "Ошибка доступа к data.gov.ua API."

    # 2. Скачивание
    local_filename = "bankruptcy_temp.csv"
    try:
        response = requests.get(resource_url, stream=True, timeout=60)
        response.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        logging.error(f"Ошибка скачивания файла: {e}")
        return "Не удалось скачать файл реестра."

    # 3. Чтение
    try:
        data_df = pd.read_csv(
            local_filename,
            sep=None,
            engine="python",
            on_bad_lines="skip",
            encoding="utf-8",
            encoding_errors='replace'
        )
    except Exception as e:
        return f"Ошибка чтения CSV: {e}"

    # Очистка
    data_df.columns = data_df.columns.str.strip()
    # Проверка наличия нужных колонок
    if 'firm_edrpou' not in data_df.columns:
         return "Ошибка структуры файла: нет колонки firm_edrpou"
         
    data_df['firm_edrpou'] = data_df['firm_edrpou'].astype(str).str.strip()
    data_df['firm_name'] = data_df['firm_name'].astype(str).str.strip()

    # 4. Поиск
    date_threshold = datetime.datetime.strptime("01.01.2025", "%d.%m.%Y").date()
    results = []

    for code in enterprise_codes:
        info = data_df[data_df['firm_edrpou'] == code]
        if not info.empty:
            full_name = info['firm_name'].values[0]
            date_str = info['date'].values[0]
            
            if pd.isna(date_str):
                continue
            
            date_str = str(date_str).strip()
            try:
                date_obj = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                if date_obj > date_threshold:
                    results.append({
                        "code": code,
                        "name": full_name,
                        "date": date_str,
                        "date_obj": date_obj
                    })
            except ValueError:
                continue

    # Удаляем временный файл
    if os.path.exists(local_filename):
        os.remove(local_filename)

    results.sort(key=lambda x: x["date_obj"])

    if not results:
        return "✅ В списке мониторинга новых банкротов не найдено."

    message = f"⚠️ <b>НАЙДЕНЫ БАНКРОТЫ ({len(results)}):</b>\n\n"
    for i, entry in enumerate(results, 1):
        message += (
            f"{i}. <b>Код:</b> {entry['code']}\n"
            f"🏢 <b>Компания:</b> {entry['name']}\n"
            f"📅 <b>Дата:</b> {entry['date']}\n"
            f"_____________________\n"
        )
    return message

# --- ОБРАБОТЧИКИ БОТА ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    add_subscriber(chat_id)
    await update.message.reply_text(
        f"Привет! Я добавил этот чат ({chat_id}) в список рассылки.\n"
        "Я буду проверять реестр банкротов каждое утро в 09:00.\n"
        "Чтобы проверить прямо сейчас, нажми /check"
    )

async def manual_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск проверки."""
    await update.message.reply_text("⏳ Начинаю проверку реестра... Это может занять минуту.")
    report = await asyncio.to_thread(check_bankruptcy_logic)
    await update.message.reply_text(report, parse_mode='HTML')

async def scheduled_check(context: ContextTypes.DEFAULT_TYPE):
    """Функция для автоматического запуска."""
    subscribers = get_subscribers()
    if not subscribers:
        logging.warning("Нет подписчиков для рассылки.")
        return

    logging.info("Запуск проверки по расписанию...")
    report = await asyncio.to_thread(check_bankruptcy_logic) # Запускаем тяжелую задачу в отдельном потоке

    for chat_id in subscribers:
        try:
            await context.bot.send_message(chat_id=chat_id, text=report, parse_mode='HTML')
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение пользователю {chat_id}: {e}")

# --- ЗАПУСК ---

if __name__ == '__main__':
    if not TOKEN:
        print("Ошибка: Не задан BOT_TOKEN в файле .env")
        exit()

    application = ApplicationBuilder().token(TOKEN).build()

    # Планировщик задач
    scheduler = AsyncIOScheduler()
    # Запускаем проверку каждый день в 09:00 утра по времени сервера
    scheduler.add_job(scheduled_check, 'cron', hour=9, minute=0, args=[application])
    scheduler.start()

    # Хендлеры
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("check", manual_check))

    print("Бот запущен...")
    application.run_polling()
