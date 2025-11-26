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
RESOURCE_ID = 'deb76481-a6c8-4a45-ae6c-f02aa87e9f4a'
BACKUP_URL = f'https://data.gov.ua/dataset/{DATASET_ID}/resource/{RESOURCE_ID}/download/vidomosti-pro-spravi-pro-bankrutstvo.csv'
DAYS_TO_CHECK = 2  # Проверять банкротства за последний год

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
    """Читает коды предприятий из внешнего файла."""
    if not os.path.exists(COMPANIES_FILE):
        logger.warning(f"Файл {COMPANIES_FILE} не найден.")
        return []
    
    try:
        with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
            codes = [line.strip() for line in f if line.strip()]
        
        # Убираем дубликаты, сохраняя порядок
        unique_codes = []
        seen = set()
        for code in codes:
            if code not in seen:
                unique_codes.append(code)
                seen.add(code)
        
        if len(codes) != len(unique_codes):
            logger.info(f"Удалено {len(codes) - len(unique_codes)} дубликатов из списка кодов.")
        
        logger.info(f"Загружено {len(unique_codes)} уникальных кодов предприятий.")
        return unique_codes
    except Exception as e:
        logger.error(f"Ошибка чтения {COMPANIES_FILE}: {e}")
        return []


def get_subscribers():
    """Читает ID пользователей для рассылки."""
    if not os.path.exists(SUBSCRIBERS_FILE):
        return set()
    
    try:
        with open(SUBSCRIBERS_FILE, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    except Exception as e:
        logger.error(f"Ошибка чтения подписчиков: {e}")
        return set()


def add_subscriber(chat_id):
    """Добавляет пользователя в рассылку."""
    subs = get_subscribers()
    chat_id_str = str(chat_id)
    
    if chat_id_str not in subs:
        try:
            with open(SUBSCRIBERS_FILE, 'a') as f:
                f.write(f"{chat_id_str}\n")
            logger.info(f"Добавлен подписчик: {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления подписчика: {e}")
            return False
    return False


def remove_subscriber(chat_id):
    """Удаляет пользователя из рассылки."""
    subs = get_subscribers()
    chat_id_str = str(chat_id)
    
    if chat_id_str in subs:
        try:
            subs.remove(chat_id_str)
            with open(SUBSCRIBERS_FILE, 'w') as f:
                for sub in subs:
                    f.write(f"{sub}\n")
            logger.info(f"Удален подписчик: {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления подписчика: {e}")
            return False
    return False


def get_resource_url():
    """Получает актуальную ссылку на CSV файл."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    
    # Попытка через API
    try:
        package_show_url = f'https://data.gov.ua/api/3/action/package_show?id={DATASET_ID}'
        logger.info("Получение URL через API...")
        
        response = requests.get(package_show_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        data_json = response.json()
        if data_json.get('success'):
            resources = data_json['result']['resources']
            if resources:
                url = resources[-1]['url']
                logger.info(f"URL получен через API: {url}")
                return url
    except Exception as e:
        logger.warning(f"API недоступен: {e}")
    
    # Резервная ссылка
    logger.info("Используется резервная ссылка.")
    return BACKUP_URL


def download_csv(url, filename="bankruptcy_temp.csv"):
    """Скачивает CSV файл."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        logger.info(f"Скачивание файла: {url}")
        response = requests.get(
            url, 
            headers=headers, 
            stream=True, 
            timeout=120,
            verify=True  # БЕЗОПАСНО: проверяем SSL
        )
        response.raise_for_status()
        
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info("Файл успешно скачан.")
        return True
        
    except requests.exceptions.SSLError:
        # Только если есть проблемы с SSL, пробуем без проверки
        logger.warning("SSL ошибка, повторная попытка без проверки...")
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=120, verify=False)
            response.raise_for_status()
            
            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            logger.error(f"Критическая ошибка скачивания: {e}")
            return False
    except Exception as e:
        logger.error(f"Ошибка скачивания: {e}")
        return False


def read_csv(filename):
    """Читает CSV файл с автоопределением кодировки."""
    # Порядок важен: сначала пробуем украинские кодировки
    encodings = ["utf-8", "cp1251", "windows-1251", "utf-8-sig", "latin-1"]
    
    for encoding in encodings:
        try:
            df = pd.read_csv(
                filename,
                sep=None,
                engine="python",
                on_bad_lines="skip",
                encoding=encoding,
                encoding_errors='replace'  # Заменяем проблемные символы
            )
            
            # Проверяем, что кодировка правильная (нет "крякозябр")
            test_text = str(df.iloc[0, 0]) if len(df) > 0 else ""
            if '�' not in test_text or encoding == encodings[-1]:
                logger.info(f"CSV прочитан с кодировкой: {encoding}")
                return df
                
        except Exception as e:
            logger.debug(f"Не удалось прочитать с {encoding}: {e}")
            continue
    
    logger.error("Не удалось подобрать кодировку.")
    return None


def find_column(df, keywords):
    """Ищет колонку по ключевым словам."""
    for col in df.columns:
        if any(keyword in col.lower() for keyword in keywords):
            return col
    return None


def parse_date(date_str):
    """Парсит дату из строки."""
    if pd.isna(date_str) or str(date_str).lower() == 'nan':
        return None
    
    date_str = str(date_str).strip().split()[0]
    
    try:
        return datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        return None


def check_bankruptcy_logic():
    """Основная логика проверки банкротств."""
    try:
        # 1. Получение списка кодов
        enterprise_codes = get_monitored_codes()
        if not enterprise_codes:
            return "⚠️ Список предприятий (companies.txt) пуст или не найден."
        
        # 2. Получение URL и скачивание
        resource_url = get_resource_url()
        local_filename = "bankruptcy_temp.csv"
        
        if not download_csv(resource_url, local_filename):
            return "❌ Не удалось скачать файл реестра. Проверьте подключение к интернету."
        
        # 3. Чтение CSV
        data_df = read_csv(local_filename)
        if data_df is None:
            return "❌ Ошибка: не удалось прочитать файл реестра."
        
        # Очистка названий колонок
        data_df.columns = data_df.columns.str.strip()
        
        # 4. Поиск нужных колонок
        edrpou_col = find_column(data_df, ['код', 'edrpou', 'єдрпоу'])
        if not edrpou_col:
            if 'firm_edrpou' in data_df.columns:
                edrpou_col = 'firm_edrpou'
            else:
                return f"❌ Не найдена колонка с кодом ЄДРПОУ.\nДоступные: {', '.join(data_df.columns[:5])}..."
        
        name_col = find_column(data_df, ['назва', 'name', 'найменування'])
        if not name_col:
            name_col = data_df.columns[1]  # Берем вторую колонку
        
        date_col = find_column(data_df, ['дата', 'date'])
        if not date_col:
            return "❌ Не найдена колонка с датой."
        
        # 5. Подготовка данных
        data_df['clean_code'] = data_df[edrpou_col].astype(str).str.strip()
        
        # Динамическая дата порога (N дней назад)
        date_threshold = datetime.date.today() - datetime.timedelta(days=DAYS_TO_CHECK)
        
        # 6. Поиск совпадений
        results = []
        seen_codes = set()  # Для отслеживания уже добавленных кодов
        
        for code in enterprise_codes:
            # Пропускаем, если код уже обработан
            if code in seen_codes:
                continue
                
            matches = data_df[data_df['clean_code'] == code]
            
            if not matches.empty:
                # Берем только первую (самую свежую) запись для каждого кода
                row = matches.iloc[0]
                full_name = str(row[name_col])
                
                # Пытаемся исправить кодировку названия, если она битая
                try:
                    # Если название выглядит как latin-1, но на самом деле cp1251
                    if any(ord(c) > 127 for c in full_name):
                        # Пробуем перекодировать
                        try:
                            full_name = full_name.encode('latin-1').decode('cp1251')
                        except:
                            try:
                                full_name = full_name.encode('cp1252').decode('cp1251')
                            except:
                                pass  # Оставляем как есть
                except:
                    pass  # Оставляем оригинальное название
                
                date_obj = parse_date(row[date_col])
                
                if date_obj and date_obj > date_threshold:
                    results.append({
                        "code": code,
                        "name": full_name,
                        "date": date_obj.strftime("%d.%m.%Y"),
                        "date_obj": date_obj
                    })
                    seen_codes.add(code)  # Помечаем код как обработанный
        
        # Очистка
        if os.path.exists(local_filename):
            os.remove(local_filename)
        
        # 7. Формирование отчета
        if not results:
            return f"✅ В списке мониторинга новых банкротов не найдено\n(проверка за последние {DAYS_TO_CHECK} дней)."
        
        results.sort(key=lambda x: x["date_obj"], reverse=True)
        
        # Ограничиваем вывод, если слишком много результатов
        MAX_DISPLAY = 20
        total_count = len(results)
        display_results = results[:MAX_DISPLAY]
        
        message = f"⚠️ <b>НАЙДЕНЫ БАНКРОТЫ ({total_count})</b>\n\n"
        
        for i, entry in enumerate(display_results, 1):
            # Укорачиваем длинные названия
            name = entry['name']
            if len(name) > 80:
                name = name[:77] + "..."
            
            message += (
                f"{i}. <b>Код:</b> {entry['code']}\n"
                f"🏢 {name}\n"
                f"📅 {entry['date']}\n"
                f"{'-' * 25}\n"
            )
        
        if total_count > MAX_DISPLAY:
            message += f"\n<i>... и еще {total_count - MAX_DISPLAY} записей</i>"
        
        return message
        
    except Exception as e:
        logger.error(f"Критическая ошибка в check_bankruptcy_logic: {e}", exc_info=True)
        return f"❌ Произошла ошибка при проверке: {str(e)[:200]}"

        
# --- ОБРАБОТЧИКИ БОТА ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    chat_id = update.effective_chat.id
    is_new = add_subscriber(chat_id)
    
    if is_new:
        message = (
            f"👋 Привет! Ты подписан на рассылку.\n\n"
            f"🔔 Я буду проверять реестр банкротов каждое утро в 09:00 (Киев).\n\n"
            f"Доступные команды:\n"
            f"/check - проверить прямо сейчас\n"
            f"/stop - отписаться от рассылки\n"
            f"/help - показать справку"
        )
    else:
        message = "Ты уже подписан! Используй /check для ручной проверки."
    
    await update.message.reply_text(message)


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /stop - отписка"""
    chat_id = update.effective_chat.id
    
    if remove_subscriber(chat_id):
        await update.message.reply_text(
            "👋 Ты отписан от рассылки.\n"
            "Чтобы подписаться снова, используй /start"
        )
    else:
        await update.message.reply_text("Ты и так не подписан на рассылку.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    message = (
        "📖 <b>Справка по боту</b>\n\n"
        "Этот бот мониторит реестр банкротств Украины.\n\n"
        "<b>Команды:</b>\n"
        "/start - подписаться на рассылку\n"
        "/check - проверить сейчас\n"
        "/stop - отписаться\n"
        "/cleandup - удалить дубликаты из списка\n"
        "/debug - диагностика (проверка настроек)\n"
        "/help - эта справка\n\n"
        f"⏰ Автоматическая проверка: каждый день в 09:00\n"
        f"📊 Проверяются банкротства за последние {DAYS_TO_CHECK} дней"
    )
    await update.message.reply_text(message, parse_mode='HTML')


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /debug - диагностика"""
    try:
        # Проверка файлов
        companies_exist = os.path.exists(COMPANIES_FILE)
        subscribers_exist = os.path.exists(SUBSCRIBERS_FILE)
        
        codes = get_monitored_codes()
        subs = get_subscribers()
        
        # Проверяем дубликаты в исходном файле
        duplicates_count = 0
        if companies_exist:
            with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
                all_codes = [line.strip() for line in f if line.strip()]
                duplicates_count = len(all_codes) - len(set(all_codes))
        
        # Проверка интернета
        try:
            test_url = "https://data.gov.ua"
            response = requests.get(test_url, timeout=10)
            internet_ok = response.status_code == 200
            internet_status = f"✅ OK ({response.status_code})"
        except Exception as e:
            internet_ok = False
            internet_status = f"❌ Ошибка: {str(e)[:100]}"
        
        # Попытка получить URL ресурса
        try:
            resource_url = get_resource_url()
            url_status = f"✅ {resource_url[:50]}..."
        except Exception as e:
            resource_url = None
            url_status = f"❌ {str(e)[:100]}"
        
        message = (
            "🔍 <b>ДИАГНОСТИКА БОТА</b>\n\n"
            f"📁 Файл companies.txt: {'✅ Существует' if companies_exist else '❌ Не найден'}\n"
            f"   Уникальных кодов: <b>{len(codes)}</b>\n"
        )
        
        if duplicates_count > 0:
            message += f"   ⚠️ Найдено дубликатов: <b>{duplicates_count}</b>\n"
        
        message += f"   Коды: {', '.join(codes[:5])}{' ...' if len(codes) > 5 else ''}\n\n"
        message += (
            f"📁 Файл subscribers.txt: {'✅ Существует' if subscribers_exist else '❌ Не найден'}\n"
            f"   Подписчиков: <b>{len(subs)}</b>\n\n"
            f"🌐 Доступ к data.gov.ua: {internet_status}\n\n"
            f"🔗 URL ресурса: {url_status}\n\n"
        )
        
        # Детальная проверка
        if not codes:
            message += "⚠️ <b>ВНИМАНИЕ:</b> Файл companies.txt пуст!\n"
            message += "Создайте файл и добавьте коды ЄДРПОУ (по одному на строку).\n\n"
        
        if duplicates_count > 0:
            message += f"⚠️ В файле {duplicates_count} дубликатов. Используйте /cleandup для очистки.\n\n"
        
        if not internet_ok:
            message += "⚠️ <b>ВНИМАНИЕ:</b> Нет доступа к data.gov.ua!\n"
            message += "Проверьте интернет-соединение.\n\n"
        
        message += "Используйте /check для полной проверки."
        
        await update.message.reply_text(message, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка в debug_command: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка диагностики:\n<code>{str(e)}</code>",
            parse_mode='HTML'
        )


async def cleandup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /cleandup - удаление дубликатов из companies.txt"""
    try:
        if not os.path.exists(COMPANIES_FILE):
            await update.message.reply_text("❌ Файл companies.txt не найден.")
            return
        
        # Читаем все коды
        with open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
            all_codes = [line.strip() for line in f if line.strip()]
        
        original_count = len(all_codes)
        
        # Убираем дубликаты
        unique_codes = []
        seen = set()
        for code in all_codes:
            if code not in seen:
                unique_codes.append(code)
                seen.add(code)
        
        duplicates_removed = original_count - len(unique_codes)
        
        if duplicates_removed == 0:
            await update.message.reply_text("✅ Дубликатов не найдено, файл чистый!")
            return
        
        # Создаем бэкап
        backup_file = f"{COMPANIES_FILE}.backup"
        with open(backup_file, 'w', encoding='utf-8') as f:
            for code in all_codes:
                f.write(f"{code}\n")
        
        # Записываем очищенный список
        with open(COMPANIES_FILE, 'w', encoding='utf-8') as f:
            for code in unique_codes:
                f.write(f"{code}\n")
        
        message = (
            f"✅ Очистка завершена!\n\n"
            f"Было кодов: <b>{original_count}</b>\n"
            f"Удалено дубликатов: <b>{duplicates_removed}</b>\n"
            f"Осталось: <b>{len(unique_codes)}</b>\n\n"
            f"Резервная копия сохранена: {backup_file}"
        )
        
        await update.message.reply_text(message, parse_mode='HTML')
        logger.info(f"Удалено {duplicates_removed} дубликатов из {COMPANIES_FILE}")
        
    except Exception as e:
        logger.error(f"Ошибка в cleandup_command: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при очистке:\n<code>{str(e)}</code>",
            parse_mode='HTML'
        )


async def send_long_message(update: Update, text: str, parse_mode='HTML'):
    """Отправляет длинное сообщение частями (лимит Telegram 4096 символов)"""
    MAX_LENGTH = 4000  # Оставляем запас
    
    if len(text) <= MAX_LENGTH:
        await update.message.reply_text(text, parse_mode=parse_mode)
        return
    
    # Разбиваем по разделителям
    parts = []
    current_part = ""
    
    for line in text.split('\n'):
        if len(current_part) + len(line) + 1 > MAX_LENGTH:
            parts.append(current_part)
            current_part = line + '\n'
        else:
            current_part += line + '\n'
    
    if current_part:
        parts.append(current_part)
    
    # Отправляем части
    for i, part in enumerate(parts):
        header = f"📄 Часть {i+1}/{len(parts)}\n\n" if len(parts) > 1 else ""
        await update.message.reply_text(header + part, parse_mode=parse_mode)


async def manual_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /check - ручная проверка"""
    try:
        await update.message.reply_text("⏳ Начинаю проверку реестра... Это может занять минуту.")
        
        # Запускаем в отдельном потоке
        report = await asyncio.to_thread(check_bankruptcy_logic)
        
        # Отправляем с поддержкой длинных сообщений
        await send_long_message(update, report)
        
    except Exception as e:
        logger.error(f"Ошибка в manual_check: {e}", exc_info=True)
        error_msg = (
            f"❌ Произошла ошибка при проверке:\n\n"
            f"<code>{str(e)[:500]}</code>\n\n"
            f"Проверьте логи бота для подробностей."
        )
        await update.message.reply_text(error_msg, parse_mode='HTML')


async def scheduled_check(context: ContextTypes.DEFAULT_TYPE):
    """Автоматическая проверка по расписанию"""
    subscribers = get_subscribers()
    
    if not subscribers:
        logger.warning("Нет подписчиков для рассылки.")
        return
    
    logger.info(f"Запуск проверки по расписанию для {len(subscribers)} подписчиков...")
    
    try:
        report = await asyncio.to_thread(check_bankruptcy_logic)
        
        # Разбиваем длинное сообщение на части
        MAX_LENGTH = 4000
        messages = []
        
        if len(report) <= MAX_LENGTH:
            messages = [report]
        else:
            # Разбиваем по строкам
            parts = []
            current_part = ""
            
            for line in report.split('\n'):
                if len(current_part) + len(line) + 1 > MAX_LENGTH:
                    parts.append(current_part)
                    current_part = line + '\n'
                else:
                    current_part += line + '\n'
            
            if current_part:
                parts.append(current_part)
            
            messages = parts
        
        success_count = 0
        for chat_id in subscribers:
            try:
                for i, msg in enumerate(messages):
                    header = f"📄 Часть {i+1}/{len(messages)}\n\n" if len(messages) > 1 else ""
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=header + msg,
                        parse_mode='HTML'
                    )
                success_count += 1
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение {chat_id}: {e}")
        
        logger.info(f"Рассылка завершена: успешно {success_count}/{len(subscribers)}")
        
    except Exception as e:
        logger.error(f"Ошибка в scheduled_check: {e}", exc_info=True)


# --- ЗАПУСК ---

if __name__ == '__main__':
    if not TOKEN:
        print("❌ Ошибка: Не задан BOT_TOKEN в файле .env")
        exit(1)
    
    # Создаем приложение
    application = ApplicationBuilder().token(TOKEN).build()
    
    # Настройка планировщика
    job_queue = application.job_queue
    kyiv_tz = pytz.timezone('Europe/Kiev')
    target_time = datetime.time(hour=9, minute=0, tzinfo=kyiv_tz)
    
    # Добавляем задачу
    job_queue.run_daily(scheduled_check, time=target_time)
    
    # Регистрация команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("check", manual_check))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("debug", debug_command))
    application.add_handler(CommandHandler("cleandup", cleandup_command))
    
    logger.info("🤖 Бот запущен и готов к работе!")
    logger.info(f"📅 Автоматическая проверка: каждый день в {target_time.hour:02d}:{target_time.minute:02d}")
    
    application.run_polling()
