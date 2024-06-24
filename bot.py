import telebot
from telebot import types
import pandas as pd
import schedule
import time
import threading
import asyncio
import json
from data_fetcher import fetch_all_data  # Импортируем функцию из data_fetcher.py

# Telegram bot token
TELEGRAM_TOKEN = '7066463143:AAG7p9SFxKAmCNwqz48YErtuEhh7TmXm7sE'

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# Загружаем настройки пользователей из файла
def load_user_settings():
    try:
        with open('user_settings.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

# Сохраняем настройки пользователей в файл
def save_user_settings(settings):
    with open('user_settings.json', 'w') as f:
        json.dump(settings, f, indent=4)

user_settings = load_user_settings()

# Функция для получения chat_id пользователя
def get_user_setting(user_id, key, default_value):
    return user_settings.get(str(user_id), {}).get(key, default_value)

# Функция для установки chat_id пользователя
def set_user_setting(user_id, key, value):
    if str(user_id) not in user_settings:
        user_settings[str(user_id)] = {}
    user_settings[str(user_id)][key] = value
    save_user_settings(user_settings)

# Function to split message into chunks
def split_message(message, max_length=4096):
    return [message[i:i+max_length] for i in range(0, len(message), max_length)]

# Function to check changes in Open Interest
def check_oi_changes(df):
    try:
        df['open_interest'] = df['open_interest'].astype(float)  # Преобразование open_interest в float
        df['price'] = df['price'].astype(float)  # Преобразование price в float
        # Group data by symbol
        grouped = df.groupby('symbol')

        for symbol, group in grouped:
            group = group.sort_values(by='timestamp', ascending=False)
            if len(group) >= 4:
                latest_oi = group.iloc[0]['open_interest']
                latest_price = group.iloc[0]['price']
                oi_5min_ago = group.iloc[1]['open_interest']
                oi_10min_ago = group.iloc[2]['open_interest']
                oi_15min_ago = group.iloc[3]['open_interest']
                price_5min_ago = group.iloc[1]['price']
                price_10min_ago = group.iloc[2]['price']
                price_15min_ago = group.iloc[3]['price']
                
                print(f"Checking {symbol}:")
                print(f"Latest OI: {latest_oi}, Latest Price: {latest_price}, 5 min ago: {oi_5min_ago}, 10 min ago: {oi_10min_ago}, 15 min ago: {oi_15min_ago}")
                print(f"Price 5 min ago: {price_5min_ago}, Price 10 min ago: {price_10min_ago}, Price 15 min ago: {price_15min_ago}")

                # Calculate percentage changes
                change_5min = ((latest_oi - oi_5min_ago) / oi_5min_ago) * 100
                change_10min = ((latest_oi - oi_10min_ago) / oi_10min_ago) * 100
                change_15min = ((latest_oi - oi_15min_ago) / oi_15min_ago) * 100

                price_change_5min = ((latest_price - price_5min_ago) / price_5min_ago) * 100
                price_change_10min = ((latest_price - price_10min_ago) / price_10min_ago) * 100
                price_change_15min = ((latest_price - price_15min_ago) / price_15min_ago) * 100

                print(f"Percentage changes for {symbol}: 5 min: {change_5min}%, 10 min: {change_10min}%, 15 min: {change_15min}%")
                print(f"Price percentage changes for {symbol}: 5 min: {price_change_5min}%, 10 min: {price_change_10min}%, 15 min: {price_change_15min}%")

                # Check if any change exceeds the threshold for each user
                for user_id, settings in user_settings.items():
                    threshold = settings.get('threshold', 5)
                    if abs(change_5min) > threshold or abs(change_10min) > threshold or abs(change_15min) > threshold:
                        message = f"OI Change Alert for {symbol}:\n" \
                                  f"Current Price: {latest_price}\n" \
                                  f"5min change: {change_5min:.2f}% (OI: {latest_oi}, Price Change: {price_change_5min:.2f}%)\n" \
                                  f"10min change: {change_10min:.2f}% (OI: {oi_10min_ago}, Price Change: {price_change_10min:.2f}%)\n" \
                                  f"15min change: {change_15min:.2f}% (OI: {oi_15min_ago}, Price Change: {price_change_15min:.2f}%)"
                        messages = split_message(message)
                        for msg in messages:
                            send_telegram_message(user_id, msg)
    except Exception as e:
        print(f"Error checking OI changes: {e}")

# Function to send a message via Telegram bot
def send_telegram_message(user_id, message):
    bot.send_message(user_id, text=message)

# Function to run the data fetcher script
def run_data_fetcher():
    print("Fetching data...")
    df = asyncio.run(fetch_all_data())
    print("Data fetched, checking OI changes...")
    check_oi_changes(df)

# Schedule tasks
def schedule_tasks():
    schedule.every().hour.at(":01").do(run_data_fetcher)
    schedule.every().hour.at(":06").do(run_data_fetcher)
    schedule.every().hour.at(":11").do(run_data_fetcher)
    schedule.every().hour.at(":16").do(run_data_fetcher)
    schedule.every().hour.at(":21").do(run_data_fetcher)
    schedule.every().hour.at(":26").do(run_data_fetcher)
    schedule.every().hour.at(":31").do(run_data_fetcher)
    schedule.every().hour.at(":36").do(run_data_fetcher)
    schedule.every().hour.at(":41").do(run_data_fetcher)
    schedule.every().hour.at(":46").do(run_data_fetcher)
    schedule.every().hour.at(":51").do(run_data_fetcher)
    schedule.every().hour.at(":56").do(run_data_fetcher)

    while True:
        schedule.run_pending()
        time.sleep(1)

# Command to start the bot and register the user
@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.chat.id
    if str(user_id) not in user_settings:
        user_settings[str(user_id)] = {'threshold': 5, 'interval': 15}
        save_user_settings(user_settings)
    bot.reply_to(message, "Welcome! Use /settings to set your preferences.")

# Command to set the threshold
@bot.message_handler(commands=['setthreshold'])
def set_threshold(message):
    try:
        user_id = message.chat.id
        threshold = float(message.text.split()[1])
        set_user_setting(user_id, 'threshold', threshold)
        bot.reply_to(message, f"Threshold set to {threshold}%")
    except (IndexError, ValueError):
        bot.reply_to(message, 'Usage: /setthreshold <percentage>')

# Command to set the interval
@bot.message_handler(commands=['setinterval'])
def set_interval(message):
    try:
        user_id = message.chat.id
        interval = int(message.text.split()[1])
        set_user_setting(user_id, 'interval', interval)
        bot.reply_to(message, f"Interval set to {interval} minutes")
    except (IndexError, ValueError):
        bot.reply_to(message, 'Usage: /setinterval <minutes>')

# Inline buttons for setting threshold and interval
@bot.message_handler(commands=['settings'])
def settings(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Set Threshold", callback_data="set_threshold"))
    markup.add(types.InlineKeyboardButton("Set Interval", callback_data="set_interval"))
    bot.send_message(message.chat.id, "Choose a setting:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "set_threshold")
def set_threshold_inline(call):
    msg = bot.send_message(call.message.chat.id, "Enter new threshold percentage:")
    bot.register_next_step_handler(msg, process_threshold_step)

def process_threshold_step(message):
    try:
        user_id = message.chat.id
        threshold = float(message.text)
        set_user_setting(user_id, 'threshold', threshold)
        bot.reply_to(message, f"Threshold set to {threshold}%")
    except ValueError:
        bot.reply_to(message, "Invalid input. Please enter a valid number.")

@bot.callback_query_handler(func=lambda call: call.data == "set_interval")
def set_interval_inline(call):
    msg = bot.send_message(call.message.chat.id, "Enter new interval in minutes:")
    bot.register_next_step_handler(msg, process_interval_step)

def process_interval_step(message):
    try:
        user_id = message.chat.id
        interval = int(message.text)
        set_user_setting(user_id, 'interval', interval)
        bot.reply_to(message, f"Interval set to {interval} minutes")
    except ValueError:
        bot.reply_to(message, "Invalid input. Please enter a valid number.")

if __name__ == "__main__":
    # Start the scheduler in a new thread
    scheduler_thread = threading.Thread(target=schedule_tasks)
    scheduler_thread.start()

    # Start the Telegram bot
    bot.polling(none_stop=True)
