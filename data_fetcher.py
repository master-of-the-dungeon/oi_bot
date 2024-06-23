import aiohttp
import asyncio
import pandas as pd
from datetime import datetime
import pytz

async def fetch_symbols_binance(session):
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    async with session.get(url) as response:
        data = await response.json()
    
    symbols = []
    if 'symbols' in data:
        for item in data['symbols']:
            symbols.append(item['symbol'])
    
    return symbols

async def fetch_open_interest_binance(session, symbol, semaphore):
    url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=4"  # Получаем последние 4 значения с интервалом 5 минут
    async with semaphore:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Error fetching data for {symbol}: HTTP {response.status}")
                return []

            try:
                data = await response.json()
            except aiohttp.ContentTypeError:
                print(f"ContentTypeError: Unexpected mimetype for {symbol}. URL: {url}")
                return []

            if isinstance(data, list):
                return data  # Возвращаем последние 4 значения
            else:
                print(f"Error fetching data from Binance for symbol {symbol}: {data}")
                return []

def format_timestamp(timestamp, tz_info):
    # Преобразование из Unix времени в локальный формат YYYY-MM-DD HH:MM:SS
    dt = datetime.utcfromtimestamp(timestamp / 1000).replace(tzinfo=pytz.utc)
    local_dt = dt.astimezone(tz_info)
    return local_dt.strftime('%Y-%m-%d %H:%M:%S')

async def fetch_all_data():
    local_tz = pytz.timezone('Europe/Moscow')  # Замените на ваш локальный часовой пояс
    semaphore = asyncio.Semaphore(10)  # Ограничиваем количество одновременных запросов

    async with aiohttp.ClientSession() as session:
        symbols = await fetch_symbols_binance(session)
        
        tasks = []
        for symbol in symbols:
            tasks.append(fetch_open_interest_binance(session, symbol, semaphore))

        all_data_binance = []
        results = await asyncio.gather(*tasks)

        for symbol, open_interest_data_binance in zip(symbols, results):
            if len(open_interest_data_binance) == 4:  # Убедимся, что у нас есть четыре значения
                for record in open_interest_data_binance:
                    all_data_binance.append({
                        'platform': 'Binance',
                        'symbol': symbol,
                        'open_interest': record['sumOpenInterest'],
                        'timestamp': format_timestamp(int(record['timestamp']), local_tz)
                    })

        df_binance = pd.DataFrame(all_data_binance)
        return df_binance

if __name__ == "__main__":
    df = asyncio.run(fetch_all_data())
    print(df)
