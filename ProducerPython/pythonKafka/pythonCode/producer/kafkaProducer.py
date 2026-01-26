import json
import asyncio
import websockets
from confluent_kafka import Producer
from datetime import datetime
import redis

# Конфигурация для Kafka и Redis
KAFKA_TOPIC = 'crypto_kline_data'
KAFKA_SERVER = 'localhost:9092'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_PASSWORD = 'my-password'
REDIS_USER = 'my-user'


# Функция обратного вызова для обработки результатов доставки сообщений в Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки: {err}")
    else:
        print(f"Сообщение доставлено: {msg.topic()} {msg.key()} [{msg.partition()}] @ {msg.offset()}")


# Инициализация Kafka продюсера
producer = Producer({'bootstrap.servers': KAFKA_SERVER})

# Инициализация Redis с проверкой подключения
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, username=REDIS_USER, password=REDIS_PASSWORD)
try:
    info = redis_client.info()
    print(f"Версия Redis: {info['redis_version']}")
    response = redis_client.ping()
    if response:
        print("Подключение к Redis успешно!")
    else:
        print("Не удалось подключиться к Redis.")
except redis.exceptions.RedisError as e:
    print(f"Ошибка Redis: {e}")


# Функция для подключения к Binance WebSocket API и обработки данных свечей
async def stream_kline(symbol='btcusdt', interval='1s'):
    uri = f"wss://stream.binance.com:9443/ws/{symbol}@kline_{interval}"

    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            data = json.loads(message)

            # Извлечение нужных данных из свечи (kline)
            kline_data = data['k']
            event_time = kline_data['t']  # Время начала свечи
            symbol = kline_data['s']  # Символ криптовалюты
            open_price = kline_data['o']  # Цена открытия
            close_price = kline_data['c']  # Цена закрытия
            #high_price = kline_data['h']  # Максимальная цена
            #low_price = kline_data['l']  # Минимальная цена
            #volume = kline_data['v']  # Объем торгов

            # Создаем уникальный ключ на основе временной метки и символа
            unique_id = f"{symbol}_{event_time}"

            # Проверяем наличие этого сообщения в Redis (чтобы избежать дубликатов)
            if not redis_client.exists(unique_id):
                # Формируем сообщение для Kafka
                kline_message = {
                    'symbol': symbol,
                    'event_time': datetime.fromtimestamp(event_time / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'open': open_price,
                    'close': close_price,
                    #'high': high_price,
                    #'low': low_price,
                    #'volume': volume
                }

                # Отправляем данные в Kafka
                producer.produce(
                    KAFKA_TOPIC,
                    key=symbol,
                    value=json.dumps(kline_message),
                    callback=delivery_report
                )

                # Принудительно отправляем буферизированные данные
                producer.flush()

                print(f"Отправлено в Kafka: {json.dumps(kline_message)}")

                # Сохраняем уникальный идентификатор в Redis с TTL 1 час
                redis_client.set(unique_id, 1)
                redis_client.expire(unique_id, 1800)  # Время жизни ключа 30 минут
            else:
                print(f"Дубликат сообщения: {unique_id}, игнорируем")


# Основная функция для запуска WebSocket стрима
async def main():
    await stream_kline('btcusdt', '1s')  # Пример для пары BTC/USDT с интервалом 1 секунда


# Если файл запущен напрямую
if __name__ == "__main__":
    asyncio.run(main())
