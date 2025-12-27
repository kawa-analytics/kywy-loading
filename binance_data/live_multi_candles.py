import os
from binance.client import Client
from binance_websocket import BinanceWebSocketClient
from kywy_consumer import KywyConsumer
from dotenv import load_dotenv
from logging_config import get_logger

logger = get_logger(__name__)
load_dotenv()
binance_client = Client(api_key=os.getenv("BINANCE_API_KEY"),
                        api_secret=os.getenv("BINANCE_API_SECRET"))
candles = ['1m', '5m', '15m', '4h', '1d', '1w']

ws = BinanceWebSocketClient(binance_client=binance_client,
                            live=True,
                            candle=None,
                            candles=candles)

consumer = KywyConsumer(data_queue=ws.queue(),
                        datasource_name='[DO NOT USE DIRECTLY] Binance Live candles',
                        live=ws.live)

consumer.start()
ws.start()

