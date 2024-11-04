import threading
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

ws = BinanceWebSocketClient(binance_client=binance_client)
consumer = KywyConsumer(data_queue=ws.queue(), datasource_name='binance')

consumer.start()
ws.start()
