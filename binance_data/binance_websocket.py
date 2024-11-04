import queue
import websocket
import json
import time
import threading

from datetime import datetime
from logging_config import get_logger

logger = get_logger(__name__)

AUTOMATICALLY_RECONNECTS_EVERY_S = 60 * 60


class BinanceWebSocketClient:

    def __init__(self,
                 binance_client):
        self._queue = queue.Queue()
        self._binance_client = binance_client
        self._ws = None

    def start(self):

        while True:
            self._init_websocket()
            websocket_thread = threading.Thread(
                target=self._connect_ws,
                name="binance_websocket",
                daemon=True
            )
            websocket_thread.start()
            logger.info(f"Will renew the connection in {AUTOMATICALLY_RECONNECTS_EVERY_S}s")
            time.sleep(AUTOMATICALLY_RECONNECTS_EVERY_S)
            self._ws.close()
            self._ws = None
            websocket_thread.join()

    def _connect_ws(self):
        logger.info('Starting the websocket client')
        while self._ws:
            try:
                self._ws.run_forever(
                    ping_interval=10,
                    ping_timeout=5,
                )
            except Exception as e:
                logger.error("Connection error:", e)

            time.sleep(1)
            if self._ws:
                print("Reconnecting in 1 second...")

        print('End of main WS thread')

    def queue(self) -> queue.Queue:
        return self._queue

    def _load_symbols(self):
        logger.info('Loading symbols')
        exchange_info = self._binance_client.futures_exchange_info()
        symbols = []
        for symbol in exchange_info.get('symbols'):
            if symbol['quoteAsset'] == 'USDT' and symbol['contractType'] == 'PERPETUAL':
                symbols.append(symbol['symbol'])
        logger.info('{} Symbols were found'.format(len(symbols)))
        return symbols

    def _init_websocket(self):
        logger.info('Initializing the websocket')
        symbols = self._load_symbols()
        streams = '/'.join([f'{s.lower()}_perpetual@continuousKline_1m' for s in symbols])
        ws_url = f'wss://fstream.binance.com/ws/{streams}'
        logger.debug(f'Here is the WS url: {ws_url}')
        logger.debug(f'The WS url is {ws_url}')
        self._ws = websocket.WebSocketApp(ws_url,
                                          on_open=self._on_open,
                                          on_message=self._on_message,
                                          on_error=self._on_error,
                                          on_close=self._on_close)

    def _on_message(self, ws, json_message):
        try:
            message = json.loads(json_message)
            candle = message['k']
            symbol = message['ps']
            contract = message['ct']
            timestamp = message['E']
            is_candle_closed = candle['x']
            if is_candle_closed:
                logger.debug(f'Appending a message in the queue for symbol={symbol}')
                event_ts = datetime.fromtimestamp(timestamp / 1000)
                self._queue.put({
                    'start_time': datetime.fromtimestamp(candle['t'] / 1000),
                    'end_time': datetime.fromtimestamp(candle['T'] / 1000),
                    'symbol': symbol,
                    'exchange': 'BINANCE',
                    'contract': contract,
                    'timestamp': event_ts,
                    'date': event_ts.date(),
                    'interval': candle.get('i'),
                    'first_trade_id': candle.get('f'),
                    'last_trade_id': candle.get('L'),
                    'open': float(candle.get('o')),
                    'close': float(candle.get('c')),
                    'high': float(candle.get('h')),
                    'low': float(candle.get('l')),
                    'base_asset_volume': float(candle.get('v')),
                    'quote_asset_volume': float(candle.get('q')),
                    'taker_buy_base_asset_volume': float(candle.get('V')),
                    'taker_buy_quote_asset_volume': float(candle.get('Q')),
                    'num_trades': candle.get('n'),
                })

        except Exception as e:
            print(e)

    @staticmethod
    def _on_error(ws, error):
        logger.error(f"Error: {error}")

    @staticmethod
    def _on_open(ws):
        logger.info("WebSocket connection opened")

    @staticmethod
    def _on_close(ws, close_status_code, close_msg):
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
