import queue
from typing import List

import websocket
import json
import time
import threading
import ssl

from datetime import datetime
from logging_config import get_logger

logger = get_logger(__name__)

AUTOMATICALLY_RECONNECTS_EVERY_S = 60 * 60
MAX_SYMBOLS_PER_WEBSOCKET = 300

SYMBOL_BLACKLIST = [
    "AGIXUSDT",
    "AMBUSDT",
    "BADGERUSDT",
    "BALUSDT",
    "BLZUSDT",
    "BNXUSDT",
    "BONDUSDT",
    "COMBOUSDT",
    "CTKUSDT",
    "CVCUSDT",
    "CVXUSDT",
    "DARUSDT",
    "DGBUSDT",
    "FTMUSDT",
    "FTTUSDT",
    "GLMRUSDT",
    "IDEXUSDT",
    "KEYUSDT",
    "KLAYUSDT",
    "LINAUSDT",
    "LITUSDT",
    "LOOMUSDT",
    "MDTUSDT",
    "NULSUSDT",
    "OCEANUSDT",
    "OMGUSDT",
    "ORBSUSDT",
    "RADUSDT",
    "RAYUSDT",
    "REEFUSDT",
    "RENUSDT",
    "SCUSDT",
    "SLPUSDT",
    "SNTUSDT",
    "STMXUSDT",
    "STPTUSDT",
    "STRAXUSDT",
    "TROYUSDT",
    "UNFIUSDT",
    "VIDTUSDT",
    "WAVESUSDT",
    "XEMUSDT",
    "USDCUSDT",
]


class BinanceWebSocketClient:

    def __init__(self,
                 binance_client):
        self._queue = queue.Queue()
        self._binance_client = binance_client
        self._websockets: List[websocket.WebSocketApp] = []
        self.renewing_websockets: bool = False

    def start(self):
        #  Connect - and automatic close and reconnect loop
        while True:
            self.renewing_websockets = False
            self._init_websockets()
            ws_threads: List[threading.Thread] = []
            for i, ws in enumerate(self._websockets):
                websocket_thread = threading.Thread(
                    target=self._connect_ws,
                    args=(ws, i),
                    name=f'binance_websocket_{i}',
                    daemon=True
                )
                websocket_thread.start()
                ws_threads.append(websocket_thread)

            logger.info(f"Will renew the {len(ws_threads)} connections in {AUTOMATICALLY_RECONNECTS_EVERY_S}s")
            time.sleep(AUTOMATICALLY_RECONNECTS_EVERY_S)
            while self.is_time_around_minute():
                logger.info('We are waiting to renew the connections to not be around a potential event')
                time.sleep(2)
            logger.info(f'Now renewing the {len(ws_threads)} connections')
            self.renewing_websockets = True
            for ws in self._websockets:
                ws.close()
            self._websockets = []
            for thread in ws_threads:
                thread.join()

    def _connect_ws(self, ws: websocket.WebSocketApp, websocket_id: int):
        logger.info(f'Starting the websocket client {websocket_id}')
        while ws:
            try:
                ws.run_forever(
                    ping_interval=10,
                    ping_timeout=5,
                    sslopt={"cert_reqs": ssl.CERT_NONE},
                )
            except Exception as e:
                logger.error(f'Connection error in client {websocket_id}:', e)
            time.sleep(1)
            if not self.renewing_websockets:
                logger.info(f'Reconnecting in 1 second client {websocket_id}...')
            else:
                break

        logger.info(f'End of main WS thread {websocket_id}')

    def queue(self) -> queue.Queue:
        return self._queue

    def _load_symbols(self) -> List[str]:
        logger.info('Loading symbols')
        exchange_info = self._binance_client.futures_exchange_info()
        symbols = []
        for symbol in exchange_info.get('symbols'):
            quote_asset_usdt = symbol['quoteAsset'] == 'USDT'
            perpetual = symbol['contractType'] == 'PERPETUAL'
            not_in_blacklist = symbol['symbol'] not in SYMBOL_BLACKLIST
            if quote_asset_usdt and perpetual and not_in_blacklist:
                symbols.append(symbol['symbol'])
        logger.info('{} Symbols were found'.format(len(symbols)))
        return symbols

    def _init_websockets(self):
        logger.info('Initializing the websockets')
        symbols = self._load_symbols()
        logger.info(f'There are {len(symbols)} symbols to load')
        self._websockets = [self._build_websocket(symbols_chunk)
                            for symbols_chunk in self.chunk_list(symbols, MAX_SYMBOLS_PER_WEBSOCKET)]

    def _build_websocket(self, symbols: List[str]) -> websocket.WebSocketApp:
        logger.info(f'Building websocket for {len(symbols)} symbols')
        streams = '/'.join([f'{s.lower()}_perpetual@continuousKline_1m' for s in symbols])
        ws_url = f'wss://fstream.binance.com/ws/{streams}'
        return websocket.WebSocketApp(ws_url,
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

    @staticmethod
    def chunk_list(lst: List[str], chunk_size=200) -> List[List[str]]:
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

    @staticmethod
    def is_time_around_minute():
        now_seconds = datetime.now().second
        return now_seconds > 45 or now_seconds < 10
