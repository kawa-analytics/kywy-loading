import dataclasses
import queue
from typing import List, Optional

import websocket
import json
import time
import threading
import ssl
import binance

from datetime import datetime
from logging_config import get_logger

logger = get_logger(__name__)

AUTOMATICALLY_RECONNECTS_EVERY_S = 60 * 60
MAX_SUBJECTS_PER_WEBSOCKET = 300

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


@dataclasses.dataclass
class SymbolInterval:
    symbol: str
    interval: str


class BinanceWebSocketClient:

    def __init__(self,
                 binance_client,
                 live: bool,
                 candle: Optional[str],
                 candles: Optional[List[str]]):
        self._queue = queue.Queue()
        self._binance_client = binance_client
        self._websockets: List[websocket.WebSocketApp] = []
        self.renewing_websockets: bool = False
        self.symbols = []
        self.live: bool = live
        if (candle and candles) or (not candle and not candles):
            raise Exception('Websocket should define a single or a list of candles not both')
        if candle:
            logger.info(f'Websocket will listen to {candle} candle')
        if candles:
            logger.info(f'Websocket will listen to {candles} candles')
        self.candle_intervals = [candle] if candle else candles

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

        for i in range(5):
            try:
                self._load_symbols_or_use_previously_loaded_ones()
                continue
            except Exception as e:
                logger.warning(f'Issue when loading the symbols, try {i}/5: with error {e}')

        if not self.symbols:
            raise Exception('Could not load the symbols, stopping')

        logger.info(f'There are {len(self.symbols)} symbols to load')
        symbol_interval_tuples = [SymbolInterval(symbol, interval)
                                  for symbol in self.symbols
                                  for interval in self.candle_intervals]

        self._websockets = [self._build_websocket(symbol_interval_tuples_chunk)
                            for symbol_interval_tuples_chunk in
                            self.chunk_list(symbol_interval_tuples, MAX_SUBJECTS_PER_WEBSOCKET)]

    def _load_symbols_or_use_previously_loaded_ones(self):
        try:
            self.symbols = self._load_symbols()
        except Exception as e:
            if self.symbols:
                logger.warning(f'Issue when loading the symbols, using previously loaded symbols: {e}')
            else:
                raise e

    def _build_websocket(self, symbol_intervals: List[SymbolInterval]) -> websocket.WebSocketApp:
        logger.info(f'Building websocket for {len(symbol_intervals)} symbol_intervals')
        streams = '/'.join([f'{s.symbol.lower()}_perpetual@continuousKline_{s.interval}'
                            for s in symbol_intervals])
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
            if is_candle_closed or self.live:
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
    def chunk_list(lst: List, chunk_size=200) -> List[List]:
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

    @staticmethod
    def is_time_around_minute():
        now_seconds = datetime.now().second
        return now_seconds > 45 or now_seconds < 10
