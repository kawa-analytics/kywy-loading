import queue
import time
import pyarrow as pa
import threading
from datetime import datetime, timedelta
from kywy.client.kawa_client import KawaClient as K

from logging_config import get_logger

logger = get_logger(__name__)

RETENTION_IN_DAYS = 7
DELAY_BETWEEN_TWO_ITERATIONS = 5


class KywyConsumer:

    def __init__(self,
                 data_queue: queue.Queue,
                 datasource_name: str):
        self._queue = data_queue
        self._datasource_name = datasource_name
        self._datasource_id = None
        self._kawa = K.load_client_from_environment()
        self.main_event_loop_iter = 0

    def start(self):
        consumer_thread = threading.Thread(
            target=self.run,
            name="kawa_appender",
            daemon=True
        )
        consumer_thread.start()

    def run(self):
        self._init_datasource()
        logger.info('Starting the consumer')

        # Main event loop
        while True:
            time.sleep(DELAY_BETWEEN_TWO_ITERATIONS)
            logger.debug('Will process the queue')
            self._process_queue()

            if self.main_event_loop_iter % 20 == 0:
                self._remove_old_partitions()

            self.main_event_loop_iter += 1

    def _init_datasource(self):
        logger.info('Initializing the datasource on kawa')
        arrow_table = self._build_arrow_tables(SEED_DATA)
        loader = self._kawa.new_arrow_data_loader(
            datasource_name=self._datasource_name,
            arrow_table=arrow_table
        )
        created_datasource = loader.create_datasource()
        self._datasource_id = created_datasource['id']

        # Partition by date to allow proper retention policy
        self._kawa.commands.replace_datasource_primary_keys(
            datasource=self._datasource_id,
            new_primary_keys=['date', 'record_id'],
            partition_key='date',
        )

    def _process_queue(self):
        records = []
        while not self._queue.empty():
            records.append(self._queue.get())

        if not records:
            logger.debug('Empty queue')
            return

        for i in range(1, 30):
            logger.info('(ITERATION {})Processing {} elements'.format(i, len(records)))
            try:
                arrow_table = self._build_arrow_tables(records)
                loader = self._kawa.new_arrow_data_loader(
                    datasource_name=self._datasource_name,
                    arrow_table=arrow_table
                )
                loader.load_data(
                    reset_before_insert=False,
                    create_sheet=False,
                    optimize_after_insert=False
                )
                return
            except Exception as e:
                logger.error("Impossible to send data to KAWA:", e)
                retry_in = i * 5
                logger.info(f"Will retry in {retry_in} seconds")
                time.sleep(retry_in)

    def _remove_old_partitions(self):
        cutoff_date = (datetime.today() - timedelta(days=RETENTION_IN_DAYS)).date()
        logger.info(f'Will remove everything before {cutoff_date}')
        self._kawa.commands.delete_data(
            datasource=self._datasource_id,
            delete_where=[
                K.where('date').date_range(to_inclusive=cutoff_date)
            ])

    @staticmethod
    def _build_arrow_tables(records):
        columns = {
            key: pa.array([item.get(key) for item in records], type=SCHEMA.field(key).type) for key in SCHEMA.names
        }
        return pa.table(columns, schema=SCHEMA)


SCHEMA = pa.schema([
    ('symbol', pa.string()),
    ('exchange', pa.string()),
    ('contract', pa.string()),
    ('interval', pa.string()),
    ('timestamp', pa.timestamp(unit='ns', tz='UTC')),
    ('date', pa.date32()),
    ('start_time', pa.timestamp(unit='ns', tz='UTC')),
    ('end_time', pa.timestamp(unit='ns', tz='UTC')),
    ('first_trade_id', pa.int64()),
    ('last_trade_id', pa.int64()),
    ('open', pa.float64()),
    ('close', pa.float64()),
    ('high', pa.float64()),
    ('low', pa.float64()),
    ('base_asset_volume', pa.float64()),
    ('quote_asset_volume', pa.float64()),
    ('taker_buy_base_asset_volume', pa.float64()),
    ('taker_buy_quote_asset_volume', pa.float64()),
    ('num_trades', pa.int64()),
])

SEED_DATA = [
    {
        'start_time': datetime.fromtimestamp(1730680620000 / 1000),
        'end_time': datetime.fromtimestamp(1730680679999 / 1000),
        'symbol': 'ORDIUSDT',
        'exchange': 'BINANCE',
        'contract': 'PERPETUAL',
        'timestamp': datetime.fromtimestamp(1730680680017 / 1000),
        'date': datetime.fromtimestamp(1730680680017 / 1000).date(),
        'interval': '1m',
        'first_trade_id': 5662323094075,
        'last_trade_id': 5662329969760,
        'open': 31.232000,
        'close': 31.167000,
        'high': 31.242000,
        'low': 31.162000,
        'base_asset_volume': 7978.9,
        'quote_asset_volume': 248836.4747000,
        'taker_buy_base_asset_volume': 2709.3,
        'taker_buy_quote_asset_volume': 84524.6712000,
        'num_trades': 1190,
    }
]
