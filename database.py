import peewee as pw
from config import config
from logger import get_logger
logger = get_logger(__name__)


db = pw.MySQLDatabase(
    config.get('Database', 'DBName'),
    user=config.get('Database', 'DBUser'),
    password=config.get('Database', 'DBPass'),
    charset='utf8mb4'
)


class BaseModel(pw.Model):
    class Meta:
        database = db


class DBArbResult(BaseModel):
    # triangle: e.g. "BTC ETH XRP" (alphabetically ordered)
    triangle = pw.CharField()
    # parallels: 1, 2 or 3, number of parallel actions on the first step
    parallels = pw.IntegerField()
    # scenario: failed, unfilled, reverted N, finalized, normal
    scenario = pw.CharField()
    # A, B, C are the currencies of the triangle (in the order as specified above)
    profit_a = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    profit_b = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    profit_c = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    # action filling: -1 failed, 0 unfilled, 0.* partially filled, 1.0 filled
    filling_1 = pw.FloatField()
    filling_2 = pw.FloatField()
    filling_3 = pw.FloatField()
    # timings in milliseconds
    # placement is measured from arbitrage detection
    all_placed_in = pw.IntegerField()
    placed_1_in = pw.IntegerField()
    placed_2_in = pw.IntegerField()
    placed_3_in = pw.IntegerField()
    # filling time of each order
    done_1_in = pw.IntegerField()
    done_2_in = pw.IntegerField()
    done_3_in = pw.IntegerField()
    # total time from detection to completion of arbitrage execution
    completed_in = pw.IntegerField()

    class Meta:
        table_name = 'arb_result'


def init_db():
    logger.info('Initializing the database...')
    db.create_tables([DBArbResult], safe=True)
    logger.info('Database initialized')


if __name__ == '__main__':
    init_db()
