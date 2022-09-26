from datetime import datetime
from dateutil import tz


class TotalYearlyBitcoinGreenhouseGasEmissionChartPoint(dict):
    def __init__(self, point, date):
        super().__init__(
            x=int(datetime(date.year, 1, 1, tzinfo=tz.tzutc()).timestamp()),
            y=round(point['v'], 2),
            cumulative_y=round(point['cumulative_v'], 2),
        )
