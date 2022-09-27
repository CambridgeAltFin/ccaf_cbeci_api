from datetime import datetime
from dateutil import tz


class YearlyDataResource(dict):
    def __init__(self, date, item):
        super().__init__(
            timestamp=int(datetime(date.year, 1, 1, tzinfo=tz.tzutc()).timestamp()),
            year=date.strftime('%Y'),
            value=round(item['guess_consumption'], 2),
            cumulative_value=round(item['cumulative_guess_consumption'], 2),
        )
