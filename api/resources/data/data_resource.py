
from datetime import datetime


class DataResource(dict):
    def __init__(self, timestamp, item):
        super().__init__(
            timestamp=timestamp,
            date=datetime.utcfromtimestamp(timestamp).isoformat(),
            guess_power=round(item['guess_power'], 2),
            max_power=round(item['max_power'], 2),
            min_power=round(item['min_power'], 2),
            guess_consumption=round(item['guess_consumption'], 2),
            max_consumption=round(item['max_consumption'], 2),
            min_consumption=round(item['min_consumption'], 2),
        )
