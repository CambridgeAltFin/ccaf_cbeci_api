from calendar import month_name
from datetime import datetime


class YearlyTotalElectricityConsumptionDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=datetime.fromtimestamp(point['timestamp']).strftime('%Y'),
            consumption=round(point['guess_consumption'], 4),
            cumulative_consumption=round(point['cumulative_guess_consumption'], 4),
        )
