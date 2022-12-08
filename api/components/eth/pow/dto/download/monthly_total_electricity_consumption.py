from calendar import month_name
from datetime import datetime


class MonthlyTotalElectricityConsumptionDto(dict):
    def __init__(self, point):
        date = datetime.fromtimestamp(point['timestamp'])
        super().__init__(
            timestamp=month_name[date.month] + date.strftime(' %Y'),
            consumption=round(point['guess_consumption'], 4),
            cumulative_consumption=round(point['cumulative_guess_consumption'], 4),
        )
