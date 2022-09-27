

class MonthlyBitcoinPowerMixChartPoint(dict):
    def __init__(self, point):
        super().__init__(
            x=point['timestamp'],
            y=round(point['value'], 4),
            name=point['name'],
        )
