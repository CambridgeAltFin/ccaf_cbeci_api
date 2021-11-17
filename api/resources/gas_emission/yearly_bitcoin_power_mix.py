

class YearlyBitcoinPowerMixChartPoint(dict):
    def __init__(self, point):
        super().__init__(
            y=point['timestamp'],
            x=round(point['value'], 2),
            name=point['name'],
        )
