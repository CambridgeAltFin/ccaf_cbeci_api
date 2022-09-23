

class TotalYearlyBitcoinGreenhouseGasEmissionChartPoint(dict):
    def __init__(self, point, date):
        super().__init__(
            x=int(date.timestamp()),
            y=round(point['v'], 2),
            cumulative_y=round(point['cumulative_v'], 2),
        )
