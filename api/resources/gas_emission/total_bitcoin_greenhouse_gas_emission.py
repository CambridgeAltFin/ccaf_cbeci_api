

class TotalBitcoinGreenhouseGasEmissionChartPoint(dict):
    def __init__(self, point):
        super().__init__(
            x=int(point['timestamp']),
            y=round(point['v'], 2),
            cumulative_y=round(point['cumulative_v'], 2),
        )
