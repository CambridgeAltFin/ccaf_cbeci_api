

class BitcoinGreenhouseGasEmissionChartPoint(dict):
    def __init__(self, point):
        super().__init__(
            x=point['timestamp'],
            y=round(point['value'], 2),
            name=point['name'],
        )
