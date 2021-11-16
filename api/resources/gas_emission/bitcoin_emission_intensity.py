

class BitcoinEmissionIntensityChartPoint(dict):
    def __init__(self, point):
        super().__init__(
            x=point['timestamp'],
            y=round(point['co2_coef'], 2),
            name=point['name'],
        )
