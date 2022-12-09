class ComparisonOfAnnualConsumptionDto(dict):
    def __init__(self, point):
        super().__init__(
            name=self.get_name(point['asset']),
            guess_consumption=self.get_consumption(point['asset'], point['guess_consumption']),
        )

    def get_name(self, asset):
        name_map = {
            'eth': 'Ethereum 1.0',
            'eth_pos': 'Ethereum',
            'btc': 'Bitcoin',
        }
        return name_map.get(asset, asset)

    def get_consumption(self, asset, consumption):
        if asset == 'eth_pos':
            return round(consumption, 3)
        return round(consumption, 2)
