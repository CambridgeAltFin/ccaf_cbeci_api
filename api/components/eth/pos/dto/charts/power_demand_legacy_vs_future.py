class PowerDemandLegacyVsFutureDto(dict):
    def __init__(self, point):
        super().__init__(
            name=self.get_name(point['asset']),
            guess_power=round(point['guess_power'], 2),
        )

    def get_name(self, asset):
        name_map = {
            'eth': 'Ethereum 1.0',
            'eth_pos': 'Ethereum'
        }

        return name_map.get(asset, asset)
