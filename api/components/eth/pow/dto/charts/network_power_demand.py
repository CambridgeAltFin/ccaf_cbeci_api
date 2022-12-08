class NetworkPowerDemandDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=point['timestamp'],
            min_power=round(point['min_power'], 2),
            guess_power=round(point['guess_power'], 2),
            max_power=round(point['max_power'], 2),
        )
