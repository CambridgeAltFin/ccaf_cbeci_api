from datetime import datetime


class NetworkPowerDemandDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=datetime.utcfromtimestamp(point['timestamp']).isoformat(),
            min_power=round(point['min_power'], 4),
            guess_power=round(point['guess_power'], 4),
            max_power=round(point['max_power'], 4),
        )
