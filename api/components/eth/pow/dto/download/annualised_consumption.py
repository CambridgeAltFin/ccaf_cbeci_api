from datetime import datetime


class AnnualisedConsumptionDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=datetime.utcfromtimestamp(point['timestamp']).isoformat(),
            min_consumption=round(point['min_consumption'], 4),
            guess_consumption=round(point['guess_consumption'], 4),
            max_consumption=round(point['max_consumption'], 4),
        )
