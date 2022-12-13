class AnnualisedConsumptionDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=point['timestamp'],
            min_consumption=round(point['min_consumption'], 2),
            guess_consumption=round(point['guess_consumption'], 2),
            max_consumption=round(point['max_consumption'], 2),
        )
