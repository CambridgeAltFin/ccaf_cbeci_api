class ProfitabilityThresholdDto(dict):
    def __init__(self, point):
        super().__init__(
            x=point['timestamp'],
            y=round(point['machine_efficiency'], 6),
        )
