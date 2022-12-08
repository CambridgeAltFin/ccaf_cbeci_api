class ProfitabilityThresholdDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=point['timestamp'],
            efficiency=round(point['machine_efficiency'], 2),
        )
