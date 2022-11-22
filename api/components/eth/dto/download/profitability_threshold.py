from datetime import datetime


class ProfitabilityThresholdDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=datetime.fromtimestamp(point['timestamp']).isoformat(),
            efficiency=str(round(point['machine_efficiency'], 4)) + 'μ',
        )
