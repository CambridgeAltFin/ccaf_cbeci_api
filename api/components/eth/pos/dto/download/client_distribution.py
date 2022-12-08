from datetime import datetime


class ClientDistributionDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=datetime.utcfromtimestamp(point.pop('timestamp')).isoformat(),
            **point,
        )
