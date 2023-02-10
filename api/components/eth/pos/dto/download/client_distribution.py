from datetime import datetime


class ClientDistributionDto(dict):
    def __init__(self, point):
        timestamp = point.pop('timestamp')
        for node in point:
            point[node] = str(round(point[node] * 100, 2)) + '%'
        super().__init__(
            timestamp=datetime.utcfromtimestamp(timestamp).isoformat(),
            **point,
        )
