from datetime import datetime


class MiningEquipmentEfficiencyDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=datetime.fromtimestamp(point['timestamp']).isoformat(),
            name=point['name'],
            efficiency=round(point['efficiency'], 4),
        )
