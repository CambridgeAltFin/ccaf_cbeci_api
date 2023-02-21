class MiningEquipmentEfficiencyDto(dict):
    def __init__(self, point):
        super().__init__(
            name=point['name'],
            x=point['timestamp'],
            y=round(point['efficiency'], 4),
        )
