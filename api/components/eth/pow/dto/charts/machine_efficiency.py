class MachineEfficiencyDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=point['timestamp'],
            label=point['name'],
            efficiency=round(point['efficiency'], 2),
        )
