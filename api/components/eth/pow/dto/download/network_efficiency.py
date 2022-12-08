from datetime import datetime


class NetworkEfficiencyDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=datetime.fromtimestamp(point['timestamp']).isoformat(),
            miners='; '.join([f'{miner}: {round(float(eff), 4)}' for [miner, eff] in point['miners']]),
            efficiency=round(point['efficiency'], 4),
        )
