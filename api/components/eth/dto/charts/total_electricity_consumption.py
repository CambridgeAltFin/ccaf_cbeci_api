class TotalElectricityConsumptionDto(dict):
    def __init__(self, point):
        super().__init__(
            timestamp=point['timestamp'],
            consumption=round(point['guess_consumption'], 2),
            cumulative_consumption=round(point['cumulative_guess_consumption'], 2),
        )
