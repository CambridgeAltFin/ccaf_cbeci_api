class StatsDto(dict):
    def __init__(self, stats):
        super().__init__({
            'min_power': round(stats.get('min_power', 0) * 1_000_000, 2),
            'guess_power': round(stats.get('guess_power', 0) * 1_000_000, 2),
            'max_power': round(stats.get('max_power', 0) * 1_000_000, 2),
            'min_consumption': round(stats.get('min_consumption', 0) * 1_000_000, 2),
            'guess_consumption': round(stats.get('guess_consumption', 0) * 1_000_000, 2),
            'max_consumption': round(stats.get('max_consumption', 0) * 1_000_000, 2),
        })
