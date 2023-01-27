

class StatsResource(dict):
    def __init__(self, item):
        super().__init__({
            'guess_consumption': round(item['guess_consumption'] * 1_000_000, 2),
            'guess_power': round(item['guess_power'] * 1_000_000, 2),
            'max_consumption': round(item['max_consumption'] * 1_000_000, 2),
            'max_power': round(item['max_power'] * 1_000_000, 2),
            'min_consumption': round(item['min_consumption'] * 1_000_000, 2),
            'min_power': round(item['min_power'] * 1_000_000, 2),
        })
