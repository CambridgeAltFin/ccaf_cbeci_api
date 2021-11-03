

class StatsResource(dict):
    def __init__(self, item):
        super().__init__({key: round(value, 2) for key, value in item.items()})
