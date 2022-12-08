class ActiveNodeDto(dict):
    def __init__(self, point):
        super().__init__(
            x=point['timestamp'],
            y=point['total'],
        )
