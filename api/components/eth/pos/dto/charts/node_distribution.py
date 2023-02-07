from datetime import timezone


class NodeDistributionDto(dict):
    def __init__(self, point):
        super().__init__(
            name=point['name'],
            code=point['code'],
            flag=point['flag'],
            number_of_nodes=point['number_of_nodes'],
            country_share=float(round(point['country_share'], 6)),
            timestamp=int(point['date'].replace(hour=0, minute=0, tzinfo=timezone.utc).timestamp()),
        )
