from datetime import timezone, datetime


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


class MonthlyNodeDistributionDto(dict):
    def __init__(self, point):
        super().__init__(
            name=point['name'],
            code=point['code'],
            flag=point['flag'],
            number_of_nodes=float(round(point['number_of_nodes'], 2)),
            country_share=float(round(point['country_share'], 6)),
            timestamp=int(datetime.strptime(point['date'], '%Y-%m').replace(
                day=1,
                hour=0,
                minute=0,
                tzinfo=timezone.utc
            ).timestamp()),
        )
