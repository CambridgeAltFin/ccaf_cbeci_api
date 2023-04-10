

class NodeDistributionDto(dict):
    def __init__(self, point):
        super().__init__(
            date=point['date'],
            name=point['name'],
            number_of_nodes=point['number_of_nodes'],
            country_share=str(round(point['country_share'] * 100, 2)) + '%',
        )


class MonthlyNodeDistributionDto(dict):
    def __init__(self, point):
        super().__init__(
            date=point['date'],
            name=point['name'],
            number_of_nodes=str(round(point['number_of_nodes'], 2)),
            country_share=str(round(point['country_share'] * 100, 2)) + '%',
        )
