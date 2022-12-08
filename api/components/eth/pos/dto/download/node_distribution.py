

class NodeDistributionDto(dict):
    def __init__(self, point):
        super().__init__(
            name=point['name'],
            number_of_nodes=point['number_of_nodes'],
        )
