

class NodeDistributionMetaDto(dict):
    def __init__(self, meta):
        super().__init__(
            min=meta.get('min').strftime('%Y-%m-%d') if meta and meta.get('min') else None,
            max=meta.get('max').strftime('%Y-%m-%d') if meta and meta.get('max') else None,
        )


class MonthlyNodeDistributionMetaDto(dict):
    def __init__(self, meta):
        super().__init__(
            min=meta.get('min'),
            max=meta.get('max'),
        )
