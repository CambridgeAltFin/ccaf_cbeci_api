

class GhgHistoricalEmission(dict):
    def __init__(self, emission):
        super().__init__(
            code=emission['code'],
            name=emission['name'],
            year=int(emission['year']),
            value=round(float(emission['value']), 2)
        )
