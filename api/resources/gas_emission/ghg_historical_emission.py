

class GhgHistoricalEmission(dict):
    def __init__(self, emission):
        super().__init__(
            code=emission['code'],
            name=emission['name'],
            year=emission['year'],
            value=float(emission['value'])
        )
