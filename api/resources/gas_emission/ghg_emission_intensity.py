

class GhgEmissionIntensity(dict):
    def __init__(self, emission):
        super().__init__(
            code=emission['code'],
            name=emission['name'],
            value=float(emission['value']) if emission['value'] is not None else None,
            flag=emission['flag']
        )
