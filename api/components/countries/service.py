from .repository import CountryRepository


class CountryService:

    def __init__(self, countries: CountryRepository):
        self.countries = countries

    def update_btc_value(self, value):
        self.countries.update_by_code('BTC', value)

    def get_btc_index(self):
        countries = self.countries.all()
        return next((i for i, item in enumerate(countries) if item['code'] == 'BTC'), -1) + 1
