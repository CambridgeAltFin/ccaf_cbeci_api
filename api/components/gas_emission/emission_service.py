
from components.gas_emission.gas_emission_repository import GasEmissionRepository

from datetime import datetime


class EmissionService:

    def __init__(self, repository: GasEmissionRepository):
        self.repository = repository

    def get_annualised_emission(self):
        world_emission = self.repository.get_actual_world_emission()
        btc_emission = self.repository.get_annualised_btc_greenhouse_gas_emissions(datetime.now().year - 1)
        return world_emission, btc_emission

    def get_emissions(self):
        emissions = self.repository.get_emissions()
        btc_emission = self.repository.get_actual_btc_greenhouse_gas_emission()
        emissions.append(btc_emission)
        emissions.sort(key=lambda i: -i['value'])
        return emissions

    def get_btc_index(self):
        return next((i for i, item in enumerate(self.get_emissions()) if item['code'] == 'BTC'), -1) + 1

    def get_emission_intensities(self):
        emission_intensities = self.repository.get_emission_intensities()
        return emission_intensities

    def get_best_guess_and_digiconomist(self):
        actual = self.repository.get_actual_btc_greenhouse_gas_emission()
        digiconomist = self.repository.get_digiconomist_emission()
        return {
            'index': {'best_guess': round(actual['value'], 2)},
            'digiconomist': digiconomist,
        }
