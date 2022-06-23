
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

    def get_emission_intensities(self):
        emission_intensities = self.repository.get_emission_intensities()
        return emission_intensities
