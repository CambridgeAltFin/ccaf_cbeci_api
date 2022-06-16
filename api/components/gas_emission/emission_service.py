
from components.gas_emission.gas_emission_repository import GasEmissionRepository


class EmissionService:

    def __init__(self, repository: GasEmissionRepository):
        self.repository = repository

    def get_world_emission(self):
        emission = self.repository.get_actual_world_emission()
        return emission

    def get_emissions(self, limit=None):
        emissions = self.repository.get_emissions(limit)
        return emissions

    def get_emission_intensities(self):
        emission_intensities = self.repository.get_emission_intensities()
        return emission_intensities
