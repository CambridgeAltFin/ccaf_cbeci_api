
from components.gas_emission.gas_emission_repository import GasEmissionRepository


class EmissionService:

    def __init__(self, repository: GasEmissionRepository):
        self.repository = repository

    def get_global_emissions(self):
        emission = self.repository.get_actual_world_emission()
        print(emission)
        return emission
