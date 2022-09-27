
from components.gas_emission.gas_emission_repository import GasEmissionRepository


class PowerMixService:

    def __init__(self, repository: GasEmissionRepository):
        self.repository = repository

    def get_monthly_data(self):
        return self.repository.get_monthly_bitcoin_power_mix()

    def get_yearly_data(self):
        return self.repository.get_yearly_bitcoin_power_mix()
