
from components.energy_consumption import EnergyConsumptionServiceFactory
from components.gas_emission.gas_emission_repository import GasEmissionRepository
from components.gas_emission.emission_intensity_service import EmissionIntensityService
from components.gas_emission.greenhouse_gas_emission_service import GreenhouseGasEmissionService


class GasEmissionRepositoryFactory:
    @staticmethod
    def create():
        return GasEmissionRepository()


class EmissionIntensityServiceFactory:
    @staticmethod
    def create():
        return EmissionIntensityService(repository=GasEmissionRepositoryFactory.create())


class GreenhouseGasEmissionServiceFactory:
    @staticmethod
    def create():
        return GreenhouseGasEmissionService(
            repository=GasEmissionRepositoryFactory.create(),
            energy_consumption=EnergyConsumptionServiceFactory.create()
        )
