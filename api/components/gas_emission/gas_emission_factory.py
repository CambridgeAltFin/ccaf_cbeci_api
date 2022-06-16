
from components.gas_emission.power_mix_service import PowerMixService
from components.energy_consumption import EnergyConsumptionServiceFactory
from components.gas_emission.gas_emission_repository import GasEmissionRepository
from components.gas_emission.emission_intensity_service import EmissionIntensityService
from components.gas_emission.greenhouse_gas_emission_service import GreenhouseGasEmissionService
from components.gas_emission.emission_service import EmissionService


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


class PowerMixServiceFactory:
    @staticmethod
    def create():
        return PowerMixService(
            repository=GasEmissionRepositoryFactory.create(),
        )


class EmissionServiceFactory:
    @staticmethod
    def create():
        return EmissionService(
            repository=GasEmissionRepositoryFactory.create()
        )
