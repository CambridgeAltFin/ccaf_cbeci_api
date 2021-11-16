
from components.gas_emission.gas_emission_repository import GasEmissionRepository
from components.gas_emission.emission_intensity_service import EmissionIntensityService


class EmissionIntensityServiceFactory:

    @staticmethod
    def create():
        return EmissionIntensityService(repository=GasEmissionRepository())
