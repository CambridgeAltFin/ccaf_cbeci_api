
from components.energy_consumption.energy_consumption_service import EnergyConsumptionService
from components.energy_consumption.energy_consumption_repository import EnergyConsumptionRepository
from components.energy_consumption.energy_consumption_calculator import EnergyConsumptionCalculator


class EnergyConsumptionRepositoryFactory:
    @staticmethod
    def create():
        return EnergyConsumptionRepository()


class EnergyConsumptionCalculatorFactory:
    @staticmethod
    def create():
        return EnergyConsumptionCalculator()


class EnergyConsumptionServiceFactory:
    @staticmethod
    def create():
        return EnergyConsumptionService(
            energy_consumption_calculator=EnergyConsumptionCalculatorFactory.create(),
            repository=EnergyConsumptionRepositoryFactory.create()
        )
