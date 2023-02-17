from components.carbon_accounting_tool.repository import CarbonAccountingToolRepository
from components.carbon_accounting_tool.service import CarbonAccountingToolService
from components.carbon_accounting_tool.validator import CarbonAccountingToolValidator


class CarbonAccountingToolFactory:

    @staticmethod
    def create_repository():
        return CarbonAccountingToolRepository()

    @staticmethod
    def create_service():
        return CarbonAccountingToolService(CarbonAccountingToolFactory.create_repository())

    @staticmethod
    def create_validator():
        return CarbonAccountingToolValidator()
