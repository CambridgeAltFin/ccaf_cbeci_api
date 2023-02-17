from components.carbon_accounting_tool.repository import CarbonAccountingToolRepository
from components.carbon_accounting_tool.service import CarbonAccountingToolService


class CarbonAccountingToolFactory:

    @staticmethod
    def create_repository():
        return CarbonAccountingToolRepository()

    @staticmethod
    def create_service():
        return CarbonAccountingToolService(CarbonAccountingToolFactory.create_repository())
