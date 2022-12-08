from .eth_repository import EthRepository
from .eth_service import EthService


class EthFactory:
    @staticmethod
    def create_repository() -> EthRepository:
        return EthRepository()

    @staticmethod
    def create_service():
        return EthService(
            repository=EthFactory.create_repository(),
        )
