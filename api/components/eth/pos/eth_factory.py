from .eth_repository import EthRepository


class EthFactory:
    @staticmethod
    def create_repository() -> EthRepository:
        return EthRepository()
