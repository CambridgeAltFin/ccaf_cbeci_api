from components.bitcoin_cost_of_minting.bitcoin_cost_of_minting_service import BitcoinCostOfMintingService
from components.bitcoin_cost_of_minting.bitcoin_cost_of_minting_repository import BitcoinCostOfMintingRepository


class BitcoinCostOfMintingRepositoryFactory:
    @staticmethod
    def create():
        return BitcoinCostOfMintingRepository()


class BitcoinCostOfMintingServiceFactory:
    @staticmethod
    def create():
        return BitcoinCostOfMintingService(
            repository=BitcoinCostOfMintingRepositoryFactory.create()
        )
