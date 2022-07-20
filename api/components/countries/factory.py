from components.countries.repository import CountryRepository
from components.countries.service import CountryService


class CountryFactory:

    @staticmethod
    def create_repository():
        return CountryRepository()

    @staticmethod
    def create_service():
        return CountryService(CountryFactory.create_repository())
