
from datetime import datetime


class EmissionIntensityService:

    HISTORICAL = 'Historical'
    ESTIMATED = 'Estimated'
    PROVISIONAL = 'Provisional'

    def __init__(self, repository):
        self.repository = repository

    def get_bitcoin_emission_intensity(self):
        return self.repository.get_global_co2_coefficients()

    def create_chart_point(self, date: datetime, co2_coefficient: float, name: str):
        self.repository.create_co2_coefficient_record(date.strftime('%Y-%m-%d'), co2_coefficient, name)

    def create_provisional_point(self, date: datetime):
        co2_coefficient = self.repository.get_newest_co2_coefficient()
        self.create_chart_point(date, co2_coefficient, self.PROVISIONAL)
