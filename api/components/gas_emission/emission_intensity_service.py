
from components.gas_emission.gas_emission_repository import GasEmissionRepository

import pandas as pd
from datetime import datetime


class EmissionIntensityService:

    HISTORICAL = 'Historical'
    ESTIMATED = 'Estimated'
    PROVISIONAL = 'Provisional'

    def __init__(self, repository: GasEmissionRepository):
        self.repository = repository

    def get_bitcoin_emission_intensity(self):
        return self.repository.get_global_co2_coefficients()

    def create_chart_point(self, date: datetime, co2_coefficient: float, name: str):
        self.repository.create_co2_coefficient_record(date.strftime('%Y-%m-%d'), co2_coefficient, name)

    def create_provisional_points(self):
        last_record = self.repository.get_newest_co2_coefficient()
        for day in pd.date_range(start=last_record['date'], end=datetime.today()).tolist():
            self.repository.create_co2_coefficient_record(
                day.strftime('%Y-%m-%d'),
                last_record['co2_coef'],
                self.PROVISIONAL
            )
