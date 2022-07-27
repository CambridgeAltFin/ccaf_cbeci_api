
from components.gas_emission.co2_statistic import Co2Statistic
from components.gas_emission.gas_emission_repository import GasEmissionRepository
from components.gas_emission.emission_intensity_service import EmissionIntensityService
from components.energy_consumption.energy_consumption_service import EnergyConsumptionService

import numpy as np
import pandas as pd
from datetime import datetime


class GreenhouseGasEmissionService:

    HISTORICAL_MIN_CO2 = 'Historical Hydro-only'
    HISTORICAL_MAX_CO2 = 'Historical Coal-only'
    HISTORICAL_GUESS_CO2 = 'Historical Estimated'

    MIN_CO2 = 'Hydro-only'
    MAX_CO2 = 'Coal-only'
    GUESS_CO2 = 'Estimated'

    PROVISIONAL_MIN_CO2 = 'Provisional Hydro-only'
    PROVISIONAL_MAX_CO2 = 'Provisional Coal-only'
    PROVISIONAL_GUESS_CO2 = 'Provisional Estimated'

    def __init__(self, repository: GasEmissionRepository, energy_consumption: EnergyConsumptionService):
        self.repository = repository
        self.energy_consumption = energy_consumption

    def get_greenhouse_gas_emissions(self, price: float):
        return self.repository.get_greenhouse_gas_emissions(price) \
            if self.is_calculated(price) \
            else self.calc_greenhouse_gas_emissions(price).to_dict('records')

    def get_flat_greenhouse_gas_emissions(self, price: float):
        return self.repository.get_flat_greenhouse_gas_emissions(price) \
            if self.is_calculated(price) \
            else self.calc_flat_greenhouse_gas_emissions(price).to_dict('records')

    @staticmethod
    def is_calculated(price: float) -> bool:
        return int(price * 100) in range(1, 21)

    def calc_greenhouse_gas_emissions(self, price: float) -> pd.DataFrame:
        pivot = self.join_energy_consumption_to_co2_coefficients(price)

        his = pivot[pivot['name'] == EmissionIntensityService.HISTORICAL]
        est = pivot[pivot['name'] == EmissionIntensityService.ESTIMATED]
        prov = pivot[pivot['name'] == EmissionIntensityService.PROVISIONAL]

        his_min = his[['timestamp', 'date', 'name', 'min_co2']]
        his_min.loc[:, 'name'] = self.HISTORICAL_MIN_CO2
        his_min.loc[:, 'value'] = his_min['min_co2']

        his_guess = his[['timestamp', 'date', 'name', 'guess_co2']]
        his_guess.loc[:, 'name'] = self.HISTORICAL_GUESS_CO2
        his_guess.loc[:, 'value'] = his_guess['guess_co2']

        his_max = his[['timestamp', 'date', 'name', 'max_co2']]
        his_max.loc[:, 'name'] = self.HISTORICAL_MAX_CO2
        his_max.loc[:, 'value'] = his_max['max_co2']

        est_min = est[['timestamp', 'date', 'name', 'min_co2']]
        est_min.loc[:, 'name'] = self.MIN_CO2
        est_min.loc[:, 'value'] = est_min['min_co2']

        est_guess = est[['timestamp', 'date', 'name', 'guess_co2']]
        est_guess.loc[:, 'name'] = self.GUESS_CO2
        est_guess.loc[:, 'value'] = est_guess['guess_co2']

        est_max = est[['timestamp', 'date', 'name', 'max_co2']]
        est_max.loc[:, 'name'] = self.MAX_CO2
        est_max.loc[:, 'value'] = est_max['max_co2']

        prov_min = prov[['timestamp', 'date', 'name', 'min_co2']]
        prov_min.loc[:, 'name'] = self.PROVISIONAL_MIN_CO2
        prov_min.loc[:, 'value'] = prov_min['min_co2']

        prov_guess = prov[['timestamp', 'date', 'name', 'guess_co2']]
        prov_guess.loc[:, 'name'] = self.PROVISIONAL_GUESS_CO2
        prov_guess.loc[:, 'value'] = prov_guess['guess_co2']

        prov_max = prov[['timestamp', 'date', 'name', 'max_co2']]
        prov_max.loc[:, 'name'] = self.PROVISIONAL_MAX_CO2
        prov_max.loc[:, 'value'] = prov_max['max_co2']

        df = pd.DataFrame()
        df = df.append(his_min, ignore_index=True)
        df = df.append(his_guess, ignore_index=True)
        df = df.append(his_max, ignore_index=True)
        df = df.append(est_min, ignore_index=True)
        df = df.append(est_guess, ignore_index=True)
        df = df.append(est_max, ignore_index=True)
        df = df.append(prov_min, ignore_index=True)
        df = df.append(prov_guess, ignore_index=True)
        df = df.append(prov_max, ignore_index=True)

        return df

    def calc_flat_greenhouse_gas_emissions(self, price: float) -> pd.DataFrame:
        result = self.join_energy_consumption_to_co2_coefficients(price)[['date', 'min_co2', 'guess_co2', 'max_co2', 'timestamp']]
        result['date'] = result['date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return result

    def join_energy_consumption_to_co2_coefficients(self, price: float) -> pd.DataFrame:
        def to_dict(timestamp, row):
            row['date'] = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
            return row

        energy_consumption = pd.DataFrame([to_dict(timestamp, row) for timestamp, row in self.energy_consumption.get_data(price)])
        co2_coefficients = pd.DataFrame(self.repository.get_global_co2_coefficients())

        energy_consumption['date'] = pd.to_datetime(energy_consumption['date'])
        co2_coefficients['date'] = pd.to_datetime(co2_coefficients['date'])
        pivot = pd.merge(energy_consumption, co2_coefficients, how='inner', on='date')

        pivot['max_co2'] = (pivot['guess_consumption'] * 10 ** 9 * Co2Statistic.coal()) / 10 ** 12  # 10**6 = conversion to tons and 10**9 # conversion from twh to kwh
        pivot['min_co2'] = (pivot['guess_consumption'] * 10 ** 9 * Co2Statistic.hydro()) / 10 ** 12
        pivot['guess_co2'] = (pivot['guess_consumption'] * 10 ** 9 * pivot['co2_coef']) / 10 ** 12

        pivot['timestamp'] = pivot['date'].values.astype(np.int64) // 10 ** 9

        return pivot

    def get_total_greenhouse_gas_emissions(self, price: float):
        return self.repository.get_total_greenhouse_gas_emissions(price) \
            if self.is_calculated(price) \
            else self.calc_total_greenhouse_gas_emissions(price).to_dict('records')

    def calc_total_greenhouse_gas_emissions(self, price: float):
        def to_dict(date, row):
            row['date'] = date.strftime('%Y-%m-%d')
            return row

        energy_cumulative = pd.DataFrame([
            to_dict(date, row) for date, row
            in self.energy_consumption.get_monthly_data(price, current_month=True)
        ])
        energy_cumulative['date'] = pd.to_datetime(energy_cumulative['date'])
        energy_cumulative['timestamp'] = energy_cumulative['date'].values.astype(np.int64) // 10 ** 9
        energy_cumulative['month'] = pd.to_datetime(
            energy_cumulative['date'].dt.year.astype('str') + '-' + energy_cumulative['date'].dt.month.astype('str')
        )
        co2_coefficients = pd.DataFrame(self.repository.get_total_global_co2_coefficients())
        co2_coefficients['month'] = pd.to_datetime(co2_coefficients['month'])

        pivot = energy_cumulative.merge(co2_coefficients, how='inner', on='month')
        pivot['v'] = (pivot['guess_consumption'] * 10 ** 9 * pivot['co2_coef']) / 10 ** 12
        pivot['cumulative_v'] = pivot['v'].cumsum()

        return pivot[['timestamp', 'date', 'v', 'cumulative_v']]
