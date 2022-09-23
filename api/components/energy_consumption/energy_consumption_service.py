
from components.energy_consumption.energy_consumption_calculator import EnergyConsumptionCalculator
from components.energy_consumption.energy_consumption_repository import EnergyConsumptionRepository
from components.energy_consumption.v1_1_1.energy_consumption_service import EnergyConsumptionService as EnergyConsumptionService_v1_1_1

from typing import Any
from datetime import datetime
import pandas as pd


class EnergyConsumptionService(EnergyConsumptionService_v1_1_1):
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    default_price = 0.05

    def __init__(
            self,
            energy_consumption_calculator: EnergyConsumptionCalculator,
            repository: EnergyConsumptionRepository
    ):
        super().__init__(energy_consumption_calculator=energy_consumption_calculator, repository=repository)

        self.calculated_prices = range(1, 21)

    def get_data(self, price: float):
        return map(lambda row: (row['timestamp'], row), self.repository.get_energy_consumptions(price)) \
            if self.is_calculated(price) \
            else self.calc_data(price)

    def is_calculated(self, price: float) -> bool:
        return int(price * 100) in self.calculated_prices

    def get_monthly_data(self, price: float, current_month=True):
        return map(
            lambda row: (datetime.combine(row['date'], datetime.min.time()), row),
            self.repository.get_cumulative_energy_consumptions(price, current_month=current_month)
        ) if self.is_calculated(price) else self.calc_monthly_data(price, current_month=current_month)

    def get_actual_data(self, price: float):
        result = self.repository.get_actual_energy_consumption(price)\
            if self.is_calculated(price)\
            else self.calc_data(price)

        return list(result)[-1]

    def _get_equipment_list(
            self,
            price: float,
            timestamp: int,
            prof_threshold_value: float,
            miners: list
    ) -> list[dict[Any, Any]]:
        equipment_list = []
        price_coefficient = self.default_price / price

        for miner in miners:
            if miner['unix_date_of_release'] < timestamp < miner['five_years_after_release'] \
                    and prof_threshold_value * price_coefficient > miner['efficiency_j_gh']:
                equipment_list.append(dict(miner))

        return equipment_list

    def _get_energy_dataframe(self, price: float, metrics):
        miners = self.repository.get_miners()
        hash_rates = pd.DataFrame(self.repository.get_hash_rates()).drop('date', axis=1).set_index('timestamp')

        data = [
            self._get_date_metrics(
                price,
                timestamp,
                row['value'],
                metrics,
                miners,
                None,
                None,
                hash_rates
            ) for timestamp, row in self._get_prof_thresholds_dataframe().iterrows()
        ]

        return self._data_to_energy_dataframe(data)
