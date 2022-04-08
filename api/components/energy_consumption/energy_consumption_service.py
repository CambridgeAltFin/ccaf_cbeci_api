
from components.energy_consumption.energy_consumption_calculator import EnergyConsumptionCalculator
from components.energy_consumption.energy_consumption_repository import EnergyConsumptionRepository
from components.energy_consumption.v1_1_1.energy_consumption_service import EnergyConsumptionService as EnergyConsumptionService_v1_1_1

from typing import List, Dict, Union
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

    def get_monthly_data(self, price: float):
        return map(lambda row: (datetime.combine(row['date'], datetime.min.time()), row), self.repository.get_cumulative_energy_consumptions(price)) \
            if self.is_calculated(price) \
            else self.calc_monthly_data(price)

    def _get_profitability_equipment(
            self,
            price: float,
            timestamp: int,
            prof_threshold_value: float,
            miners: list
    ) -> List[float]:
        profitability_equipment = []
        price_coefficient = self.default_price / price

        for miner in miners:
            if timestamp > miner['unix_date_of_release'] and prof_threshold_value * price_coefficient > miner['efficiency_j_gh']:
                profitability_equipment.append(miner['efficiency_j_gh'])

        return profitability_equipment

    def _get_date_metrics(
            self,
            price: float,
            timestamp: int,
            prof_threshold_value: float,
            metrics: List,
            miners: list,
            hash_rates_df: pd.DataFrame
    ) -> Union[Dict[str, float], None]:
        profitability_equipment = self._get_profitability_equipment(price, timestamp, prof_threshold_value, miners)

        result = {
            'date': datetime.utcfromtimestamp(timestamp).isoformat(),
            'timestamp': timestamp,
            'profitability_equipment': EnergyConsumptionCalculator.get_avg(profitability_equipment)
        }

        if len(profitability_equipment) == 0:
            return result | {metrics[i]: None for i in range(len(metrics))}

        if 'max_consumption' in metrics:
            result['max_consumption'] = self.energy_consumption_calculator.max_consumption(
                profitability_equipment,
                hash_rates_df['value'][timestamp]
            )

        if 'min_consumption' in metrics:
            result['min_consumption'] = self.energy_consumption_calculator.min_consumption(
                profitability_equipment,
                hash_rates_df['value'][timestamp]
            )

        if 'guess_consumption' in metrics:
            result['guess_consumption'] = self.energy_consumption_calculator.guess_consumption(
                profitability_equipment,
                hash_rates_df['value'][timestamp]
            )

        if 'max_power' in metrics:
            result['max_power'] = self.energy_consumption_calculator.max_power(
                profitability_equipment,
                hash_rates_df['value'][timestamp]
            )

        if 'min_power' in metrics:
            result['min_power'] = self.energy_consumption_calculator.min_power(
                profitability_equipment,
                hash_rates_df['value'][timestamp]
            )

        if 'guess_power' in metrics:
            result['guess_power'] = self.energy_consumption_calculator.guess_power(
                profitability_equipment,
                hash_rates_df['value'][timestamp]
            )

        return result

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
                hash_rates
            ) for timestamp, row in self._get_prof_thresholds_dataframe().iterrows()
        ]
        smooth_data = self._smooth_data(data)

        energy_df = pd.DataFrame(smooth_data).sort_values(by='timestamp').set_index('timestamp') \
            .rolling(window=7, min_periods=1).mean()

        return energy_df
