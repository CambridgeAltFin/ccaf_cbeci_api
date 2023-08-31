from components.energy_consumption.energy_consumption_calculator import EnergyConsumptionCalculator
from components.energy_consumption.energy_consumption_repository import EnergyConsumptionRepository
from components.energy_consumption.v1_3_1.energy_consumption_service import EnergyConsumptionService as EnergyConsumptionService_v1_3_1
from helpers import get_hash_rates_by_miners_types

from datetime import datetime
from typing import List, Dict, Union, Any
import pandas as pd


class EnergyConsumptionService(EnergyConsumptionService_v1_3_1):
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    default_price = 0.05

    weight_map = {0: 1, 1: 0.8, 2: 0.6, 3: 0.4, 4: 0.2, "False": 0}

    def __init__(
        self,
        energy_consumption_calculator: EnergyConsumptionCalculator,
        repository: EnergyConsumptionRepository
    ):
        super().__init__(energy_consumption_calculator=energy_consumption_calculator, repository=repository)
        self.repository = repository

    def is_calculated(self, price: float) -> bool:
        return int(price * 100) in self.calculated_prices

    def energy_efficiency_of_mining_hardware_chart(self):
        data = self.repository.get_daily_profitability_equipment()
        return [{
            'x': int(item['date'].timestamp()),
            'lower_bound': round(item['lower_bound'] * 10000 * 1000) / 10000,
            'estimated': round(item['estimated'] * 10000 * 1000) / 10000,
            'upper_bound': round(item['upper_bound'] * 10000 * 1000) / 10000,
        } for item in data]

    def download_energy_efficiency_of_mining_hardware(self):
        data = self.repository.get_daily_profitability_equipment()
        return [{
            'date': item['date'].strftime('%Y-%m-%d'),
            'lower_bound': item['lower_bound'] * 1000,
            'estimated': item['estimated'] * 1000,
            'upper_bound': item['upper_bound'] * 1000,
        } for item in data]

    def energy_efficiency_of_mining_hardware_yearly_chart(self):
        data = self.repository.get_yearly_profitability_equipment()
        return [{
            'x': int(datetime.strptime(str(int(item['date'])), '%Y').timestamp()),
            'lower_bound': round(item['lower_bound'] * 10000 * 1000) / 10000,
            'estimated': round(item['estimated'] * 10000 * 1000) / 10000,
            'upper_bound': round(item['upper_bound'] * 10000 * 1000) / 10000,
        } for item in data]

    def download_efficiency_of_mining_hardware_yearly(self):
        data = self.repository.get_yearly_profitability_equipment()
        return [{
            'year': str(int(item['date'])),
            'lower_bound': item['lower_bound'] * 1000,
            'estimated': item['estimated'] * 1000,
            'upper_bound': item['upper_bound'] * 1000,
        } for item in data]

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
                equipment_list.append(dict(miner) | {'weight': self._calculate_miner_weight(timestamp, miner)})

        return [m for m in equipment_list if m['weight'] > 0]

    def _get_date_equipment_list(self, price, timestamp, prof_threshold, miners):
        equipment_list = self._get_equipment_list(
            price,
            timestamp,
            prof_threshold,
            miners
        )

        if len(equipment_list) == 0:
            return {
                'date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
                'timestamp': timestamp,
                'profitability_equipment': 0,
                'profitability_equipment_lower_bound': 0,
                'profitability_equipment_upper_bound': 0,
                'equipment_list': [],
            }

        profitability_equipment = [x['efficiency_j_gh'] for x in equipment_list]
        efficiency_weighted = self._calc_efficiency_weighted(equipment_list)

        return {
            'date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
            'timestamp': timestamp,
            'profitability_equipment': sum(efficiency_weighted),
            'profitability_equipment_lower_bound': min(profitability_equipment),
            'profitability_equipment_upper_bound': max(profitability_equipment),
            'equipment_list': equipment_list
        }

    def _get_date_metrics(
            self,
            price: float,
            timestamp: int,
            prof_threshold_value: float,
            metrics: List,
            miners: list,
            typed_avg_efficiency,
            typed_hash_rates,
            hash_rates_df: pd.DataFrame
    ) -> Union[Dict[str, float], None]:
        hash_rates = {}
        if 'guess_consumption' in metrics or 'guess_power' in metrics:
            hash_rates = get_hash_rates_by_miners_types(typed_hash_rates, timestamp)

        equipment_list = self._get_equipment_list(
            price,
            timestamp,
            prof_threshold_value,
            miners
        )

        result = {
            'date': datetime.utcfromtimestamp(timestamp).isoformat(),
            'timestamp': timestamp,
        }

        if len(equipment_list) == 0:
            return result | {metrics[i]: None for i in range(len(metrics))}

        profitability_equipment = [x['efficiency_j_gh'] for x in equipment_list]
        efficiency_weighted = [sum(self._calc_efficiency_weighted(equipment_list))]

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
                efficiency_weighted,
                hash_rates_df['value'][timestamp],
                hash_rates,
                typed_avg_efficiency
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
                efficiency_weighted,
                hash_rates_df['value'][timestamp],
                hash_rates,
                typed_avg_efficiency
            )

        return result

    def _calc_efficiency_weighted(self, equipment_list):
        total_weight = sum([x['weight'] for x in equipment_list])
        count = len(equipment_list)
        factor = (count / total_weight) * (1 / count)
        return [
            (x['power'] / x['hashing_power']) * (x['weight'] * factor) / 1000 for x in equipment_list
        ]

    def _calculate_miner_weight(self, timestamp, miner):
        years = self.__calculate_years(timestamp, miner)
        return self.weight_map[years]

    def __calculate_years(self, timestamp, miner):
        date = datetime.fromtimestamp(timestamp)
        date_of_release = datetime.fromtimestamp(miner['date_of_release'])
        difference = (date.year - date_of_release.year) * 12 + (date.month - date_of_release.month)
        if difference < 0:
            return "False"
        elif difference < 12:
            return 0
        elif 12 <= difference < 24:
            return 1
        elif 24 <= difference < 36:
            return 2
        elif 36 <= difference < 48:
            return 3
        elif 48 <= difference < 60:
            return 4
        else:
            return "False"
