
from components.energy_consumption.energy_consumption_calculator import EnergyConsumptionCalculator
from components.energy_consumption.energy_consumption_repository import EnergyConsumptionRepository
from components.energy_consumption.v1_3_1.energy_consumption_service import EnergyConsumptionService as EnergyConsumptionService_v1_3_1

from typing import Any
from datetime import datetime
import pandas as pd


class EnergyConsumptionService(EnergyConsumptionService_v1_3_1):
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    default_price = 0.05

    weight_map = {0: 1, 1: 0.8, 2: 0.6, 3: 0.4, 4: 0.2, "False": 0}

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

        return equipment_list

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

        total_weight = sum([x['weight'] for x in equipment_list])
        count = len(equipment_list)
        factor = (count / total_weight) * (1 / count)
        efficiency_weighted = [(x['power'] / x['hashing_power']) * (x['weight'] * factor) / 1000 for x in equipment_list]

        return {
            'date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
            'timestamp': timestamp,
            'profitability_equipment': sum(efficiency_weighted),
            'profitability_equipment_lower_bound': min(profitability_equipment),
            'profitability_equipment_upper_bound': max(profitability_equipment),
            'equipment_list': equipment_list
        }

    def _calculate_miner_weight(self, timestamp, miner):
        years = self.__calculate_years(timestamp, miner)
        return self.weight_map[years]

    def __calculate_years(self, timestamp, miner):
        date = datetime.fromtimestamp(timestamp)
        difference = (date.year - miner['date_of_release'].year) * 12 + (date.month - miner['date_of_release'].month)
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
