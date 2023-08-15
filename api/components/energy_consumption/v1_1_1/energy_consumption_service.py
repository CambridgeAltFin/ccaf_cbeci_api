
from components.energy_consumption.energy_consumption_calculator import EnergyConsumptionCalculator
from components.energy_consumption.v1_1_1.energy_consumption_repository import EnergyConsumptionRepository
from helpers import get_hash_rates_by_miners_types

from typing import List, Dict, Union, Any
from datetime import datetime
from dateutil import tz
import pandas as pd


def _get_avg_efficiency_by_miners_types(miners):
    miners_by_types = {}
    typed_avg_efficiency = {}
    for miner in miners:
        miner_type = miner['type']
        if miner_type:
            if miner_type not in miners_by_types:
                miners_by_types[miner_type] = []
            miners_by_types[miner_type].append(miner['efficiency_j_gh'])

    for t, efficiencies in miners_by_types.items():
        typed_avg_efficiency[t] = sum(efficiencies) / len(efficiencies)

    return typed_avg_efficiency


class EnergyConsumptionService(object):
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    default_price = 0.05

    def __init__(
            self,
            energy_consumption_calculator: EnergyConsumptionCalculator,
            repository: EnergyConsumptionRepository
    ):
        self.energy_consumption_calculator = energy_consumption_calculator
        self.repository = repository
        self.metrics = [
            'min_consumption',
            'max_consumption',
            'guess_consumption',
            'min_power',
            'max_power',
            'guess_power'
        ]

    def get_data(self, price: float):
        return self.calc_data(price)

    def calc_data(self, price: float):
        return self._get_energy_dataframe(price, self.metrics).iterrows()

    def get_yearly_data(self, price: float, current_month=True):
        return self.calc_yearly_data(price, current_month=current_month)

    def get_monthly_data(self, price: float, current_month=True):
        return self.calc_monthly_data(price, current_month=current_month)

    def calc_monthly_data(self, price: float, current_month=True):
        energy_df = self._get_energy_dataframe(price, ['guess_power'])

        if not current_month:
            today = datetime.utcnow().date()
            start_of_month = datetime(today.year, today.month, 1, tzinfo=tz.tzutc())
            energy_df = energy_df.loc[energy_df.index < int(start_of_month.timestamp())]

        energy_df.reset_index(inplace=True)
        energy_df['date'] = pd.to_datetime(energy_df['timestamp'], unit='s')
        energy_df.drop(columns=['timestamp'])
        energy_df.set_index('date', inplace=True)
        energy_df = energy_df.groupby(pd.Grouper(freq='M')).sum()
        energy_df['guess_consumption'] = energy_df['guess_power'].apply(lambda x: x * 24 / 1000)
        energy_df['cumulative_guess_consumption'] = energy_df['guess_consumption'].cumsum()

        return energy_df.iterrows()

    def calc_yearly_data(self, price: float, current_month=True):
        energy_df = pd.DataFrame([dict(row) | {'date': date} for date, row in self.get_monthly_data(price, current_month)])
        energy_df['date'] = pd.to_datetime(energy_df['date'])
        energy_df.set_index('date', inplace=True)
        energy_df = energy_df.groupby(pd.Grouper(freq='Y')).sum()
        energy_df['cumulative_guess_consumption'] = energy_df['guess_consumption'].cumsum()
        return energy_df.iterrows()

    def get_actual_data(self, price: float):
        return self._get_energy_dataframe(price, self.metrics).iloc[-1]

    def get_equipment_list(self, price):
        miners = self.repository.get_miners()

        return [
            self._get_date_equipment_list(
                price,
                timestamp,
                row['value'],
                miners
            ) for timestamp, row in self._get_prof_thresholds_dataframe().iterrows()
        ]

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

        profitability_equipment = list(map(lambda x: x['efficiency_j_gh'], equipment_list))

        return {
            'date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
            'timestamp': timestamp,
            'profitability_equipment': self.energy_consumption_calculator.get_avg(profitability_equipment),
            'profitability_equipment_lower_bound': min(profitability_equipment),
            'profitability_equipment_upper_bound': max(profitability_equipment),
            'equipment_list': equipment_list
        }

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
            if timestamp > miner['unix_date_of_release'] and prof_threshold_value * price_coefficient > miner['efficiency_j_gh']:
                if miner['type'] not in ['S7', 'S9']:
                    equipment_list.append(dict(miner))

        return equipment_list

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

        profitability_equipment = list(map(lambda x: x['efficiency_j_gh'], equipment_list))

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
                profitability_equipment,
                hash_rates_df['value'][timestamp],
                hash_rates,
                typed_avg_efficiency
            )

        return result

    @staticmethod
    def _smooth_data(data: List[Union[Dict[str, float], None]]) -> List[Dict[str, float]]:
        smooth_data = []
        for index, row in enumerate(data):
            prev = smooth_data[-1] if len(smooth_data) > 0 else {metric: 0 for metric in row.keys()}

            smooth_data.insert(index, {
                'timestamp': row['timestamp'],
                'date': row['date'],
            } | {metric: row[metric] if row[metric] is not None else prev[metric] for metric in row.keys()})
        return smooth_data

    def _get_energy_dataframe(self, price: float, metrics):
        miners = self.repository.get_miners()
        typed_avg_efficiency = _get_avg_efficiency_by_miners_types(miners)
        hash_rates = pd.DataFrame(self.repository.get_hash_rates()).drop('date', axis=1).set_index('timestamp')
        typed_hash_rates = self.repository.get_typed_hash_rates()

        data = [
            self._get_date_metrics(
                price,
                timestamp,
                row['value'],
                metrics,
                miners,
                typed_avg_efficiency,
                typed_hash_rates,
                hash_rates
            ) for timestamp, row in self._get_prof_thresholds_dataframe().iterrows()
        ]

        return self._data_to_energy_dataframe(data)

    def _data_to_energy_dataframe(self, data):
        smooth_data = self._smooth_data(data)

        energy_df = pd.DataFrame(smooth_data).sort_values(by='timestamp').set_index('timestamp')\
            .rolling(window=7, min_periods=1).mean()

        return energy_df

    def _get_prof_thresholds_dataframe(self):
        prof_thresholds_df = pd.DataFrame(self.repository.get_prof_thresholds()) \
            .sort_values(by='timestamp') \
            .drop('date', axis=1) \
            .set_index('timestamp')
        return prof_thresholds_df.rolling(window=14, min_periods=1).mean()
