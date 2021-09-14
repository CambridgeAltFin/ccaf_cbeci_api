from extensions import cache
from config import config, start_date
from typing import List, Dict, Union
from datetime import datetime
from helpers import load_typed_hasrates, get_avg_effciency_by_miners_types, get_hash_rates_by_miners_types
from services.energy_calculation_service import EnergyCalculationService
import psycopg2
import psycopg2.extras
import pandas as pd


@cache.cached(key_prefix='actual-prof_threshold')
def get_prof_thresholds():
    with psycopg2.connect(**config['blockchain_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute('SELECT timestamp, date, value FROM prof_threshold WHERE timestamp >= %s',
                       (start_date.timestamp(),))
        return cursor.fetchall()


@cache.cached(key_prefix='actual-hash_rate')
def get_hash_rates():
    with psycopg2.connect(**config['blockchain_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute('SELECT timestamp, date, value FROM hash_rate WHERE timestamp >= %s', (start_date.timestamp(),))
        return cursor.fetchall()


@cache.cached(key_prefix='actual-miners')
def get_miners():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            'SELECT miner_name, unix_date_of_release, efficiency_j_gh, qty, type FROM miners WHERE is_active is true')
        return cursor.fetchall()


class EnergyConsumptionPowerByTypes(object):
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    default_price = 0.05

    def __init__(self):
        self.energy_calculation_service = EnergyCalculationService()
        self.prof_thresholds = get_prof_thresholds()
        self.hash_rates = get_hash_rates()
        self.miners = get_miners()
        self.typed_hash_rates = load_typed_hasrates()
        self.typed_avg_efficiency = get_avg_effciency_by_miners_types(self.miners)
        self.hash_rates_df = pd.DataFrame(self.hash_rates).drop('date', axis=1).set_index('timestamp')

    def get_data(self, price: float):
        return self._get_energy_dataframe(price).iterrows()

    def get_monthly_data(self, price: float):
        energy_df = self._get_energy_dataframe(price)
        energy_df.reset_index(inplace=True)
        energy_df['date'] = pd.to_datetime(energy_df['timestamp'], unit='s')
        energy_df.drop(columns=['timestamp'])
        energy_df.set_index('date', inplace=True)
        energy_df = energy_df.groupby(pd.Grouper(freq='M')).sum().round(2)
        energy_df['guess_power'] = energy_df['guess_power'].apply(lambda x: x * 24)
        energy_df['cumulative_guess_power'] = energy_df['guess_power'].cumsum()

        return energy_df.iterrows()

    def get_profitability_equipment(self, price: float, timestamp: int, prof_threshold_value: float) -> List[float]:
        profitability_equipment = []
        price_coefficient = self.default_price / price

        for miner in self.miners:
            if timestamp > miner['unix_date_of_release'] \
                    and prof_threshold_value * price_coefficient > miner['efficiency_j_gh']:
                if not miner['type']:
                    profitability_equipment.append(miner['efficiency_j_gh'])

        return profitability_equipment

    def get_date_consumption(self, price: float, timestamp: int, prof_threshold_value: float) -> Union[Dict[str, float], None]:
        hash_rates = get_hash_rates_by_miners_types(self.typed_hash_rates, timestamp)
        profitability_equipment = self.get_profitability_equipment(price, timestamp, prof_threshold_value)

        if len(profitability_equipment) == 0:
            return {
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'timestamp': timestamp,
                'min_consumption': None,
                'max_consumption': None,
                'guess_consumption': None,
                'min_power': None,
                'max_power': None,
                'guess_power': None
            }

        max_consumption = self.energy_calculation_service.max_consumption(
            profitability_equipment,
            self.hash_rates_df['value'][timestamp]
        )
        min_consumption = self.energy_calculation_service.min_consumption(
            profitability_equipment,
            self.hash_rates_df['value'][timestamp]
        )
        guess_consumption = self.energy_calculation_service.guess_consumption(
            profitability_equipment,
            self.hash_rates_df['value'][timestamp],
            hash_rates,
            self.typed_avg_efficiency
        )

        return {
            'date': datetime.utcfromtimestamp(timestamp).isoformat(),
            'timestamp': timestamp,
            'min_consumption': min_consumption,
            'max_consumption': max_consumption,
            'guess_consumption': guess_consumption,
            'min_power': self.energy_calculation_service.min_power(profitability_equipment, self.hash_rates_df['value'][timestamp]),
            'max_power': self.energy_calculation_service.max_power(profitability_equipment, self.hash_rates_df['value'][timestamp]),
            'guess_power': self.energy_calculation_service.guess_power(
                profitability_equipment,
                self.hash_rates_df['value'][timestamp],
                hash_rates,
                self.typed_avg_efficiency
            )
        }

    def smooth_consumptions(self, consumptions: List[Union[Dict[str, float], None]]) -> List[Dict[str, float]]:
        smooth_data = []
        for index, consumption in enumerate(consumptions):
            prev_consumption = smooth_data[-1] if len(smooth_data) > 0 \
                else {'min_consumption': 0, 'max_consumption': 0, 'guess_consumption': 0, 'min_power': 0,
                      'max_power': 0, 'guess_power': 0}

            smooth_data.insert(index, {
                'min_consumption': consumption['min_consumption'] if consumption['min_consumption'] is not None else prev_consumption['min_consumption'],
                'max_consumption': consumption['max_consumption'] if consumption['max_consumption'] is not None else prev_consumption['max_consumption'],
                'guess_consumption': consumption['guess_consumption'] if consumption['guess_consumption'] is not None else prev_consumption['guess_consumption'],
                'min_power': consumption['min_power'] if consumption['min_power'] is not None else prev_consumption['min_power'],
                'max_power': consumption['max_power'] if consumption['max_power'] is not None else prev_consumption['max_power'],
                'guess_power': consumption['guess_power'] if consumption['guess_power'] is not None else prev_consumption['guess_power'],
                'timestamp': consumption['timestamp'],
                'date': consumption['date'],
            })
        return smooth_data

    def _get_energy_dataframe(self, price: float):
        prof_thresholds_df = pd.DataFrame(self.prof_thresholds) \
            .sort_values(by='timestamp') \
            .drop('date', axis=1) \
            .set_index('timestamp')
        prof_thresholds_ma_df = prof_thresholds_df.rolling(window=14, min_periods=1).mean()

        consumptions = [self.get_date_consumption(price, timestamp, row['value']) for timestamp, row in
                        prof_thresholds_ma_df.iterrows()]
        smooth_consumptions = self.smooth_consumptions(consumptions)

        energy_df = pd.DataFrame(smooth_consumptions).sort_values(by='timestamp').set_index('timestamp') \
            .rolling(window=7, min_periods=1).mean()

        return energy_df
