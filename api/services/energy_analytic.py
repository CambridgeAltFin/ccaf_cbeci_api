from extensions import cache
from config import config, start_date
from typing import List, Dict, Union
from datetime import datetime
from dateutil import tz
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


class EnergyAnalytic(object):
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
        self.metrics = ['min_consumption', 'max_consumption', 'guess_consumption', 'min_power', 'max_power', 'guess_power']

    def get_data(self, price: float):
        return self._get_energy_dataframe(price, self.metrics).iterrows()

    def get_monthly_data(self, price: float):
        today = datetime.utcnow().date()
        start_of_month = datetime(today.year, today.month, 1, tzinfo=tz.tzutc())

        energy_df = self._get_energy_dataframe(price, ['guess_power'])
        energy_df = energy_df.loc[energy_df.index < int(start_of_month.timestamp())]

        energy_df.reset_index(inplace=True)
        energy_df['date'] = pd.to_datetime(energy_df['timestamp'], unit='s')
        energy_df.drop(columns=['timestamp'])
        energy_df.set_index('date', inplace=True)
        energy_df = energy_df.groupby(pd.Grouper(freq='M')).sum()
        energy_df['guess_consumption'] = energy_df['guess_power'].apply(lambda x: x * 24 / 1000).round(2)
        energy_df['cumulative_guess_consumption'] = energy_df['guess_consumption'].cumsum()

        return energy_df.iterrows()

    def get_actual_data(self, price: float):
        return self._get_energy_dataframe(price, self.metrics).iloc[-1]

    def _get_profitability_equipment(self, price: float, timestamp: int, prof_threshold_value: float) -> List[float]:
        profitability_equipment = []
        price_coefficient = self.default_price / price

        for miner in self.miners:
            if timestamp > miner['unix_date_of_release'] \
                    and prof_threshold_value * price_coefficient > miner['efficiency_j_gh']:
                if not miner['type']:
                    profitability_equipment.append(miner['efficiency_j_gh'])

        return profitability_equipment

    def _get_date_metrics(self, price: float, timestamp: int, prof_threshold_value: float, metrics: List) -> Union[Dict[str, float], None]:
        hash_rates = {}
        if 'guess_consumption' in metrics or 'guess_power' in metrics:
            hash_rates = get_hash_rates_by_miners_types(self.typed_hash_rates, timestamp)
        profitability_equipment = self._get_profitability_equipment(price, timestamp, prof_threshold_value)

        result = {
            'date': datetime.utcfromtimestamp(timestamp).isoformat(),
            'timestamp': timestamp
        }

        if len(profitability_equipment) == 0:
            return result | {metrics[i]: None for i in range(len(metrics))}

        if 'max_consumption' in metrics:
            result['max_consumption'] = self.energy_calculation_service.max_consumption(
                profitability_equipment,
                self.hash_rates_df['value'][timestamp]
            )

        if 'min_consumption' in metrics:
            result['min_consumption'] = self.energy_calculation_service.min_consumption(
                profitability_equipment,
                self.hash_rates_df['value'][timestamp]
            )

        if 'guess_consumption' in metrics:
            result['guess_consumption'] = self.energy_calculation_service.guess_consumption(
                profitability_equipment,
                self.hash_rates_df['value'][timestamp],
                hash_rates,
                self.typed_avg_efficiency
            )

        if 'max_power' in metrics:
            result['max_power'] = self.energy_calculation_service.max_power(
                profitability_equipment,
                self.hash_rates_df['value'][timestamp]
            )

        if 'min_power' in metrics:
            result['min_power'] = self.energy_calculation_service.min_power(
                profitability_equipment,
                self.hash_rates_df['value'][timestamp]
            )

        if 'guess_power' in metrics:
            result['guess_power'] = self.energy_calculation_service.guess_power(
                profitability_equipment,
                self.hash_rates_df['value'][timestamp],
                hash_rates,
                self.typed_avg_efficiency
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
        data = [
            self._get_date_metrics(price, timestamp, row['value'], metrics) for timestamp, row in self._get_prof_thresholds_dataframe().iterrows()
        ]
        smooth_data = self._smooth_data(data)

        energy_df = pd.DataFrame(smooth_data).sort_values(by='timestamp').set_index('timestamp') \
            .rolling(window=7, min_periods=1).mean()

        return energy_df

    def _get_prof_thresholds_dataframe(self):
        prof_thresholds_df = pd.DataFrame(self.prof_thresholds) \
            .sort_values(by='timestamp') \
            .drop('date', axis=1) \
            .set_index('timestamp')
        return prof_thresholds_df.rolling(window=14, min_periods=1).mean()

    def _get_cache_key(self, prefix: str) -> str:
        return f'{prefix}_{len(self.prof_thresholds)}'
