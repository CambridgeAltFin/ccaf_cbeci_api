from extensions import cache
from config import config, start_date
from typing import List, Dict, Union, Any
from datetime import datetime
from dateutil import tz, relativedelta
from services.energy_calculation_service import EnergyCalculationService
from services.v1_1_1.energy_analytic import EnergyAnalytic as EnergyAnalytic_v_1_1_1
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
            'SELECT miner_name, unix_date_of_release, efficiency_j_gh, qty, type '
            'FROM miners '
            'WHERE is_active is true AND is_manufacturer = 1'
        )
        return cursor.fetchall()


class EnergyAnalytic(EnergyAnalytic_v_1_1_1):
    def __init__(self):
        super().__init__()
        self.prof_thresholds = get_prof_thresholds()
        self.hash_rates = get_hash_rates()
        self.miners = get_miners()
        self.hash_rates_df = pd.DataFrame(self.hash_rates).drop('date', axis=1).set_index('timestamp')

    def _get_equipment_list(self, price: float, timestamp: int, prof_threshold_value: float) -> list[dict[Any, Any]]:
        price_coefficient = self.default_price / price
        equipment_list = []

        for miner in self.miners:
            five_years = datetime.fromtimestamp(miner['unix_date_of_release']) + relativedelta.relativedelta(years=5)
            if miner['unix_date_of_release'] < timestamp < five_years.timestamp() \
                    and prof_threshold_value * price_coefficient > miner['efficiency_j_gh']:
                equipment_list.append(dict(miner))

        return equipment_list

    def _get_date_metrics(
        self, price: float, timestamp: int, prof_threshold_value: float, metrics: List
    ) -> Union[Dict[str, float], None]:
        equipment_list = self._get_equipment_list(
            price,
            timestamp,
            prof_threshold_value
        )

        result = {
            'date': datetime.utcfromtimestamp(timestamp).isoformat(),
            'timestamp': timestamp,
        }

        if len(equipment_list) == 0:
            return result | {metrics[i]: None for i in range(len(metrics))}

        profitability_equipment = list(map(lambda x: x['efficiency_j_gh'], equipment_list))

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
                self.hash_rates_df['value'][timestamp]
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
                self.hash_rates_df['value'][timestamp]
            )

        return result

    def _get_cache_key(self, prefix: str) -> str:
        return f'{prefix}_{len(self.prof_thresholds)}_{len(self.miners)}'
