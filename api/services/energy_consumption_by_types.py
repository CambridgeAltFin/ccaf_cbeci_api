from extensions import cache
from config import config, start_date
from typing import List, Dict, Union
from datetime import datetime
from helpers import load_typed_hasrates, get_avg_effciency_by_miners_types, get_hash_rates_by_miners_types, get_guess_consumption
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


class EnergyConsumptionByTypes(object):
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    default_price = 0.05

    def get_data(self, price: float):
        def get_profitability_equipment(price: float, timestamp: int, prof_threshold_value: float) -> List[float]:
            profitability_equipment = []
            price_coefficient = self.default_price / price

            for miner in miners:
                if timestamp > miner['unix_date_of_release']\
                        and prof_threshold_value * price_coefficient > miner['efficiency_j_gh']:
                    if not miner['type']:
                        profitability_equipment.append(miner['efficiency_j_gh'])

            return profitability_equipment

        def get_date_consumption(price: float, timestamp: int, prof_threshold_value: float
                                 ) -> Union[Dict[str, float], None]:
            hash_rates = get_hash_rates_by_miners_types(typed_hasrates, timestamp)
            profitability_equipment = get_profitability_equipment(price, timestamp, prof_threshold_value)
            if len(profitability_equipment) == 0:
                return {
                    'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                    'timestamp': timestamp,
                    'min_consumption': None,
                    'max_consumption': None,
                    'guess_consumption': None
                }

            max_consumption = max(profitability_equipment) * hash_rates_df['value'][timestamp] * 365.25 * 24 / 1e9 * 1.2
            min_consumption = min(profitability_equipment) * hash_rates_df['value'][
                timestamp] * 365.25 * 24 / 1e9 * 1.01
            guess_consumption = get_guess_consumption(profitability_equipment, hash_rates_df['value'][timestamp], hash_rates, typed_avg_effciency)

            return {
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'timestamp': timestamp,
                'min_consumption': min_consumption,
                'max_consumption': max_consumption,
                'guess_consumption': guess_consumption
            }

        def smooth_consumptions(consumptions: List[Union[Dict[str, float], None]]) -> List[Dict[str, float]]:
            smooth_data = []
            for index, consumption in enumerate(consumptions):
                prev_consumption = smooth_data[-1] if len(smooth_data) > 0 \
                    else {'min_consumption': 0, 'max_consumption': 0, 'guess_consumption': 0}

                smooth_data.insert(index, {
                    'min_consumption': consumption['min_consumption'] if consumption['min_consumption'] is not None
                    else prev_consumption['min_consumption'],
                    'max_consumption': consumption['max_consumption'] if consumption['max_consumption'] is not None
                    else prev_consumption['max_consumption'],
                    'guess_consumption': consumption['guess_consumption'] if consumption[
                                                                                 'guess_consumption'] is not None
                    else prev_consumption['guess_consumption'],
                    'timestamp': consumption['timestamp'],
                    'date': consumption['date'],
                })
            return smooth_data

        prof_thresholds = get_prof_thresholds()
        hash_rates = get_hash_rates()
        miners = get_miners()
        typed_hasrates = load_typed_hasrates()
        typed_avg_effciency = get_avg_effciency_by_miners_types(miners)

        hash_rates_df = pd.DataFrame(hash_rates).drop('date', axis=1).set_index('timestamp')

        prof_thresholds_df = pd.DataFrame(prof_thresholds).sort_values(by='timestamp').drop('date', axis=1).set_index(
            'timestamp')
        prof_thresholds_ma_df = prof_thresholds_df.rolling(window=14, min_periods=1).mean()

        consumptions = [get_date_consumption(price, timestamp, row['value']) for timestamp, row in
                        prof_thresholds_ma_df.iterrows()]
        smooth_consumptions = smooth_consumptions(consumptions)

        energy_df = pd.DataFrame(smooth_consumptions).sort_values(by='timestamp').set_index('timestamp') \
            .rolling(window=7, min_periods=1).mean()

        return energy_df.iterrows()
