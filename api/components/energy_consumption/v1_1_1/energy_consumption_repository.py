
from config import config, Connection, start_date

from datetime import datetime
from dateutil import tz

import calendar
import psycopg2
import psycopg2.extras


class EnergyConsumptionRepository:

    def get_energy_consumptions(self, price: float) -> list:
        sql = 'SELECT timestamp, ' \
              'date, ' \
              'min_power::float, ' \
              'guess_power::float, ' \
              'max_power::float, ' \
              'min_consumption::float, ' \
              'guess_consumption::float, ' \
              'max_consumption::float ' \
              'FROM energy_consumptions ' \
              'WHERE price = %s ' \
              'ORDER BY timestamp'
        return self._run_select_query(sql, (str(price),))

    def get_actual_energy_consumption(self, price: float) -> list:
        sql = 'SELECT timestamp, ' \
              'date, ' \
              'min_power::float, ' \
              'guess_power::float, ' \
              'max_power::float, ' \
              'min_consumption::float, ' \
              'guess_consumption::float, ' \
              'max_consumption::float ' \
              'FROM energy_consumptions ' \
              'WHERE price = %s ' \
              'ORDER BY timestamp DESC ' \
              'LIMIT 1'
        return self._run_select_query(sql, (str(price),))

    def get_cumulative_energy_consumptions(self, price: float, current_month=False) -> list:
        sql = 'SELECT' \
              ' timestamp' \
              ', date' \
              ', monthly_consumption::float guess_consumption' \
              ', cumulative_consumption::float cumulative_guess_consumption ' \
              'FROM cumulative_energy_consumptions ' \
              'WHERE price = %s '
        if not current_month:
            today = datetime.utcnow().date()
            start_of_month = datetime(today.year, today.month, 1, tzinfo=tz.tzutc())
            sql += 'AND timestamp < {}'.format(int(start_of_month.timestamp()))
        return self._run_select_query(sql, (str(price),))

    def get_prof_thresholds(self):
        sql = 'SELECT timestamp, date, value FROM prof_threshold WHERE timestamp >= %s'
        return self._run_select_query(sql, (start_date.timestamp(),), Connection.blockchain_data)

    def get_hash_rates(self):
        sql = 'SELECT timestamp, date, value FROM hash_rate WHERE timestamp >= %s'
        return self._run_select_query(sql, (start_date.timestamp(),), Connection.blockchain_data)

    def get_typed_hash_rates(self):
        typed_hash_rates = {}
        hash_rate_types = ['s7', 's9']
        for hash_rate_type in hash_rate_types:
            sql = 'SELECT type, value, date FROM hash_rate_by_types WHERE type = %s;'
            data = self._run_select_query(sql, (hash_rate_type,), Connection.blockchain_data)
            formatted_data = {}
            for row in data:
                formatted_data[calendar.timegm(row['date'].timetuple())] = row
            typed_hash_rates[hash_rate_type] = formatted_data
        return typed_hash_rates

    def get_miners(self):
        sql = 'SELECT miner_name, unix_date_of_release, efficiency_j_gh, qty, type FROM miners WHERE is_active is true'
        return self._run_select_query(sql, (), Connection.custom_data)

    @staticmethod
    def _run_select_query(sql: str, bindings: tuple = None, connection: str = Connection.custom_data):
        with psycopg2.connect(**config[connection]) as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(sql, bindings)
            return cursor.fetchall()

    @staticmethod
    def _run_query(sql: str, bindings: tuple = None):
        with psycopg2.connect(**config[Connection.custom_data]) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, bindings)
