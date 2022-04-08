
from config import config, Connection, start_date

from datetime import datetime
from dateutil.relativedelta import relativedelta

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
              'max_consumption::float, ' \
              'profitability_equipment::float ' \
              'FROM energy_consumptions ' \
              'WHERE price = %s ' \
              'ORDER BY timestamp'
        return self._run_select_query(sql, (str(price),))

    def get_cumulative_energy_consumptions(self, price: float) -> list:
        sql = 'SELECT timestamp, date, monthly_consumption::float guess_consumption, cumulative_consumption::float cumulative_guess_consumption FROM cumulative_energy_consumptions ' \
              'WHERE price = %s'
        return self._run_select_query(sql, (str(price),))

    def get_prof_thresholds(self):
        sql = 'SELECT timestamp, date, value FROM prof_threshold WHERE timestamp >= %s'
        return self._run_select_query(sql, (start_date.timestamp(),), Connection.blockchain_data)

    def get_hash_rates(self):
        sql = 'SELECT timestamp, date, value FROM hash_rate WHERE timestamp >= %s'
        return self._run_select_query(sql, (start_date.timestamp(),), Connection.blockchain_data)

    def get_miners(self):
        five_years_ago = datetime.now() - relativedelta(years=5)
        sql = 'SELECT miner_name, unix_date_of_release, efficiency_j_gh, qty, type FROM miners ' \
              'WHERE is_active is true AND is_manufacturer = 1 ' \
              'AND (unix_date_of_release >= %s OR unix_date_of_release < %s)'
        return self._run_select_query(sql, (int(five_years_ago.timestamp()), int(datetime(2014, 7, 1).timestamp())), Connection.custom_data)

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
