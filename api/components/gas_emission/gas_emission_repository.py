from config import config

import time
import psycopg2
import psycopg2.extras
from datetime import datetime


class GasEmissionRepository:

    def get_global_co2_coefficients(self):
        sql = 'SELECT t.timestamp, t.date, t.co2_coef, t.name FROM co2_coefficients t'
        co2_coefficients = self._run_select_query(sql)
        return co2_coefficients

    def get_total_global_co2_coefficients(self):
        sql = "SELECT to_char(t.date, 'YYYY-MM') AS month, MAX(t.co2_coef) AS co2_coef FROM co2_coefficients t " \
              'GROUP BY month ORDER BY month DESC'
        return self._run_select_query(sql)

    def get_newest_co2_coefficient(self) -> dict:
        sql = 'SELECT t.timestamp, t.date, t.co2_coef FROM co2_coefficients t ORDER BY t.timestamp DESC LIMIT 1'
        result = self._run_select_query(sql)
        return result[0]

    def create_co2_coefficient_record(self, date: str, co2_coefficient: float, coefficient_type: str):
        sql = 'INSERT INTO co2_coefficients (timestamp, date, co2_coef, name) VALUES (%s, %s, %s, %s) ' \
              'ON CONFLICT ON CONSTRAINT "TIMESTAMP_UNIQUE_INDEX" DO UPDATE SET co2_coef = %s, name = %s ' \
              'WHERE co2_coefficients.timestamp = %s'
        timestamp = int(time.mktime(datetime.strptime(date, '%Y-%m-%d').timetuple()))
        self._run_query(
            sql,
            (timestamp, date, co2_coefficient, coefficient_type, co2_coefficient, coefficient_type, timestamp)
        )

    def get_greenhouse_gas_emissions(self, price):
        sql = 'SELECT name, timestamp, value FROM greenhouse_gas_emissions ' \
              'WHERE price = %s'
        return self._run_select_query(sql, (str(price),))

    def get_total_greenhouse_gas_emissions(self, price):
        sql = 'SELECT date, timestamp, value AS v, cumulative_value AS cumulative_v FROM cumulative_greenhouse_gas_emissions ' \
              'WHERE price = %s'
        return self._run_select_query(sql, (str(price),))

    def get_monthly_bitcoin_power_mix(self):
        sql = "SELECT timestamp, to_char(date, 'YYYY-MM') AS month, name, value FROM power_sources WHERE type = 'monthly'"
        return self._run_select_query(sql)

    def get_yearly_bitcoin_power_mix(self):
        sql = "SELECT timestamp, to_char(date, 'YYYY') AS year, name, value FROM power_sources WHERE type = 'yearly'"
        return self._run_select_query(sql)

    @staticmethod
    def _run_select_query(sql: str, bindings: tuple = None):
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(sql, bindings)
            return cursor.fetchall()

    @staticmethod
    def _run_query(sql: str, bindings: tuple = None):
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, bindings)
