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

    def get_actual_btc_emission_intensity(self):
        sql = "SELECT " \
              "c.id AS country_id " \
              ", c.country AS name" \
              ", c.code" \
              ", c.country_flag AS flag" \
              ", t.co2_coef AS value" \
              ", to_char(t.date, 'YYYY') AS year " \
              "FROM co2_coefficients t " \
              "JOIN countries c ON c.code = 'BTC'" \
              "ORDER BY timestamp DESC LIMIT 1"
        co2_coefficients = self._run_select_query(sql)
        return co2_coefficients[0]

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

    def get_flat_greenhouse_gas_emissions(self, price):
        sql = "SELECT timestamp, to_char(MAX(DATE), 'YYYY-MM-DD\"T\"HH24:MI:SS') AS date " \
              ", MAX(CASE WHEN name = ANY (ARRAY ['Historical Hydro-only', 'Hydro-only', 'Provisional Hydro-only']) THEN value END) AS min_co2 " \
              ", MAX(CASE WHEN name = ANY (ARRAY ['Historical Estimated', 'Estimated', 'Provisional Estimated']) THEN value END) AS guess_co2 " \
              ", MAX(CASE WHEN name = ANY (ARRAY ['Historical Coal-only', 'Coal-only', 'Provisional Coal-only']) THEN value END) AS max_co2 " \
              "FROM greenhouse_gas_emissions " \
              "WHERE greenhouse_gas_emissions.price = %s " \
              "GROUP BY greenhouse_gas_emissions.timestamp " \
              "ORDER BY timestamp"
        return self._run_select_query(sql, (str(price),))

    def get_total_greenhouse_gas_emissions(self, price):
        sql = 'SELECT date, timestamp, value AS v, cumulative_value AS cumulative_v ' \
              'FROM cumulative_greenhouse_gas_emissions ' \
              'WHERE price = %s'
        return self._run_select_query(sql, (str(price),))

    def get_monthly_bitcoin_power_mix(self):
        sql = "SELECT timestamp, to_char(date, 'YYYY-MM') AS month, name, value " \
              "FROM power_sources " \
              "WHERE type = 'monthly' " \
              "ORDER BY power_sources.order, timestamp"
        return self._run_select_query(sql)

    def get_yearly_bitcoin_power_mix(self):
        sql = "SELECT timestamp, to_char(date, 'YYYY') AS year, name, value " \
              "FROM power_sources WHERE type = 'yearly' " \
              "ORDER BY power_sources.order, timestamp"
        return self._run_select_query(sql)

    def get_actual_world_emission(self):
        sql = "SELECT code, name, year, value " \
              "FROM ghg_historical_emissions " \
              "WHERE code = 'WORLD' " \
              "ORDER BY year DESC " \
              "LIMIT 1"
        result = self._run_select_query(sql)
        return result[0]

    def get_emissions(self):
        sql = "SELECT c.code, c.country AS name, ghg.year, ghg.value " \
              "FROM ghg_historical_emissions ghg " \
              "JOIN countries c ON ghg.country_id = c.id " \
              "WHERE ghg.year = (SELECT MAX(year) FROM ghg_historical_emissions WHERE country_id = ghg.country_id) " \
              "ORDER BY ghg.value DESC"

        return self._run_select_query(sql)

    def get_emission_intensities(self):
        sql = "SELECT c.code, c.country AS name, ghg.value, c.country_flag AS flag " \
              "FROM ghg_emission_intensities ghg " \
              "JOIN countries c ON ghg.country_id = c.id " \
              "WHERE ghg.year = (SELECT MAX(year) FROM ghg_emission_intensities WHERE country_id = ghg.country_id) " \
              "AND ghg.value IS NOT NULL " \
              "ORDER BY ghg.value"

        return self._run_select_query(sql)

    def get_annualised_btc_greenhouse_gas_emissions(self, year):
        sql = "SELECT SUM(value) as value FROM cumulative_greenhouse_gas_emissions " \
              "WHERE price = %s " \
              "AND to_char(date, 'YYYY') = %s "
        result = self._run_select_query(sql, (str(.05), str(year)))
        return {'code': 'BTC', 'name': 'Annualised', 'year': year, 'value': result[0]['value']}

    def get_actual_btc_greenhouse_gas_emission(self):
        sql = "SELECT value, to_char(date, 'YYYY') AS year FROM greenhouse_gas_emissions " \
              "WHERE price = %s " \
              "AND name IN ('Historical Estimated', 'Estimated', 'Provisional Estimated') " \
              "ORDER BY timestamp DESC LIMIT 1"
        result = self._run_select_query(sql, (str(.05),))
        return {'code': 'BTC', 'name': 'Bitcoin', 'year': result[0]['year'], 'value': result[0]['value']}

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
