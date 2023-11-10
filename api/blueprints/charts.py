from flask import Blueprint, jsonify
import psycopg2
import calendar
import pandas as pd

from config import config
from extensions import cache
from decorators.cache_control import cache_control
from queries import get_mining_countries, get_mining_provinces, get_countries_dict
from resources.gas_emission.yearly_bitcoin_power_mix import YearlyBitcoinPowerMixChartPoint
from resources.gas_emission.monthly_bitcoin_power_mix import MonthlyBitcoinPowerMixChartPoint
from resources.gas_emission.bitcoin_emission_intensity import BitcoinEmissionIntensityChartPoint
from resources.gas_emission.bitcoin_greenhouse_gas_emission import BitcoinGreenhouseGasEmissionChartPoint
from resources.gas_emission.total_bitcoin_greenhouse_gas_emission import TotalBitcoinGreenhouseGasEmissionChartPoint
from resources.gas_emission.total_yearly_bitcoin_greenhouse_gas_emission import \
    TotalYearlyBitcoinGreenhouseGasEmissionChartPoint
from components.gas_emission import EmissionIntensityServiceFactory, GreenhouseGasEmissionServiceFactory, \
    PowerMixServiceFactory
from components.energy_consumption import EnergyConsumptionServiceFactory

bp = Blueprint('charts', __name__, url_prefix='/charts')


@bp.route('/mining_equipment_efficiency')
@cache_control()
def mining_equipment_efficiency():
    @cache.cached(key_prefix='all_miners')
    def get_miners():
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT miner_name, unix_date_of_release, efficiency_j_gh FROM miners')
            return cursor.fetchall()

    response = []
    miners = get_miners()

    for miner in miners:
        response.append({
            'x': miner[1] * 1000,
            'y': miner[2],
            'name': miner[0]
        })

    return jsonify(data=response)


@bp.route('/profitability_threshold')
@cache_control()
def profitability_threshold():
    @cache.cached(key_prefix='all_prof_threshold')
    def get_prof_thresholds():
        with psycopg2.connect(**config['blockchain_data']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM prof_threshold')
            return cursor.fetchall()

    response = []
    prof_thresholds = get_prof_thresholds()

    df = pd.DataFrame(prof_thresholds, columns=['timestamp', 'date', 'value']).drop(columns=['timestamp'])
    df['date'] = df['date'].astype('datetime64[ns]')
    df_by_weeks = df.resample('W-MON', on='date', closed='left', label='left').mean()

    for date, value in df_by_weeks.iterrows():
        response.append({
            'x': date.timestamp() * 1000,
            'y': value.value
        })

    return jsonify(data=response)


@bp.route('/mining_countries')
@cache_control()
def mining_countries():
    response = []
    mining_countries = get_mining_countries()

    for mining_country in mining_countries:
        response.append({
            'x': calendar.timegm(mining_country['date'].timetuple()) * 1000,
            'y': mining_country['value'],
            'name': mining_country['name'],
            'code': mining_country['code']
        })

    return jsonify(data=response)


@bp.route('/absolute_mining_countries')
@cache_control()
def absolute_mining_countries():
    response = []
    mining_countries = get_mining_countries()

    for mining_country in mining_countries:
        response.append({
            'x': calendar.timegm(mining_country['date'].timetuple()) * 1000,
            'y': mining_country['absolute_value'],
            'name': mining_country['name'],
            'code': mining_country['code']
        })

    return jsonify(data=response)


@bp.route('/mining_provinces')
@bp.route('/mining_provinces/<country>')
@cache_control()
def mining_provinces(country='cn'):
    response = []
    mining_provinces = get_mining_provinces(country)

    for mining_province in mining_provinces:
        response.append({
            'x': calendar.timegm(mining_province['date'].timetuple()) * 1000,
            'y': mining_province['local_value'],
            'name': mining_province['name']
        })

    return jsonify(data=response)


@bp.route('/mining_map_countries')
@cache_control()
def mining_map_countries():
    @cache.cached(key_prefix='all_mining_map_countries')
    def get_mining_map_countries():
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT m.*, c.code FROM mining_map_countries AS m JOIN countries AS c ON c.id = m.country_id ORDER BY m.date, c.code')
            return cursor.fetchall()

    response = []
    mining_map_countries = get_mining_map_countries()

    for mining_map_country in mining_map_countries:
        response.append({
            'x': calendar.timegm(mining_map_country[3].timetuple()) * 1000,
            'y': mining_map_country[2],
            'name': mining_map_country[1],
            'code': mining_map_country[5]
        })

    return jsonify(data=response)


@bp.route('/mining_map_provinces')
@bp.route('/mining_map_provinces/<country>')
@cache_control()
def mining_map_provinces(country='cn'):
    @cache.cached(key_prefix='all_mining_map_provinces_' + country)
    def get_mining_map_provinces():
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT name, local_value, date, code FROM mining_map_provinces WHERE country_id = %s ORDER BY id',
                (get_countries_dict()[country.lower()],)
            )
            return cursor.fetchall()

    response = []
    mining_map_provinces = get_mining_map_provinces()

    for mining_map_province in mining_map_provinces:
        response.append({
            'x': calendar.timegm(mining_map_province[2].timetuple()) * 1000,
            'y': mining_map_province[1],
            'name': mining_map_province[0],
            'code': mining_map_province[3]
        })

    return jsonify(data=response)


@bp.route('/bitcoin_emission_intensity')
@cache_control()
@cache.memoize()
def bitcoin_emission_intensity():
    service = EmissionIntensityServiceFactory.create()
    return jsonify(
        data=[BitcoinEmissionIntensityChartPoint(point) for point in service.get_bitcoin_emission_intensity()])


@bp.route('/bitcoin_greenhouse_gas_emissions')
@bp.route('/bitcoin_greenhouse_gas_emissions/<price>')
@cache_control()
@cache.memoize()
def bitcoin_greenhouse_gas_emissions(price=0.05):
    service = GreenhouseGasEmissionServiceFactory.create()
    return jsonify(data=[BitcoinGreenhouseGasEmissionChartPoint(point) for point in
                         service.get_greenhouse_gas_emissions(float(price))])


@bp.route('/total_bitcoin_greenhouse_gas_emissions')
@bp.route('/total_bitcoin_greenhouse_gas_emissions/<price>')
@cache_control()
@cache.memoize()
def total_bitcoin_greenhouse_gas_emissions(price=0.05):
    service = GreenhouseGasEmissionServiceFactory.create()
    return jsonify(data=[TotalBitcoinGreenhouseGasEmissionChartPoint(point) for point in
                         service.get_total_greenhouse_gas_emissions_monthly(float(price))])


@bp.route('/total_yearly_bitcoin_greenhouse_gas_emissions')
@bp.route('/total_yearly_bitcoin_greenhouse_gas_emissions/<price>')
@cache_control()
@cache.memoize()
def total_yearly_bitcoin_greenhouse_gas_emissions(price=0.05):
    service = GreenhouseGasEmissionServiceFactory.create()
    return jsonify(data=[TotalYearlyBitcoinGreenhouseGasEmissionChartPoint(point, date) for date, point in
                         service.get_total_yearly_greenhouse_gas_emissions(float(price))])


@bp.route('/monthly_bitcoin_power_mix')
@cache_control()
@cache.memoize()
def monthly_bitcoin_power_mix():
    service = PowerMixServiceFactory.create()
    return jsonify(data=[MonthlyBitcoinPowerMixChartPoint(point) for point in service.get_monthly_data()])


@bp.route('/yearly_bitcoin_power_mix')
@cache_control()
@cache.memoize()
def yearly_bitcoin_power_mix():
    service = PowerMixServiceFactory.create()
    return jsonify(data=[YearlyBitcoinPowerMixChartPoint(point) for point in service.get_yearly_data()])


@bp.route('/energy_efficiency_of_mining_hardware/daily')
@cache_control()
@cache.memoize()
def energy_efficiency_of_mining_hardware_daily():
    service = EnergyConsumptionServiceFactory.create()
    return jsonify(data=service.energy_efficiency_of_mining_hardware_chart())


@bp.route('/energy_efficiency_of_mining_hardware/yearly')
@cache_control()
@cache.memoize()
def energy_efficiency_of_mining_hardware_yearly():
    service = EnergyConsumptionServiceFactory.create()
    return jsonify(data=service.energy_efficiency_of_mining_hardware_yearly_chart())
