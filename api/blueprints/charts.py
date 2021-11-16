from flask import Blueprint, jsonify
import psycopg2
import calendar
from config import config
from extensions import cache
import pandas as pd
from queries import get_mining_countries, get_mining_provinces
from decorators.cache_control import cache_control
from components.gas_emission.gas_emission_factory import EmissionIntensityServiceFactory
from resources.gas_emission.bitcoin_emission_intensity import BitcoinEmissionIntensityChartPoint

bp = Blueprint('charts', __name__, url_prefix='/charts')


@bp.route('/mining_equipment_efficiency')
@cache_control()
def mining_equipment_efficiency():
    @cache.cached(key_prefix='all_miners')
    def get_miners():
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM miners')
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
@cache_control()
def mining_provinces():
    response = []
    mining_provinces = get_mining_provinces()

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
            cursor.execute('SELECT m.*, c.code FROM mining_map_countries AS m JOIN countries AS c ON c.id = m.country_id ORDER BY m.date, c.code')
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
@cache_control()
def mining_map_provinces():
    @cache.cached(key_prefix='all_mining_map_provinces')
    def get_mining_map_provinces():
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM mining_map_provinces')
            return cursor.fetchall()

    response = []
    mining_map_provinces = get_mining_map_provinces()

    for mining_map_province in mining_map_provinces:
        response.append({
            'x': calendar.timegm(mining_map_province[4].timetuple()) * 1000,
            'y': mining_map_province[3],
            'name': mining_map_province[1]
        })

    return jsonify(data=response)


@bp.route('/bitcoin_emission_intensity')
@cache_control()
def bitcoin_emission_intensity():
    service = EmissionIntensityServiceFactory.create()
    return jsonify(data=[BitcoinEmissionIntensityChartPoint(point) for point in service.get_bitcoin_emission_intensity()])
