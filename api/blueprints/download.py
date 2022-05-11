from typing import List, Dict, Union
from flask import Blueprint, make_response, request
import csv
import io
from datetime import datetime
from services import EnergyConsumption, EnergyConsumptionByTypes, EnergyAnalytic
from services.v1_1_1 import EnergyAnalytic as EnergyAnalytic_v_1_1_1
from queries import get_mining_countries, get_mining_provinces
from packaging.version import parse as version_parse
from calendar import month_name
from decorators.cache_control import cache_control


bp = Blueprint('download', __name__, url_prefix='/download')


def get_data(version=None, price=0.05):
    def to_dict(timestamp, row):
        return {
            'timestamp': timestamp,
            'date': datetime.utcfromtimestamp(timestamp).isoformat(),
            'guess_consumption': row['guess_consumption'],
            'max_consumption': row['max_consumption'],
            'min_consumption': row['min_consumption']
        }

    def v1_0_5(price):
        energy_consumption = EnergyConsumption()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption.get_data(price)]

    def v1_1_0(price):
        energy_consumption_by_types = EnergyConsumptionByTypes()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption_by_types.get_data(price)]

    def v1_1_1(price):
        def to_dict(timestamp, row):
            return {
                'timestamp': timestamp,
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'guess_consumption': row['guess_consumption'],
                'max_consumption': row['max_consumption'],
                'min_consumption': row['min_consumption'],
                'guess_power': row['guess_power'],
                'max_power': row['max_power'],
                'min_power': row['min_power']
            }

        energy_analytic = EnergyAnalytic_v_1_1_1()

        return [to_dict(timestamp, row) for timestamp, row in energy_analytic.get_data(price)]

    def v1_2_0(price):
        def to_dict(timestamp, row):
            return {
                'timestamp': timestamp,
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'guess_consumption': row['guess_consumption'],
                'max_consumption': row['max_consumption'],
                'min_consumption': row['min_consumption'],
                'guess_power': row['guess_power'],
                'max_power': row['max_power'],
                'min_power': row['min_power']
            }

        energy_analytic = EnergyAnalytic()

        return [to_dict(timestamp, row) for timestamp, row in energy_analytic.get_data(price)]

    func = locals().get(version.replace('.', '_'))
    if callable(func):
        return func(price)

    raise NotImplementedError('Not Implemented')


def get_monthly_data(version, price=.05):
    def to_dict(date, row):
        return {
            'month': month_name[date.month] + date.strftime(' %Y'),
            'value': round(row['guess_consumption'], 2),
            'cumulative_value': round(row['cumulative_guess_consumption'], 2),
        }

    if version == 'v1.1.1':
        energy_analytic = EnergyAnalytic_v_1_1_1()
    elif version == 'v1.2.0':
        energy_analytic = EnergyAnalytic()
    else:
        raise NotImplementedError('Not Implemented')

    return [to_dict(date, row) for date, row in energy_analytic.get_monthly_data(price)]


def send_file(first_line=None, file_type='csv'):
    def send_csv(headers: Dict[str, str], rows: List[Dict[str, Union[str, int, float]]], filename='export.csv'):
        si = io.StringIO()
        keys = headers.keys()
        cw = csv.DictWriter(si, fieldnames=keys, extrasaction="ignore")
        if first_line is not None and len(keys) > 0:
            cw.writerow({list(keys)[0]: first_line})
        cw.writerow(headers)
        cw.writerows(rows)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = f"attachment; filename={filename}"
        output.headers["Content-type"] = "text/csv"
        return output

    return send_csv


@bp.route('/data')
@cache_control()
def data(version=None):
    file_type = request.args.get('file_type', 'csv')
    price = request.args.get('price', 0.05)
    headers = {
        'timestamp': 'Timestamp',
        'date': 'Date and Time',
        'max_consumption': 'MAX',
        'min_consumption': 'MIN',
        'guess_consumption': 'GUESS'
    }

    if version_parse(version) >= version_parse('v1.1.1'):
        headers = {
            'timestamp': 'Timestamp',
            'date': 'Date and Time',
            'max_power': 'power MAX, GW',
            'min_power': 'power MIN, GW',
            'guess_power': 'power GUESS, GW',
            'max_consumption': 'annualised consumption MAX, TWh',
            'min_consumption': 'annualised consumption MIN, TWh',
            'guess_consumption': 'annualised consumption GUESS, TWh'
        }

    rows = get_data(version, float(price))
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh', file_type=file_type)

    return send_file_func(headers, rows)


@bp.route('/data/monthly')
@cache_control()
def data_monthly(version=None):
    file_type = request.args.get('file_type', 'csv')
    price = request.args.get('price', 0.05)

    headers = {
        'month': 'Month',
        'value': 'Monthly consumption, TWh',
        'cumulative_value': 'Cumulative consumption, TWh',
    }

    rows = get_monthly_data(version, float(price))
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh', file_type=file_type)

    return send_file_func(headers, rows)


@bp.route('/profitability_equipment')
@cache_control()
def profitability_equipment(version=None):
    file_type = request.args.get('file_type', 'csv')
    price = request.args.get('price', 0.05)

    send_file_func = send_file(file_type=file_type)

    headers = {
        'timestamp': 'Timestamp',
        'date': 'Date and Time',
        'profitability_equipment': 'Profitability equipment',
        'equipment_list': 'Equipment list',
    }

    def miner_to_str(miner):
        miner_str = miner['miner_name']
        if miner['type']:
            miner_str += ' - ({type})'.format(type=miner['type'])
        return miner_str

    def to_dict(timestamp, row):
        return {
            'timestamp': timestamp,
            'date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
            'profitability_equipment': row['profitability_equipment'],
            'equipment_list': '; '.join(list(map(miner_to_str, row['equipment_list']))),
        }

    if version == 'v1.0.5':
        energy_analytic = EnergyConsumption()
    elif version == 'v1.1.0':
        energy_analytic = EnergyConsumptionByTypes()
    elif version == 'v1.1.1':
        energy_analytic = EnergyAnalytic_v_1_1_1()
    elif version == 'v1.2.0':
        energy_analytic = EnergyAnalytic()
    else:
        raise NotImplementedError('Not Implemented')

    rows = [to_dict(timestamp, row) for timestamp, row in energy_analytic.get_data(float(price))]

    return send_file_func(headers, rows)


@bp.route('/mining_countries')
@cache_control()
def mining_countries(version=None):
    if version not in ['v1.1.0', 'v1.1.1']:
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')

    rows = get_mining_countries(version[1:])
    send_file_func = send_file(file_type=file_type)

    if version == 'v1.1.0':
        headers = {
            'date': 'Date',
            'name': 'Country',
            'value': 'Share of global hashrate',
        }

        return send_file_func(headers, list(map(lambda row: {**row, 'value': round(row['value'] * 100, 2)}, rows)))

    headers = {
        'date': 'date',
        'name': 'country',
        'value': 'monthly_hashrate_%',
        'absolute_value': 'monthly_absolute_hashrate_EH/S',
    }

    return send_file_func(
        headers,
        list(map(lambda row: {
            **row,
            'value': str(round(row['value'] * 100, 2)) + '%' if row['value'] else '0%',
            'absolute_value': round(row['absolute_value'], 2) if row['absolute_value'] is not None else 0
        }, rows))
    )


@bp.route('/mining_provinces')
@bp.route('/mining_provinces/<country>')
@cache_control()
def mining_provinces(version=None, country='cn'):
    country_titles = {
        'cn': {
            'name': 'Province',
            'local_value': 'Share of Chinese hashrate'
        },
        'us': {
            'name': 'State',
            'local_value': 'US hashrate by state'
        },
    }

    if version not in ['v1.1.0', 'v1.1.1', 'v1.2.0'] or country not in country_titles:
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')
    headers = {
        'date': 'Date',
        'name': country_titles[country]['name'],
        # 'value': 'Share of global hashrate',
        'local_value': country_titles[country]['local_value']
    }
    rows = get_mining_provinces(country, version[1:])
    send_file_func = send_file(file_type=file_type)

    return send_file_func(
        headers,
        list(map(lambda row: {**row, 'local_value': round(row['local_value'] * 100, 2)}, rows))
    )


@bp.route('/absolute_mining_countries')
@cache_control()
def absolute_mining_countries(version=None):
    if version != 'v1.1.1':
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')
    headers = {
        'date': 'Date',
        'name': 'Country',
        'absolute_value': 'Estimated absolute hashrate'
    }
    rows = get_mining_countries(version[1:])
    send_file_func = send_file(file_type=file_type)

    return send_file_func(
        headers,
        rows
    )
