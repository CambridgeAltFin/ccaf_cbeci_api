from typing import List, Dict, Union
from flask import Blueprint, make_response, request
import csv
import io
from datetime import datetime

from components.gas_emission import GreenhouseGasEmissionServiceFactory, EmissionIntensityServiceFactory, \
    PowerMixServiceFactory
from services import EnergyConsumption, EnergyConsumptionByTypes
from components.energy_consumption import EnergyConsumptionServiceFactory
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

        energy_consumption = EnergyConsumptionServiceFactory.create()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption.get_data(price)]

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

    energy_consumption = EnergyConsumptionServiceFactory.create()

    return [to_dict(date, row) for date, row in energy_consumption.get_monthly_data(price)]


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
@cache_control()
def mining_provinces(version=None):
    if version not in ['v1.1.0', 'v1.1.1', 'v1.2.0']:
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')
    headers = {
        'date': 'Date',
        'name': 'Province',
        # 'value': 'Share of global hashrate',
        'local_value': 'Share of Chinese hashrate'
    }
    rows = get_mining_provinces(version[1:])
    send_file_func = send_file(file_type=file_type)

    return send_file_func(headers,
                          list(map(lambda row: {**row, 'local_value': round(row['local_value'] * 100, 2)}, rows)))


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


@bp.route('/bitcoin_greenhouse_gas_emissions')
@cache_control()
def bitcoin_greenhouse_gas_emissions(version=None):
    if version != 'v1.1.1':
        raise NotImplementedError('Not Implemented')

    price = request.args.get('price', 0.05)
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh')

    headers = {
        'date': 'Date and Time',
        'min_co2': 'Hydro-only, MMTCDE',
        'guess_co2': 'Estimated, MMTCDE',
        'max_co2': 'Coal-only, MMTCDE',
    }

    service = GreenhouseGasEmissionServiceFactory.create()

    return send_file_func(
        headers,
        service.get_flat_greenhouse_gas_emissions(float(price)),
        filename='Annualised Bitcoin greenhouse gas emissions.csv'
    )


@bp.route('/total_bitcoin_greenhouse_gas_emissions')
@cache_control()
def total_bitcoin_greenhouse_gas_emissions(version=None):
    def to_dict(row):
        row['date'] = month_name[row['date'].month] + row['date'].strftime(' %Y')
        return row

    if version != 'v1.1.1':
        raise NotImplementedError('Not Implemented')

    price = request.args.get('price', 0.05)
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh')

    headers = {
        'date': 'Month',
        'v': 'Monthly emissions, MMTCDE',
        'cumulative_v': 'Cumulative emissions, MMTCDE',
    }

    service = GreenhouseGasEmissionServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_total_greenhouse_gas_emissions(float(price))],
        filename='Total Bitcoin greenhouse gas emissions.csv'
    )


@bp.route('/bitcoin_emission_intensity')
@cache_control()
def bitcoin_emission_intensity(version=None):
    def to_dict(row):
        row['date'] = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%dT%H:%M:%S')
        return row

    if version != 'v1.1.1':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'date': 'Date and Time',
        'co2_coef': 'Global hashrate-weighted CO2 Coefficient, gCO2eq/kWh',
    }

    service = EmissionIntensityServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_bitcoin_emission_intensity()],
        filename='Implied monthly Bitcoin emission intensity.csv'
    )


@bp.route('/monthly_bitcoin_power_mix')
@cache_control()
def monthly_bitcoin_power_mix(version=None):
    def to_dict(row):
        row['date'] = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d')
        return row

    if version != 'v1.1.1':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'date': 'Date',
        'name': 'Source',
        'value': "Share of Bitcoin's power mix (monthly average)",
    }

    service = PowerMixServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_monthly_data()],
        filename='Implied monthly Bitcoin power mix.csv'
    )


@bp.route('/yearly_bitcoin_power_mix')
@cache_control()
def yearly_bitcoin_power_mix(version=None):
    def to_dict(row):
        row['date'] = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d')
        return row

    if version != 'v1.1.1':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'date': 'Date',
        'name': 'Source',
        'value': 'Share of total power mix',
    }

    service = PowerMixServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_yearly_data()],
        filename='Implied yearly Bitcoin power mix.csv'
    )
