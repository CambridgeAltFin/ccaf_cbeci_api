from typing import List, Dict, Union
from flask import Blueprint, make_response, request
import csv
import io
from datetime import datetime
from services import EnergyConsumption, EnergyConsumptionByTypes, EnergyConsumptionPowerByTypes
from queries import get_mining_countries, get_mining_provinces
from packaging.version import parse as version_parse


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
        def to_dict_extended(timestamp, row):
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

        energy_consumption_by_types = EnergyConsumptionPowerByTypes()

        return [to_dict_extended(timestamp, row) for timestamp, row in energy_consumption_by_types.get_data(price)]

    func = locals().get(version.replace('.', '_'))
    if callable(func):
        return func(price)

    raise NotImplementedError('Not Implemented')


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


@bp.route('/mining_countries')
def mining_countries(version=None):
    if version_parse(version) < version_parse('v1.1.0'):
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')
    headers = {
        'date': 'Date',
        'name': 'Country',
        'value': 'Share of global hashrate',
    }
    rows = get_mining_countries()
    send_file_func = send_file(file_type=file_type)

    return send_file_func(headers, list(map(lambda row: {**row, 'value': round(row['value'] * 100, 2)}, rows)))


@bp.route('/mining_provinces')
def mining_provinces(version=None):
    if version_parse(version) < version_parse('v1.1.0'):
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')
    headers = {
        'date': 'Date',
        'name': 'Province',
        # 'value': 'Share of global hashrate',
        'local_value': 'Share of Chinese hashrate'
    }
    rows = get_mining_provinces()
    send_file_func = send_file(file_type=file_type)

    return send_file_func(headers,
                          list(map(lambda row: {**row, 'local_value': round(row['local_value'] * 100, 2)}, rows)))
