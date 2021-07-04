from typing import List, Dict, Union
from flask import Blueprint, make_response, request
import csv
import io
import psycopg2
from datetime import datetime
from config import config, start_date
from services.energy_consumption import EnergyConsumption

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
        # @todo: implement
        def get_energy_consumption_ma():
            with psycopg2.connect(**config['blockchain_data']) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM energy_consumption_ma WHERE timestamp >= %s', (start_date.timestamp(),))
                return cursor.fetchall()

        return get_energy_consumption_ma()

    func = locals().get(version.replace('.', '_'))
    if callable(func):
        return func(price)

    raise NotImplementedError('Not Implemented')


def send_file(file_type='csv'):
    def send_csv(headers: Dict[str, str], rows: List[Dict[str, Union[str, int, float]]], filename='export.csv'):
        si = io.StringIO()
        cw = csv.DictWriter(si, fieldnames=headers.keys())
        cw.writerow(headers)
        cw.writerows(rows)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = f"attachment; filename={filename}"
        output.headers["Content-type"] = "text/csv"
        return output

    return send_csv


@bp.route('/data')
def download(version=None):
    file_type = request.args.get('file_type', 'csv')
    price = request.args.get('price', 0.05)
    headers = {
        'timestamp': 'Timestamp',
        'date': 'Date and Time',
        'max_consumption': 'MAX',
        'min_consumption': 'MIN',
        'guess_consumption': 'GUESS'
    }
    rows = get_data(version, price)
    send_file_func = send_file(file_type)

    return send_file_func(headers, rows)
