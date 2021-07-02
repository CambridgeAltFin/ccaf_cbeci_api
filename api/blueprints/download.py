from typing import List, Tuple
from flask import Blueprint, make_response, request
import csv
import io
import psycopg2
from config import config, start_date

bp = Blueprint('download', __name__, url_prefix='/download')


def get_data(version=None):
    def v1_0_5 ():
        # @todo: implement
        return []

    def v1_1_0 ():
        def get_energy_consumption_ma():
            with psycopg2.connect(**config['blockchain_data']) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM energy_consumption_ma WHERE timestamp >= %s', (start_date.timestamp(),))
                return cursor.fetchall()

        return get_energy_consumption_ma()

    func = locals().get(version.replace('.', '_'))
    if callable(func):
        return func()

    raise NotImplementedError('Not Implemented')

def send_file(file_type='csv'):
    def send_csv(headers: List[str], rows: List[Tuple], filename='export.csv'):
        si = io.StringIO()
        cw = csv.writer(si)
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
    headers = ['Timestamp', 'Date and Time', 'MAX', 'MIN', 'GUESS']
    rows = get_data(version)
    send_file_func = send_file(file_type)

    return send_file_func(headers, rows)