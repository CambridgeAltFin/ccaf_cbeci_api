import psycopg2
import datetime
from flask import Blueprint, jsonify, request
from config import config
from extensions import cache
from schema import Schema
from decorators import validators

bp = Blueprint('contribute', __name__, url_prefix='/contribute')

@cache.cached()
def get_countries():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM countries')
        return cursor.fetchall()

def datetime_validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')

        return True
    except ValueError:
        return False

schema = Schema({
    'data': [
        {
            'period': Schema(datetime_validate, error='should be date in "YYYY-MM-DD" format'),
            'frequency': Schema(lambda s: s in ('daily', 'weekly', 'biweekly', 'monthly',),
                                error='"frequency" should be "daily", "weekly", "biweekly" or "monthly"'),
            'country': Schema(lambda s: s in [row[0] for row in get_countries()], error='"country" should be one from the list, see documentation'),
            'province': Schema(str, error='"province" should be string'),
            'average_hashrate': Schema(float, error='"average_hashrate" should be numeric'),
            'unit': Schema(lambda s: s in ('th/s', 'ph/s', 'eh/s',), error='"unit" should be "th/s", "ph/s" or "eh/s"')
        }
    ]
}, ignore_extra_keys=True)

@bp.route('/miners_geo_distribution', methods=('POST',))
@validators.validate(schema)
def miners_geo_distribution():
    data = request.json['data']
    if len(data) > 0:
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            insert_sql = "INSERT INTO hashrate_geo_distribution (frequency, country, province, average_hashrate, unit, period)" \
                         " VALUES (%(frequency)s, %(country)s, %(province)s, %(average_hashrate)s, %(unit)s, %(period)s)"
            try:
                cursor.executemany(insert_sql, data)
            except Exception as error:
                return jsonify(data=data, status="fail", error=error)

    return jsonify(data=data, status="success")
