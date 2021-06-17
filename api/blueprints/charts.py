from flask import Blueprint, jsonify
import psycopg2
from config import config
from extensions import cache
import pandas as pd

bp = Blueprint('charts', __name__, url_prefix='/charts')


@bp.route('/mining_equipment_efficiency')
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
