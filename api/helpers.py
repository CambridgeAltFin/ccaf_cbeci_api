# from .extensions import cache
import calendar
import psycopg2.extras
from config import config
from flask import make_response
from typing import List, Dict, Union
import csv
import io
from datetime import timedelta, datetime


# =============================================================================
# functions for loading data
# =============================================================================
# @cache.memoize()
def load_typed_hasrates(table='hash_rate_by_types'):
    with psycopg2.connect(**config['blockchain_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        typed_hasrates = {}
        hash_rate_types = ['s7', 's9']
        for hash_rate_type in hash_rate_types:
            cursor.execute(f'SELECT type, value, date FROM {table} WHERE type = %s;', (hash_rate_type,))
            data = cursor.fetchall()
            formatted_data = {}
            for row in data:
                formatted_data[calendar.timegm(row['date'].timetuple())] = row
            typed_hasrates[hash_rate_type] = formatted_data
    return typed_hasrates


# =============================================================================
# functions for hash rate calculation
# =============================================================================
def get_hash_rates_by_miners_types(typed_hasrates, timestamp):
    if typed_hasrates is None:
        return None
    hash_rates = {}
    for miner_type, hr in typed_hasrates.items():
        hash_rates[miner_type] = hr[timestamp]['value']
    return hash_rates


# @cache.memoize()
def get_avg_effciency_by_miners_types(miners):
    miners_by_types = {}
    typed_avg_effciency = {}
    for miner in miners:
        type = miner['type']
        if type:
            if type not in miners_by_types:
                miners_by_types[type] = []
            miners_by_types[type].append(miner['efficiency_j_gh'])

    for t, efficiencies in miners_by_types.items():
        typed_avg_effciency[t] = sum(efficiencies) / len(efficiencies)

    return typed_avg_effciency


# @todo: replace this by 'get_avg_effciency_by_miners_types' and remove then
def get_avg_effciency_by_miners_types_old(miners):
    miners_by_types = {}
    typed_avg_effciency = {}
    for miner in miners:
        type = miner[5]
        if type:
            if type not in miners_by_types:
                miners_by_types[type] = []
            miners_by_types[type].append(miner[2])

    for t, efficiencies in miners_by_types.items():
        typed_avg_effciency[t] = sum(efficiencies) / len(efficiencies)

    return typed_avg_effciency
# @todo: / replace this by 'get_avg_effciency_by_miners_types' and remove then


def get_guess_consumption(prof_eqp, hash_rate, hash_rates, typed_avg_effciency):
    if len(prof_eqp) == 0:
        return 0
    guess_consumption = sum(prof_eqp) / len(prof_eqp)
    for t, hr in hash_rates.items():
        guess_consumption += hr * typed_avg_effciency.get(t.lower(), 0)
    return guess_consumption * hash_rate * 365.25 * 24 / 1e9 * 1.1


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


def daterange(start_date, end_date) -> list[datetime]:
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def is_valid_date_string_format(date: str, date_format: str = '%Y-%m-%d') -> bool:
    try:
        datetime.strptime(date, date_format)
    except ValueError:
        return False
    return True
