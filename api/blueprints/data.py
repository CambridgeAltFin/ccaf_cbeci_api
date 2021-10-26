from flask import Blueprint, jsonify
from extensions import cache

from services.energy_analytic import EnergyAnalytic
from resources.data import DataResource, MonthlyDataResource, StatsResource
from decorators import price
from decorators.cache_control import cache_control

bp = Blueprint('data', __name__, url_prefix='data')


@bp.route('/')
@bp.route('/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def data(value):
    energy_analytic = EnergyAnalytic()
    return jsonify(data=[DataResource(timestamp, row) for timestamp, row in energy_analytic.get_data(float(value))])


@bp.route('monthly/')
@bp.route('monthly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def monthly_data(value):
    energy_analytic = EnergyAnalytic()
    return jsonify(data=[MonthlyDataResource(date, row) for date, row in energy_analytic.get_monthly_data(float(value))])


@bp.route('stats/')
@bp.route('stats/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def stats(value):
    energy_analytic = EnergyAnalytic()
    return jsonify(StatsResource(energy_analytic.get_actual_data(float(value))))
