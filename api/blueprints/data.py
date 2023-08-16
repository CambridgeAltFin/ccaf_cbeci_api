from flask import Blueprint, jsonify
from extensions import cache

from components.energy_consumption import EnergyConsumptionServiceFactory
from resources.data import DataResource, MonthlyDataResource, StatsResource, YearlyDataResource
from decorators import price
from decorators.cache_control import cache_control

bp = Blueprint('data', __name__, url_prefix='data')


@bp.route('/')
@bp.route('/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def data(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    return jsonify(
        data=[DataResource(timestamp, row) for timestamp, row in energy_consumption_service.get_data(float(value))])


@bp.route('monthly/')
@bp.route('monthly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def monthly_data(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    return jsonify(data=[MonthlyDataResource(date, row) for date, row in
                         energy_consumption_service.get_monthly_data(float(value))])


@bp.route('yearly/')
@bp.route('yearly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def yearly_data(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    return jsonify(data=[YearlyDataResource(date, row) for date, row in
                         energy_consumption_service.get_yearly_data(float(value))])


@bp.route('stats/')
@bp.route('stats/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def stats(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    return jsonify(StatsResource(energy_consumption_service.get_actual_data(float(value))))
