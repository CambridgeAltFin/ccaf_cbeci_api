from flask import Blueprint, jsonify
from extensions import cache
from decorators import price
from decorators.cache_control import cache_control
from components.eth import EthPowFactory


bp = Blueprint('eth_pow_charts', __name__, url_prefix='/eth/pow/charts')
eth_service = EthPowFactory.create_service()


@bp.route('/network_power_demand')
@bp.route('/network_power_demand/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def network_power_demand(value: float):
    return jsonify(data=eth_service.network_power_demand(value))


@bp.route('/annualised_consumption')
@bp.route('/annualised_consumption/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def annualised_consumption(value: float):
    return jsonify(data=eth_service.annualised_consumption(value))


@bp.route('/total_electricity_consumption/monthly')
@bp.route('/total_electricity_consumption/monthly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def monthly_total_electricity_consumption(value: float):
    return jsonify(data=eth_service.monthly_total_electricity_consumption(value))


@bp.route('/total_electricity_consumption/yearly')
@bp.route('/total_electricity_consumption/yearly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def yearly_total_electricity_consumption(value: float):
    return jsonify(data=eth_service.yearly_total_electricity_consumption(value))


@bp.route('/network_efficiency')
@cache_control()
@cache.memoize()
def network_efficiency():
    return jsonify(data=eth_service.network_efficiency())


@bp.route('/mining_equipment_efficiency')
@cache_control()
@cache.memoize()
def mining_equipment_efficiency():
    return jsonify(data=eth_service.mining_equipment_efficiency())


@bp.route('/profitability_threshold')
@bp.route('/profitability_threshold/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def profitability_threshold(value: float):
    return jsonify(data=eth_service.profitability_threshold(value))
