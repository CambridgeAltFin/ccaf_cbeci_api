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


@bp.route('/source_comparison')
@cache_control()
@cache.memoize()
def source_comparison():
    return jsonify(data=eth_service.source_comparison())


@bp.route('/greenhouse_gas_emissions')
@bp.route('/greenhouse_gas_emissions/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def greenhouse_gas_emissions(value: float):
    return jsonify(data=eth_service.greenhouse_gas_emissions(value))


@bp.route('/total_greenhouse_gas_emissions/monthly')
@bp.route('/total_greenhouse_gas_emissions/monthly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_monthly(value: float):
    return jsonify(data=eth_service.total_greenhouse_gas_emissions_monthly(value))


@bp.route('/total_greenhouse_gas_emissions/yearly')
@bp.route('/total_greenhouse_gas_emissions/yearly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_yearly(value: float):
    return jsonify(data=eth_service.total_greenhouse_gas_emissions_yearly(value))


@bp.route('/power_mix/monthly')
@cache_control()
@cache.memoize()
def power_mix_monthly():
    return jsonify(data=eth_service.monthly_power_mix())


@bp.route('/power_mix/yearly')
@cache_control()
@cache.memoize()
def power_mix_yearly():
    return jsonify(data=eth_service.yearly_power_mix())


@bp.route('/emission_intensity')
@cache_control()
@cache.memoize()
def emission_intensity():
    return jsonify(data=eth_service.emission_intensity())


@bp.route('/emission_intensity/monthly')
@cache_control()
@cache.memoize()
def monthly_emission_intensity():
    return jsonify(data=eth_service.monthly_emission_intensity())


@bp.route('/mining_map')
@cache_control()
@cache.memoize()
def mining_map():
    return jsonify(data=eth_service.mining_map())
