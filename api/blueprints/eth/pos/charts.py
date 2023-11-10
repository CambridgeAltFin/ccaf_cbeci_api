from flask import Blueprint, jsonify, request
from extensions import cache
from decorators.cache_control import cache_control
from components.eth import EthPosFactory

bp = Blueprint('eth_pos_charts', __name__, url_prefix='/eth/pos/charts')
eth_service = EthPosFactory.create_service()


@bp.route('/network_power_demand')
@cache_control()
@cache.memoize()
def network_power_demand():
    return jsonify(data=eth_service.network_power_demand())


@bp.route('/annualised_consumption')
@cache_control()
@cache.memoize()
def annualised_consumption():
    return jsonify(data=eth_service.annualised_consumption())


@bp.route('/total_electricity_consumption/monthly')
@cache_control()
@cache.memoize()
def monthly_total_electricity_consumption():
    return jsonify(data=eth_service.monthly_total_electricity_consumption())


@bp.route('/total_electricity_consumption/yearly')
@cache_control()
@cache.memoize()
def yearly_total_electricity_consumption():
    return jsonify(data=eth_service.yearly_total_electricity_consumption())


@bp.route('/client_distribution')
@cache_control()
@cache.memoize()
def client_distribution():
    return jsonify(data=eth_service.client_distribution())


@bp.route('/active_nodes')
@cache_control()
@cache.memoize()
def active_nodes():
    return jsonify(data=eth_service.active_nodes())


@bp.route('/node_distribution')
@bp.route('/node_distribution/<date>')
@cache_control()
@cache.cached(query_string=True)
def node_distribution(date=None):
    if date is None and request.args.get('date') is not None:
        date = request.args.get('date')
    data, meta = eth_service.node_distribution(date)
    return jsonify(data=data, meta=meta)


@bp.route('/monthly_node_distribution')
@bp.route('/monthly_node_distribution/<date>')
@cache_control()
@cache.cached(query_string=True)
def monthly_node_distribution(date=None):
    if date is None and request.args.get('date') is not None:
        date = request.args.get('date')
    data, meta = eth_service.monthly_node_distribution(date)
    return jsonify(data=data, meta=meta)


@bp.route('/power_demand_legacy_vs_future')
@bp.route('/power_demand_legacy_vs_future/<date>')
@cache_control()
@cache.memoize()
def power_demand_legacy_vs_future(date=None):
    return jsonify(data=eth_service.power_demand_legacy_vs_future(date))


@bp.route('/comparison_of_annual_consumption')
@bp.route('/comparison_of_annual_consumption/<date>')
@cache_control()
@cache.memoize()
def comparison_of_annual_consumption(date=None):
    return jsonify(data=eth_service.comparison_of_annual_consumption(date))


@bp.route('/greenhouse_gas_emissions')
@cache_control()
@cache.memoize()
def greenhouse_gas_emissions():
    return jsonify(data=eth_service.greenhouse_gas_emissions())


@bp.route('/total_greenhouse_gas_emissions/monthly')
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_monthly():
    return jsonify(data=eth_service.total_greenhouse_gas_emissions_monthly())


@bp.route('/total_greenhouse_gas_emissions/yearly')
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_yearly():
    return jsonify(data=eth_service.total_greenhouse_gas_emissions_yearly())


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
