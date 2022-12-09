from flask import Blueprint, jsonify
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
@cache_control()
@cache.memoize()
def node_distribution():
    return jsonify(data=eth_service.node_distribution())


@bp.route('/power_demand_legacy_vs_future')
@cache_control()
@cache.memoize()
def power_demand_legacy_vs_future():
    return jsonify(data=eth_service.power_demand_legacy_vs_future())


@bp.route('/comparison_of_annual_consumption')
@cache_control()
@cache.memoize()
def comparison_of_annual_consumption():
    return jsonify(data=eth_service.comparison_of_annual_consumption())