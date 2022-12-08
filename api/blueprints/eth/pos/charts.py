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
