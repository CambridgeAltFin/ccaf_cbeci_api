from flask import Blueprint, jsonify
from extensions import cache
from decorators.cache_control import cache_control
from components.eth import EthPosFactory

bp = Blueprint('eth_pos_data', __name__, url_prefix='/eth/pos/data')
eth_service = EthPosFactory.create_service()


@bp.route('/stats')
@cache_control()
@cache.memoize()
def network_power_demand():
    return jsonify(data=eth_service.stats())


@bp.route('/stats/live')
@cache_control()
@cache.memoize()
def live_data():
    return jsonify(data=eth_service.get_live_data())


@bp.route('/ghg/live')
@cache_control()
@cache.memoize()
def ghg_live_data():
    return jsonify(data=eth_service.get_ghg_live_data())
