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