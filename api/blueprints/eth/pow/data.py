from flask import Blueprint, jsonify
from extensions import cache
from decorators import price
from decorators.cache_control import cache_control
from components.eth import EthPowFactory


bp = Blueprint('eth_pow_data', __name__, url_prefix='/eth/pow/data')
eth_service = EthPowFactory.create_service()


@bp.route('/stats')
@bp.route('/stats/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def network_power_demand(value: float):
    return jsonify(data=eth_service.stats(value))
