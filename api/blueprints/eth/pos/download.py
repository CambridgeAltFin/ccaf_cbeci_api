from flask import Blueprint
from extensions import cache
from decorators.cache_control import cache_control
from components.eth import EthPosFactory
from packaging.version import parse as version_parse


bp = Blueprint('eth_pos_download', __name__, url_prefix='/eth/pos/download')
eth_service = EthPosFactory.create_service()


@bp.route('/network_power_demand')
@cache_control()
@cache.memoize()
def network_power_demand(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_network_power_demand()
    raise NotImplementedError('Not Implemented')


@bp.route('/annualised_consumption')
@cache_control()
@cache.memoize()
def annualised_consumption(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_annualised_consumption()
    raise NotImplementedError('Not Implemented')


@bp.route('/total_electricity_consumption/monthly')
@cache_control()
@cache.memoize()
def monthly_total_electricity_consumption(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_monthly_total_electricity_consumption()
    raise NotImplementedError('Not Implemented')


@bp.route('/total_electricity_consumption/yearly')
@cache_control()
@cache.memoize()
def yearly_total_electricity_consumption(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_yearly_total_electricity_consumption()
    raise NotImplementedError('Not Implemented')


@bp.route('/client_distribution')
@cache_control()
@cache.memoize()
def client_distribution(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_client_distribution()
    raise NotImplementedError('Not Implemented')


@bp.route('/active_nodes')
@cache_control()
@cache.memoize()
def active_nodes(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_active_nodes()
    raise NotImplementedError('Not Implemented')


@bp.route('/node_distribution')
@cache_control()
@cache.memoize()
def node_distribution(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_node_distribution()
    raise NotImplementedError('Not Implemented')
