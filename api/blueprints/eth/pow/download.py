from flask import Blueprint
from extensions import cache
from decorators import price
from decorators.cache_control import cache_control
from components.eth import EthPowFactory
from packaging.version import parse as version_parse


bp = Blueprint('eth_pow_download', __name__, url_prefix='/eth/pow/download')
eth_service = EthPowFactory.create_service()


@bp.route('/network_power_demand')
@bp.route('/network_power_demand/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def network_power_demand(version: str, value: float):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_network_power_demand(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/annualised_consumption')
@bp.route('/annualised_consumption/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def annualised_consumption(version: str, value: float):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_annualised_consumption(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/total_electricity_consumption/monthly')
@bp.route('/total_electricity_consumption/monthly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def monthly_total_electricity_consumption(version: str, value: float):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_monthly_total_electricity_consumption(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/total_electricity_consumption/yearly')
@bp.route('/total_electricity_consumption/yearly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def yearly_total_electricity_consumption(version: str, value: float):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_yearly_total_electricity_consumption(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/network_efficiency')
@cache_control()
@cache.memoize()
def network_efficiency(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_network_efficiency()
    raise NotImplementedError('Not Implemented')


@bp.route('/mining_equipment_efficiency')
@cache_control()
@cache.memoize()
def mining_equipment_efficiency(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_mining_equipment_efficiency()
    raise NotImplementedError('Not Implemented')


@bp.route('/profitability_threshold')
@bp.route('/profitability_threshold/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def profitability_threshold(version: str, value: float):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_profitability_threshold(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/source_comparison')
@cache_control()
@cache.memoize()
def source_comparison(version: str):
    if version_parse(version) == version_parse('v1.2.0'):
        return eth_service.download_source_comparison()
    raise NotImplementedError('Not Implemented')
