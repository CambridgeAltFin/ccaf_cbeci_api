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


@bp.route('/greenhouse_gas_emissions')
@bp.route('/greenhouse_gas_emissions/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def greenhouse_gas_emissions(version: str, value: float):
    if version_parse(version) == version_parse('v1.4.0'):
        return eth_service.download_greenhouse_gas_emissions(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/total_greenhouse_gas_emissions/monthly')
@bp.route('/total_greenhouse_gas_emissions/monthly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_monthly(version: str, value: float):
    if version_parse(version) == version_parse('v1.4.0'):
        return eth_service.download_total_greenhouse_gas_emissions_monthly(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/total_greenhouse_gas_emissions/yearly')
@bp.route('/total_greenhouse_gas_emissions/yearly/<value>')
@price.get_price()
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_yearly(version: str, value: float):
    if version_parse(version) == version_parse('v1.4.0'):
        return eth_service.download_total_greenhouse_gas_emissions_yearly(value)
    raise NotImplementedError('Not Implemented')


@bp.route('/power_mix/monthly')
@cache_control()
@cache.memoize()
def monthly_power_mix(version=None):
    if version_parse(version) == version_parse('v1.4.0'):
        return eth_service.download_monthly_power_mix()
    raise NotImplementedError('Not Implemented')


@bp.route('/power_mix/yearly')
@cache_control()
@cache.memoize()
def yearly_power_mix(version=None):
    if version_parse(version) == version_parse('v1.4.0'):
        return eth_service.download_yearly_power_mix()
    raise NotImplementedError('Not Implemented')


@bp.route('/emission_intensity')
@cache_control()
@cache.memoize()
def emission_intensity(version=None):
    if version_parse(version) == version_parse('v1.4.0'):
        return eth_service.download_emission_intensity()
    raise NotImplementedError('Not Implemented')


@bp.route('/emission_intensity/monthly')
@cache_control()
@cache.memoize()
def emission_intensity_monthly(version=None):
    if version_parse(version) == version_parse('v1.4.0'):
        return eth_service.download_monthly_emission_intensity()
    raise NotImplementedError('Not Implemented')
