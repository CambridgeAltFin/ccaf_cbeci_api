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
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_network_power_demand()
    raise NotImplementedError('Not Implemented')


@bp.route('/annualised_consumption')
@cache_control()
@cache.memoize()
def annualised_consumption(version: str):
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_annualised_consumption()
    raise NotImplementedError('Not Implemented')


@bp.route('/total_electricity_consumption/monthly')
@cache_control()
@cache.memoize()
def monthly_total_electricity_consumption(version: str):
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_monthly_total_electricity_consumption()
    raise NotImplementedError('Not Implemented')


@bp.route('/total_electricity_consumption/yearly')
@cache_control()
@cache.memoize()
def yearly_total_electricity_consumption(version: str):
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_yearly_total_electricity_consumption()
    raise NotImplementedError('Not Implemented')


@bp.route('/client_distribution')
@cache_control()
@cache.memoize()
def client_distribution(version: str):
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_client_distribution()
    raise NotImplementedError('Not Implemented')


@bp.route('/active_nodes')
@cache_control()
@cache.memoize()
def active_nodes(version: str):
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_active_nodes()
    raise NotImplementedError('Not Implemented')


@bp.route('/node_distribution')
@cache_control()
@cache.memoize()
def node_distribution(version: str):
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_node_distribution()
    raise NotImplementedError('Not Implemented')


@bp.route('/monthly_node_distribution')
@cache_control()
@cache.memoize()
def monthly_node_distribution(version: str):
    if version_parse(version) == version_parse('v1.2.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_monthly_node_distribution()
    raise NotImplementedError('Not Implemented')


@bp.route('/greenhouse_gas_emissions')
@cache_control()
@cache.memoize()
def greenhouse_gas_emissions(version=None):
    if version_parse(version) == version_parse('v1.4.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_greenhouse_gas_emissions()
    raise NotImplementedError('Not Implemented')


@bp.route('/total_greenhouse_gas_emissions/monthly')
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_monthly(version=None):
    if version_parse(version) == version_parse('v1.4.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_total_greenhouse_gas_emissions_monthly()
    raise NotImplementedError('Not Implemented')


@bp.route('/total_greenhouse_gas_emissions/yearly')
@cache_control()
@cache.memoize()
def total_greenhouse_gas_emissions_yearly(version=None):
    if version_parse(version) == version_parse('v1.4.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_total_greenhouse_gas_emissions_yearly()
    raise NotImplementedError('Not Implemented')


@bp.route('/power_mix/monthly')
@cache_control()
@cache.memoize()
def monthly_power_mix(version=None):
    if version_parse(version) == version_parse('v1.4.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_monthly_power_mix()
    raise NotImplementedError('Not Implemented')


@bp.route('/power_mix/yearly')
@cache_control()
@cache.memoize()
def yearly_power_mix(version=None):
    if version_parse(version) == version_parse('v1.4.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_yearly_power_mix()
    raise NotImplementedError('Not Implemented')


@bp.route('/emission_intensity')
@cache_control()
@cache.memoize()
def emission_intensity(version=None):
    if version_parse(version) == version_parse('v1.4.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_emission_intensity()
    raise NotImplementedError('Not Implemented')


@bp.route('/emission_intensity/monthly')
@cache_control()
@cache.memoize()
def emission_intensity_monthly(version=None):
    if version_parse(version) == version_parse('v1.4.0') or version_parse(version) == version_parse('v1.5.0'):
        return eth_service.download_monthly_emission_intensity()
    raise NotImplementedError('Not Implemented')
