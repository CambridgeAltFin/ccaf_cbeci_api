from extensions import cache
from flask import Blueprint, jsonify
from decorators.cache_control import cache_control
from components.gas_emission import EmissionServiceFactory
from resources.gas_emission.ghg_historical_emission import GhgHistoricalEmission
from resources.gas_emission.ghg_emission_intensity import GhgEmissionIntensity

bp = Blueprint('ghg', __name__, url_prefix='/ghg')


@bp.route('/annualised_emission')
@cache_control()
@cache.memoize()
def ghg_annualised_emission():
    emissions = EmissionServiceFactory.create()
    world, btc = emissions.get_annualised_emission()

    return jsonify(data={
        'BTC': GhgHistoricalEmission(btc),
        'WORLD': GhgHistoricalEmission(world),
    })


@bp.route('/emissions')
@cache_control()
@cache.memoize()
def ghg_emissions():
    emissions = EmissionServiceFactory.create()

    return jsonify(data=[GhgHistoricalEmission(emission) for emission in emissions.get_emissions()])


@bp.route('/emission_intensities')
@cache_control()
@cache.memoize()
def ghg_emission_intensities():
    emissions = EmissionServiceFactory.create()

    return jsonify(data=[GhgEmissionIntensity(emission) for emission in emissions.get_emission_intensities()])


@bp.route('/ranking')
@cache_control()
@cache.memoize()
def ghg_ranking():
    emissions = EmissionServiceFactory.create()

    bitcoin_ghg_emissions = next((i for i, item in enumerate(emissions.get_emissions()) if item['code'] == 'BTC'), -1)

    return jsonify(data={
        'bitcoin_ghg_emissions': bitcoin_ghg_emissions
    })
