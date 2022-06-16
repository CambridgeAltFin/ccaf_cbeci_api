from flask import Blueprint, jsonify
from decorators.cache_control import cache_control
from components.gas_emission import EmissionServiceFactory
from resources.gas_emission.ghg_historical_emission import GhgHistoricalEmission
from resources.gas_emission.ghg_emission_intensity import GhgEmissionIntensity

bp = Blueprint('ghg', __name__, url_prefix='/ghg')


@bp.route('/world_emission')
@cache_control()
def ghg_world_emission():
    emissions = EmissionServiceFactory.create()

    return jsonify(data=GhgHistoricalEmission(emissions.get_world_emission()))


@bp.route('/emissions')
@cache_control()
def ghg_emissions():
    emissions = EmissionServiceFactory.create()

    return jsonify(data=[GhgHistoricalEmission(emission) for emission in emissions.get_emissions(40)])


@bp.route('/emission_intensities')
@cache_control()
def ghg_emission_intensities():
    emissions = EmissionServiceFactory.create()

    return jsonify(data=[GhgEmissionIntensity(emission) for emission in emissions.get_emission_intensities()])
