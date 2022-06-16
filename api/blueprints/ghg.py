from flask import Blueprint, jsonify
from decorators.cache_control import cache_control
from components.gas_emission import EmissionServiceFactory
from resources.gas_emission.global_historical_emission import GlobalHistoricalEmission

bp = Blueprint('ghg', __name__, url_prefix='/ghg')


@bp.route('/global_emission')
@cache_control()
def mining_equipment_efficiency():
    emissions = EmissionServiceFactory.create()

    return jsonify(data=GlobalHistoricalEmission(emissions.get_global_emissions()))
