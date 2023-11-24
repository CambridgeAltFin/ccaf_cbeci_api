from flask import Blueprint, jsonify, request
from components.kernel_density_estimation import KernelDensityEstimationFactory

bp = Blueprint('dmd', __name__, url_prefix='dmd')

@bp.route('/kde', methods=['POST'])
def data():
    req_body = request.json

    validator = KernelDensityEstimationFactory.create_validator()
    if not validator.validate(req_body):
        return jsonify(errors=validator.get_errors()), 422

    service = KernelDensityEstimationFactory.create_service()
    return jsonify(
        data=service.get_kde(
            values = req_body.get('values'),
            maxY = req_body.get('maxy'), 
            bandwidth = req_body.get('bandwidth'),
            start = req_body.get('from'),
            end = req_body.get('to'),
            count = req_body.get('count')
            ))
