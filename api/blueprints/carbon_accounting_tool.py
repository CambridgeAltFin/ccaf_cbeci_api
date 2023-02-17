from flask import Blueprint, jsonify, request

from components.carbon_accounting_tool import CarbonAccountingToolFactory

bp = Blueprint('carbon_accounting_tool', __name__, url_prefix='data')


@bp.route('/calculate', methods=['POST'])
def calculate():
    req_body = request.json
    validator = CarbonAccountingToolFactory.create_validator()
    if not validator.validate(req_body):
        return jsonify(errors=validator.get_errors()), 422
    calculator = CarbonAccountingToolFactory.create_service()
    return jsonify(data={'result': calculator.calculate_kg_co2e(req_body.get('data'))})


@bp.route('/download/calculation', methods=['POST'])
def download_calculation():
    req_body = request.json
    validator = CarbonAccountingToolFactory.create_validator()
    if not validator.validate(req_body):
        return jsonify(errors=validator.get_errors()), 422
    calculator = CarbonAccountingToolFactory.create_service()
    return calculator.download_calculation(req_body.get('data'))
