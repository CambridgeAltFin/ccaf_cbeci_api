from flask import Blueprint, jsonify, request, abort
from services.realtime_collection import realtime_collections, Collections

bp = Blueprint('sponsors', __name__, url_prefix='/sponsors')


@bp.route('/')
def index():
    pass
    project = request.args.get('project', 'cbeci')
    docs = realtime_collections.collections[Collections.SPONSORS].get()

    def docs_filter(doc):
        return doc.get('is_active') == True and doc.get('project') == project

    return jsonify(data=list(sorted(filter(docs_filter, docs), key=lambda doc: doc.get('order_position'))))


@bp.route('/<string:key>')
def show(key):
    doc = realtime_collections.collections[Collections.SPONSORS].get(key)

    if doc is None or not doc.get('is_active'):
        abort(404)

    return jsonify(data=doc)
