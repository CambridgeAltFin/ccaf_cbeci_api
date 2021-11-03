from flask import Blueprint, jsonify, request, abort
from services.realtime_collection import realtime_collections, Collections

bp = Blueprint('text_pages', __name__, url_prefix='/text_pages')


@bp.route('/')
def index():
    project = request.args.get('project', 'cbeci')
    parent = request.args.get('parent')
    docs = realtime_collections.collections[Collections.TEXT_PAGES].get()

    def docs_filter(doc):
        if parent is not None and doc.get('parent') != parent:
            return False

        return doc.get('is_active') == True and doc.get('project') == project

    def doc_to_dict(doc):
        copy = {}

        for prop in ['id', 'is_active', 'title', 'parent']:
            if prop in doc:
                copy[prop] = doc[prop]

        return copy

    return jsonify(data=list(map(doc_to_dict, filter(docs_filter, docs))))


@bp.route('/<string:key>')
def show(key):
    doc = realtime_collections.collections[Collections.TEXT_PAGES].get(key)

    if doc is None or not doc.get('is_active'):
        abort(404)

    return jsonify(data=doc)
