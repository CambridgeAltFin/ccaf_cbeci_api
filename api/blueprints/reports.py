import datetime
from config import BUCKETS, FOLDERS
from flask import Blueprint, jsonify, request, abort
from firebase_admin import storage
from services.realtime_collection import realtime_collections, Collections

bp = Blueprint('reports.py', __name__, url_prefix='/reports.py')


def to_obj(doc):
    doc_copy = doc.copy()
    doc_copy['fileurl'] = [
        storage.bucket(BUCKETS['default']) \
            .blob(f'{FOLDERS["reports"]}/{doc["filename"]}') \
            .generate_signed_url(
            expiration=datetime.timedelta(days=1),
        )
    ]

    return doc_copy

@bp.route('/')
def index():
    project = request.args.get('project', 'cbeci')
    docs = realtime_collections.collections[Collections.REPORTS].get()

    def docs_filter(doc):
        return doc.get('is_active') == True and doc.get('project') == project

    return jsonify(data=list(sorted(map(to_obj, filter(docs_filter, docs)), key=lambda doc: doc.get('order_position'))))


@bp.route('/<string:key>')
def show(key):
    doc = realtime_collections.collections[Collections.REPORTS].get(key)

    if doc is None or not doc.get('is_active'):
        abort(404)

    return jsonify(data=doc)
