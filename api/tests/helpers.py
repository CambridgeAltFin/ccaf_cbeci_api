
from flask import Response

import csv
import json


def response_is_successful(response: Response) -> bool:
    return response.status_code == 200


def response_has_data_list(response: Response) -> bool:
    response_data = json.loads(response.data)
    return 'data' in response_data and type(response_data['data']) == list and len(response_data['data']) > 0


def response_data_item_has_keys(response: Response, keys: list) -> bool:
    response_data = json.loads(response.data)
    item = response_data['data'][0]
    return item_has_keys(item, keys)


def item_has_keys(item: dict, keys: list) -> bool:
    if type(item) != dict:
        return False
    for key in keys:
        if key not in item:
            return False
    return True


def response_is_csv(response: Response) -> bool:
    return response.headers.get('content-type') == 'text/csv'


def csv_has_headers(response: Response, columns: list, row_i: int = 0) -> bool:
    reader = csv.reader(response.data.decode('utf-8').split('\n'))
    for col in list(reader)[row_i]:
        if col not in columns:
            return False
    return True
