from datetime import datetime
import os
import yaml


def get_config(config_path):
    with open(config_path) as fp:
        return yaml.load(fp, yaml.FullLoader)


config = get_config(os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yml'))

start_date = datetime(year=2010, month=7, day=18)

DEFAULT_ELECTRICITY_PRICE = 0.05

map_start_date = '2019-09-01'
map_end_date = '2021-09-01'


class Connection:
    custom_data = 'custom_data'
    blockchain_data = 'blockchain_data'
