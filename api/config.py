from datetime import datetime
import os
import yaml

def get_config(config_path):
    with open(config_path) as fp:
        return yaml.load(fp, yaml.FullLoader)

config = get_config(os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yml'))

start_date = datetime(year=2014, month=7, day=1)