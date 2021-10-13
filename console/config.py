
import yaml
import logging


config_path = 'CONFIG.yml'
if config_path:
    with open(config_path) as fp:
        config = yaml.load(fp, yaml.FullLoader)
else:
    config = {}

DEFAULT_LOG_LEVEL = logging.INFO
