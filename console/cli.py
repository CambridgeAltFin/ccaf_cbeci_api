
import click
import logging

import config


LOGGER = logging.getLogger()


@click.group()
@click.option('--log-level', '-l', default=config.DEFAULT_LOG_LEVEL)
def cli(log_level):
    # Logging
    level = log_level.upper() if isinstance(log_level, str) else log_level
    LOGGER.setLevel(level)
    # Console outputs
    LOGGER.addHandler(logging.StreamHandler())
