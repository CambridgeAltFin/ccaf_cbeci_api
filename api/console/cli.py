
import click
import logging


LOGGER = logging.getLogger()


@click.group(chain=True)
@click.option('--log-level', '-l', default=logging.INFO)
def cli(log_level):
    # Logging
    level = log_level.upper() if isinstance(log_level, str) else log_level
    LOGGER.setLevel(level)
    # Console outputs
    LOGGER.addHandler(logging.StreamHandler())
