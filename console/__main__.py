
import click
import logging

import config
import commands


LOGGER = logging.getLogger()


@click.group()
@click.option('--log-level', '-l', default=config.DEFAULT_LOG_LEVEL)
def cli(log_level):
    # Logging
    level = log_level.upper() if isinstance(log_level, str) else log_level
    LOGGER.setLevel(level)
    # Console outputs
    LOGGER.addHandler(logging.StreamHandler())


cli.add_command(commands.charts.save_mining_map_countries)
cli.add_command(commands.charts.save_mining_map_provinces)
cli.add_command(commands.charts.save_mining_area_countries)
cli.add_command(commands.charts.save_mining_area_provinces)

if __name__ == '__main__':
    cli()
