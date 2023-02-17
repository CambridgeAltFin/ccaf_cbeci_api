
from chart_API import app
from console import cli, commands


cli.add_command(commands.charts.save_mining_map_countries)
cli.add_command(commands.charts.save_mining_map_provinces)
cli.add_command(commands.charts.save_mining_area_countries)
cli.add_command(commands.charts.save_mining_area_provinces)
cli.add_command(commands.charts.calc_bitcoin_emission_intensity)
cli.add_command(commands.charts.calc_bitcoin_greenhouse_gas_emissions)

cli.add_command(commands.countries.save_country_id)
cli.add_command(commands.countries.save_code_3)

cli.add_command(commands.data.coinmetrics)
cli.add_command(commands.data.hashrate)
cli.add_command(commands.data.calc_energy_consumptions)
cli.add_command(commands.data.cumulative_electricity_estimates_calc)
cli.add_command(commands.data.calc_co2_coefficient)
cli.add_command(commands.data.save_btc_to_countries)

cli.add_command(commands.data.ghg_historical_emissions)
cli.add_command(commands.data.ghg_emission_intensities)

cli.add_command(commands.carbon_accounting_tool.calc_carbon_ratings)

cli()
