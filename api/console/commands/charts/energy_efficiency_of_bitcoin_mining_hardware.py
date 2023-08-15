import click
import psycopg2
import psycopg2.extras
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd
from pandas.tseries.offsets import DateOffset
import re
import requests
from io import StringIO

from config import config
from components.energy_consumption.v1_3_1 import EnergyConsumptionServiceFactory as LegacyEnergyConsumptionServiceFactory
from components.energy_consumption import EnergyConsumptionServiceFactory


weight_map = {0: 1, 1: 0.8, 2: 0.6, 3: 0.4, 4: 0.2, "False": 0}


@click.command(name='charts:save:energy-efficiency-of-bitcoin-mining-hardware')
@click.option('-v', '--version', default='1.4.0')
def handle(version):
    pd.set_option('display.max_columns', None)
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        service = EnergyConsumptionServiceFactory.create()
        equipment_list = service.get_equipment_list(.05)
        print(equipment_list)
        return
        # psycopg2.extras.execute_values(
        #     cursor,
        #     '''
        #     insert into miner_energy_efficients (lower_bound, estimated_legacy, upper_bound, date)
        #     values %s
        #     on conflict (date) do update set
        #      lower_bound = EXCLUDED.lower_bound,
        #      estimated_legacy = EXCLUDED.estimated_legacy,
        #      upper_bound = EXCLUDED.upper_bound
        #     ''',
        #     [(
        #         x['profitability_equipment_lower_bound'],
        #         x['profitability_equipment'],
        #         x['profitability_equipment_upper_bound'],
        #         x['date']
        #     ) for x in legacy_service.get_equipment_list(.05)]
        # )

        url = "https://demo.ccaf.io/cbeci/api/v1.2.0/download/profitability_equipment?price=0.05"
        df = pd.read_csv(url)
        df["Date"] = pd.to_datetime(df["Date"])
        df["Date"] = df["Date"].dt.date
        grouped_df = df.groupby(["Date", "Equipment list"]).sum()
        split_df = grouped_df.reset_index()
        split_df = split_df.join(split_df["Equipment list"].str.split("; ", expand=True).add_prefix("Equipment "))
        split_df.drop("Equipment list", axis=1, inplace=True)

        cursor.execute(
            'select *, to_timestamp(unix_date_of_release) as date_of_release from miners where is_active = true'
        )
        miner_data = pd.DataFrame.from_records(cursor.fetchall())
        miner_data['Date of release'] = pd.to_datetime(
            miner_data['date_of_release'].astype(str),
            infer_datetime_format=True
        )
        miner_data['Date of release'] = miner_data['Date of release'].apply(lambda x: x + DateOffset(months=2))

        dfs = []
        for day in split_df["Date"].unique():
            day_df = filter_data_for_date(day, split_df, miner_data)
            dfs.append(day_df)
        final_df = pd.concat(dfs).reset_index(drop=True)
        final_df['Years'] = final_df.apply(calculate_years, axis=1)
        final_df['Weight'] = final_df['Years'].map(weight_map)
        final_df = final_df.groupby("Date").apply(calculate_adjustment)
        print(final_df)
        return
        final_df["Adjusted weight"] = (final_df["power"] / final_df["hashing_power"]) \
                                      * final_df["Adjustment"]

        efficiency_df = final_df.groupby("Date")["Adjusted weight"].sum().reset_index()
        efficiency_df.rename(columns={"Adjusted weight": "Efficiency in W/Th"}, inplace=True)

        print(efficiency_df)


def calculate_adjustment(group):
    total_weight = group["Weight"].sum()
    count = len(group)
    factor = (count / total_weight) * (1 / count)
    group["Adjustment"] = group["Weight"] * factor
    return group


def calculate_years(row):
    difference = (row["Date"].year - row["Date of release"].year) * 12 + (row["Date"].month - row["Date of release"].month)
    if difference < 0:
        return "False"
    elif difference < 12:
        return 0
    elif 12 <= difference < 24:
        return 1
    elif 24 <= difference < 36:
        return 2
    elif 36 <= difference < 48:
        return 3
    elif 48 <= difference < 60:
        return 4
    else:
        return "False"


def replace_non_breaking_space(value):
    return value.replace('\xa0', ' ')


def clean_miner_name(name):
    name = re.sub("Micro\s?BT", "MicroBT", name, flags=re.IGNORECASE)
    return re.sub(" -.*", "", name)


def filter_data_for_date(date, split_df, miner_data):
    day_df = split_df[split_df["Date"] == date]
    equipment_columns = [col for col in day_df.columns if col.startswith("Equipment ")]
    day_miners = day_df[equipment_columns].melt(value_name="Miner_name").dropna()["Miner_name"].unique()

    # Clean the miner names to remove any additional text
    day_miners = [clean_miner_name(name) for name in day_miners]

    # Normalize miner names in miner_data for comparison
    normalized_miner_names = miner_data["miner_name"].apply(clean_miner_name)
    filtered_miner_data = miner_data[normalized_miner_names.isin(day_miners)]

    filtered_miner_data["Date"] = date

    return filtered_miner_data
