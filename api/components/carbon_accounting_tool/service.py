from blueprints.download import send_file
from .repository import CarbonAccountingToolRepository
import pandas as pd
from datetime import date


class CarbonAccountingToolService:
    def __init__(self, repository: CarbonAccountingToolRepository):
        self.repository = repository

    def calculate_kg_co2e(self, input_data):
        df = self.calculate(input_data)
        return round(df['result'].sum(), 2)

    def download_calculation(self, input_data):
        df = self.calculate(input_data)
        send_file_func = send_file()
        return send_file_func({
            'date': 'Date',
            'holdings': 'AVG Bitcoin holdings',
            'transactions': 'Number of transactions',
            'result': 'SUM emissions, kgCO2e'
        }, [{
            'date': str(x['date'])[:10],
            'holdings': x['holdings'],
            'transactions': x['transactions'],
            'result': round(x['result'], 4)
        } for x in df.to_records()])

    def calculate(self, input_data):
        coefficients = self.repository.get_coefficients(input_data)
        df = pd.DataFrame.from_records(coefficients)
        df['date'] = pd.to_datetime(df['date'])
        df['holdings'] = 0
        df['transactions'] = 0
        for item in input_data:
            if not item.get('to'):
                df.loc[df['date'] == item.get('from'), 'holdings'] = item.get('holdings', 0)
                df.loc[df['date'] == item.get('from'), 'transactions'] = item.get('transactions', 0)
            else:
                from_year, from_month, from_day = item.get('from').split('-')
                to_year, to_month, to_day = item.get('to').split('-')
                days_count = (date(
                    int(to_year), int(to_month), int(to_day)
                ) - date(
                    int(from_year), int(from_month), int(from_day)
                )).days
                df.loc[(df['date'] >= item.get('from')) & (df['date'] <= item.get('to')), 'holdings'] = item.get(
                    'holdings', 0)
                df.loc[(df['date'] >= item.get('from')) & (df['date'] <= item.get('to')), 'transactions'] = item.get(
                    'transactions', 0) / (days_count + 1)
        df['result'] = (df['holdings'] * df['kg_per_holding']) + (df['transactions'] * df['kg_per_tx'])
        return df

    def miners_revenue(self):
        chart_data = self.repository.get_miners_revenue()
        return [
            {'name': i['name'], 'x': int(i['timestamp']), 'y': round(float(i['value']) / 100, 4)} for i in chart_data
        ]

    def download_miners_revenue(self):
        chart_data = self.repository.get_miners_revenue_for_download()
        send_file_func = send_file()
        return send_file_func({
            'date': 'Date',
            'transaction_fees': 'Transaction fees, %',
            'mining_rewards': 'Miner rewards, %',
        }, [
            {
                'date': i['date'],
                'transaction_fees': str(round(float(i['transaction_fees']), 2)) + '%',
                'mining_rewards': str(round(float(i['mining_rewards']), 2)) + '%',
            } for i in chart_data
        ])
