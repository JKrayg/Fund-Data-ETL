import yfinance as yf
import pandas as pd
import numpy as np
import uuid
import json
from db.db_connection import connect
from db.schema import tables_from_metadata, drop_tables, insert
from etl.clean import clean, fix_exchange_rate, currency_to_usd


drop_tables()


def tsv_to_df(filename, limit):
    if limit == 0:
        return pd.read_csv(
            filename, sep='\t',
            low_memory=False, index_col=None, na_values='NULL')
    else:
        return pd.read_csv(
            filename, sep='\t',
            low_memory=False, index_col=None, na_values='NULL',
            nrows=limit)
    

with open('data/form_nport/nport_metadata.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

file_names = ['FUND_REPORTED_HOLDING.tsv',
              'IDENTIFIERS.tsv',
              'FUND_REPORTED_INFO.tsv']
tables = data['tables']

metadata = []
for i in tables:
    if i['url'] in file_names:
        metadata.append(i)

tables_from_metadata(metadata)

reported_holdings = tsv_to_df('data/form_nport/FUND_REPORTED_HOLDING.tsv', 300000)
reported_holdings.columns = [c.lower() for c in reported_holdings.columns]


assets = tsv_to_df('data/form_nport/IDENTIFIERS.tsv', 0)
assets.columns = [c.lower() for c in assets.columns]
assets = assets[assets['holding_id'].isin(reported_holdings['holding_id'])]
insert('dim_assets', assets.columns, assets.values.tolist())


funds = tsv_to_df('data/form_nport/FUND_REPORTED_INFO.tsv', 0)
funds.columns = [c.lower() for c in funds.columns]
funds = funds[funds['accession_number'].isin(reported_holdings['accession_number'])]




