import yfinance as yf
import pandas as pd
import numpy as np
import uuid
import json
from db.db_connection import connect
from db.schema import create_tables, drop_tables
from etl.clean import clean, fix_echange_rate, currency_to_usd


# drop_tables()
# create_tables()


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
    

# connect to sql
# connection = connect()
# cursor = connection.cursor()

reported_holdings = tsv_to_df('data/form_nport/FUND_REPORTED_HOLDING.tsv', 300000)
reported_holdings = clean(reported_holdings)

# print(json.dumps(reported_holdings.iloc[0:10].to_dict(), indent=4))
reported_holdings = reported_holdings.drop(['derivative_cat', 'fair_value_level',
                                            'is_restricted_security', 'other_issuer',
                                            'other_asset', 'payoff_profile',
                                            'issuer_title'], axis=1)

print()


assets = tsv_to_df('data/form_nport/IDENTIFIERS.tsv', 0)
assets = clean(assets)

# grab assets present in reported holdings batch
assets = assets[assets['asset_key'].isin(reported_holdings['asset_key'])]
assets = assets[['asset_key', 'asset_id', 'asset_symbol']]

assets = assets.merge(reported_holdings[['asset_key', 'asset_name',
                                         'asset_category', 'asset_type',
                                         'asset_lei', 'asset_cusip'
                                        ]], on='asset_key', how='left')

# insert assets data into assets table
query = ("INSERT INTO dim_assets "
        "(asset_key, asset_id, asset_symbol, "
        "asset_name, asset_category, asset_type, "
        "asset_lei, asset_cusip) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
# cursor.executemany(query, assets.values.tolist())
# connection.commit()


funds = tsv_to_df('data/form_nport/FUND_REPORTED_INFO.tsv', 0)
funds = clean(funds)
funds = funds[funds['fund_key'].isin(reported_holdings['fund_key'])]
funds = funds[['fund_key', 'fund_name', 'fund_id',
                       'fund_lei', 'total_assets', 'total_liabilities',
                       'net_assets', 'assets_misc_security',
                       'assets_invested']]

# insert funds data into funds table
# funds = funds.drop('accession_number', axis=1)
query = ("INSERT INTO dim_funds "
        "(fund_key, fund_name, fund_id, fund_lei, total_assets,"
        " total_liabilities, net_assets, assets_misc_security, assets_invested)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
# cursor.executemany(query, funds.values.tolist())
# connection.commit()

reported_holdings.insert(2, 'asset_id', assets['asset_id'])
reported_holdings = reported_holdings.drop(['asset_name', 'asset_lei',
                                            'asset_cusip', 'asset_category',
                                            'asset_type'], axis=1)

reported_holdings = fix_echange_rate(reported_holdings)
reported_holdings = currency_to_usd(reported_holdings)
print(reported_holdings.iloc[0:20])





# connection.close()


# invesco_sp_500 = yf.Ticker('SPHQ')
# print(json.dumps(invesco_sp_500.info, indent=4))
# sphq_name = invesco_sp_500.info.get('shortName')
# closing_prices = invesco_sp_500['Close']




