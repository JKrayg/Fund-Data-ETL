import yfinance as yf
import pandas as pd
import numpy as np
import uuid
import json
from db.db_connection import connect
from db.schema import tables_from_metadata, drop_tables, insert, get_col_types, create_tables
from etl.clean import clean, fix_exchange_rate, currency_to_usd

connection = connect()
cursor = connection.cursor()

drop_tables()
create_tables()


def tsv_to_df(filename, limit=0):
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

table_name_dict = {'IDENTIFIERS.tsv':'dim_assets',
                   'FUND_REPORTED_INFO.tsv':'dim_funds',
                   'FUND_REPORTED_HOLDING.tsv':'fact_holdings'}
reported_holdings = tsv_to_df('data/form_nport/FUND_REPORTED_HOLDING.tsv', 100000)
reported_holdings = clean(reported_holdings)

# print(json.dumps(reported_holdings.iloc[0:10].to_dict(), indent=4))
reported_holdings = reported_holdings.drop(['derivative_cat', 'fair_value_level',
                                            'is_restricted_security', 'other_issuer',
                                            'other_asset', 'payoff_profile',
                                            'issuer_title'], axis=1)

# print()
# 

assets = tsv_to_df('data/form_nport/IDENTIFIERS.tsv', 0)
assets = clean(assets)

# grab assets present in reported holdings batch
# assets = assets[assets['asset_key'].isin(reported_holdings['asset_key'])]
# assets = assets.merge(reported_holdings[['asset_key']], on='asset_key', how='inner')
assets = assets[['asset_key', 'asset_id', 'asset_symbol']]

assets = assets.merge(reported_holdings[['asset_key', 'asset_name',
                                         'asset_category', 'asset_type',
                                         'asset_lei', 'asset_cusip'
                                        ]], on='asset_key', how='inner')

# print(assets.columns)
reported_holdings = reported_holdings.merge(assets[['asset_key', 'asset_id']], on='asset_key', how='inner')

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

# print(funds.columns)

# insert funds data into funds table
# funds = funds.drop('accession_number', axis=1)
query = ("INSERT INTO dim_funds "
        "(fund_key, fund_name, fund_id, fund_lei, total_assets,"
        " total_liabilities, net_assets, assets_misc_security, assets_invested)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
# cursor.executemany(query, funds.values.tolist())
# connection.commit()

# reported_holdings.insert(2, 'asset_id', assets['asset_id'])
reported_holdings = reported_holdings.drop(['asset_name', 'asset_lei',
                                            'asset_cusip', 'asset_category',
                                            'asset_type', 'unit', 'other_unit_desc',
                                            'currency_code', 'exchange_rate',
                                            'investment_country'], axis=1)

# reported_holdings = reported_holdings.merge(
#     assets[['asset_key', 'asset_id']], on='asset_key', how='left')

# print(assets.iloc[0:1])
# print(reported_holdings[reported_holdings['asset_key'] == 152494455]['asset_id'])
# print(reported_holdings.columns)

# print(reported_holdings.columns)

query = ("INSERT INTO fact_holdings "
        "(fund_key, asset_key, asset_id, num_shares, market_value, weight)"
        " VALUES (%s, %s, %s, %s, %s, %s)")
# cursor.executemany(query, reported_holdings.values.tolist())
# connection.commit()


fund_asset = pd.merge(reported_holdings, funds, how='left', on='fund_key')

asset_equity = assets[assets['asset_category'] == "EC"]
# print(json.dumps(asset_equity.iloc[0:10].to_dict(orient='records'), indent=4))

mv_1m = reported_holdings[
    reported_holdings['market_value'] > 1000000
    ].sort_values(by='market_value', ascending=True)
# print(json.dumps(mv_1m.iloc[0:10].to_dict(orient='records'), indent=4))


fund_1b = funds[funds['total_assets'] > 1000000000
                ].sort_values(by='total_assets', ascending=True)
# print(json.dumps(fund_1b.iloc[0:10].to_dict(orient='records'), indent=4))

start_a = assets[assets['asset_cusip'].str.startswith('A', na=False)]
# print(json.dumps(start_a.iloc[0:10].to_dict(orient='records'), indent=4))

asset_fund_rep = pd.merge(
    reported_holdings,
    assets, on='asset_id'
)
asset_fund_rep = pd.merge(asset_fund_rep, funds, on='fund_key')
# print(json.dumps(asset_fund_rep.iloc[0:10].to_dict(orient='records'), indent=4))


asset_info = pd.merge(
    reported_holdings[['fund_key', 'asset_id', 'num_shares', 'market_value']],
    assets[['asset_id', 'asset_symbol']], on='asset_id'
)
asset_info = pd.merge(asset_info, funds[['fund_key', 'fund_name']], on='fund_key')
asset_info = asset_info.replace({np.nan: None})
# print(json.dumps(
#     asset_info[asset_info['asset_symbol'].notna()]
#     .iloc[0:10].to_dict(orient='records'), indent=4))

eq_2 = pd.merge(
    reported_holdings[['fund_key', 'asset_id', 'market_value']],
    assets[assets['asset_category'] == 'EC'][['asset_id', 'asset_symbol', 'asset_name', 'asset_category']], on='asset_id')
eq_2 = pd.merge(eq_2, funds[['fund_key', 'fund_name']], on='fund_key')
# print(json.dumps(eq_2.iloc[0:10].to_dict(orient='records'), indent=4))


dbl = asset_fund_rep[['fund_key', 'asset_symbol', 'market_value']].copy()
dbl['double_value'] = dbl['market_value'] * 2
# print(json.dumps(dbl.iloc[0:10].to_dict(orient='records'), indent=4))

debt_rat = asset_fund_rep[['fund_name', 'total_assets', 'total_liabilities']].copy()
debt_rat['debt_ratio'] = debt_rat['total_liabilities'] / debt_rat['total_assets']
# print(json.dumps(debt_rat.sample(20).iloc[0:10].to_dict(orient='records'), indent=4))

avg_asset = assets.groupby('asset_category').agg(
    num_assets = ('asset_id', 'count'),
    avg_symbol_length = ('asset_symbol', lambda x: x.str.len().mean())
).reset_index()
# print(json.dumps(avg_asset.iloc[0:10].to_dict(orient='records'), indent=4))


avg_mv = asset_fund_rep.groupby('fund_name').agg(
    total_holdings = ('total_assets', 'count'),
    avg_market_value = ('market_value', 'mean')
).reset_index()
# print(json.dumps(avg_mv.iloc[0:10].to_dict(orient='records'), indent=4))

tot_funds = asset_fund_rep.groupby('asset_symbol').agg(
    total_num_funds = ('fund_key', 'count')
).reset_index()
# print(json.dumps(tot_funds.iloc[0:10].to_dict(orient='records'), indent=4))

tot_dif_assets_held = asset_fund_rep.groupby('fund_key').agg(
    tot_dif_assets = ('asset_id', 'nunique')
).reset_index()
# print(json.dumps(tot_dif_assets_held.iloc[0:10].to_dict(orient='records'), indent=4))

asset_count = asset_fund_rep.groupby('fund_key').agg(
    tot_assets = ('asset_id', 'nunique')
).reset_index()

mt_10 = asset_count[asset_count['tot_assets'] > 10]
# print(json.dumps(mt_10.iloc[0:10].to_dict(orient='records'), indent=4))

fund_count = asset_fund_rep.groupby('asset_symbol').agg(
    tot_funds = ('fund_key', 'nunique')
).reset_index()

mt_5 = fund_count[fund_count['tot_funds'] > 5]
# print(json.dumps(mt_5.iloc[0:10].to_dict(orient='records'), indent=4))

l_mv = asset_fund_rep[['fund_key', 'asset_symbol', 'market_value']].sort_values(by='market_value', ascending=False)
print(json.dumps(l_mv.iloc[0:10].to_dict(orient='records'), indent=4))

# hta = asset_fund_rep[['asset_symbol', 'asset_name'] if asset_fund_rep['market_value'] > asset_fund_rep['market_value'].mean() else None]



















# print(fund_asset.groupby('fund_key').filter(lambda x: x == '0001752724-25-203557').to_dict())

























# reported_holdings = fix_exchange_rate(reported_holdings)
# reported_holdings = currency_to_usd(reported_holdings)
# print(reported_holdings.iloc[0:20])

# tables_from_metadata(table_name_dict, metadata)

# col_types = get_col_types()

# # print(json.dumps(col_types, indent=4))

# reported_holdings = tsv_to_df('data/form_nport/FUND_REPORTED_HOLDING.tsv', 300000)
# reported_holdings.columns = [c.lower() for c in reported_holdings.columns]
# reported_holdings = clean(reported_holdings,
#                           col_types[table_name_dict['FUND_REPORTED_HOLDING.tsv']])
# # print('reported holdings')
# # for c in reported_holdings.columns:
# #     print(reported_holdings[c].map(type).unique())

# assets = tsv_to_df('data/form_nport/IDENTIFIERS.tsv')
# assets.columns = [c.lower() for c in assets.columns]
# assets = assets[assets['holding_id'].isin(reported_holdings['holding_id'])]
# assets = clean(assets, col_types[table_name_dict['IDENTIFIERS.tsv']])

# # print('assets')
# print(assets.columns)
# for c in assets.columns:
#     print(assets[c].map(type).unique())

# funds = tsv_to_df('data/form_nport/FUND_REPORTED_INFO.tsv')
# funds.columns = [c.lower() for c in funds.columns]
# funds = funds[funds['accession_number'].isin(reported_holdings['accession_number'])]
# funds = clean(funds, col_types[table_name_dict['FUND_REPORTED_INFO.tsv']])

# print('funds')
# for c in funds.columns:
#     print(funds[c].map(type).unique())


# print(type(i) for i in assets.colums)

# insert('dim_assets', assets.columns, assets.values.tolist())
# insert('dim_funds', funds.columns, funds.values.tolist())
# insert('fact_holdings', reported_holdings.columns, reported_holdings.values.tolist())




