# standardize Form N-PORT quarterly dataset
import numpy as np
import pandas as pd
from db.schema import get_col_types

column_dict = {'series_id': 'fund_id',
               'series_lei': 'fund_lei',
               'series_name': 'fund_name',
               'holding_id': 'asset_key',
               'identifiers_id': 'asset_id',
               'issuer_name': 'asset_name',
               'issuer_lei': 'asset_lei',
               'issuer_type': 'asset_type',
               'identifier_ticker': 'asset_symbol',
               'identifier_isin': 'asset_isin',
               'assets_attrbt_to_misc_security': 'assets_misc_security',
               'issuer_cusip': 'asset_cusip',
               'asset_cat': 'asset_category',
               'accession_number': 'fund_key',
               'balance': 'num_shares',
               'currency_value': 'market_value',
               'percentage': 'weight'}


def clean(df):
    df = df.replace({np.nan: None})
    df.columns = [col.lower() for col in df.columns]
    for c in range(len(df.columns)):
        if df.columns[c] in column_dict.keys():
            df = df.rename(columns = {
                df.columns[c]: column_dict[df.columns[c]]
            })
    return df


# def clean(df, column_types):
#     df = df.replace({np.nan: None})
#     df.columns = [col.lower() for col in df.columns]

#     for col in df.columns:
#         if column_types[col] == 'float' or column_types[col] == 'int':
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#         # else:
#         #     df[col] = df[col].astype(str)

#     if 'exchange_rate' in df.columns:
#         df.loc[(df['exchange_rate'].isna()), 'exchange_rate'] = 1
#         df.loc[(df['exchange_rate'] == 0), 'exchange_rate'] = 1

#         df['currency_value_usd'] = df['currency_value'] / df['exchange_rate']
#         # df.loc[(df['currency_code'].isna()), 'exchange_rate'] = 0

#     return df

# def check_types(df):
#     col_types = get_col_types()
#     print(col_types)
#     for col in df.columns:
#         if col_types[col] == 'float':
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#         else:
#             df[col] = df[col].astype(str)

#     return df

def fix_exchange_rate(df):
    df.loc[(df['exchange_rate'].isna()), 'exchange_rate'] = 1
    df.loc[(df['exchange_rate'] == 0), 'exchange_rate'] = 1
    df.loc[(df['currency_code'].isna()), 'exchange_rate'] = 0
    return df

def currency_to_usd(df):
    df['currency_value_usd'] = df['currency_value'] / df['exchange_rate']
    return df

# PA - principal
# NS - normal stock
# NV - net capital
# OU - open units
 