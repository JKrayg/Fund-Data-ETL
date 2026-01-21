# standardize Form N-PORT quarterly dataset
import numpy as np

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
               'accession_number': 'fund_key'}


def clean(df):
    df = df.replace({np.nan: None})
    df.columns = [col.lower() for col in df.columns]
    for c in range(len(df.columns)):
        if df.columns[c] in column_dict.keys():
            df = df.rename(columns = {
                df.columns[c]: column_dict[df.columns[c]]
            })
    return df


def fix_exchange_rate(df):
    df.loc[(df['exchange_rate'].isna()), 'exchange_rate'] = 1
    df.loc[(df['exchange_rate'] == 0), 'exchange_rate'] = 1
    # df.loc[(df['currency_code'].isna()), 'exchange_rate'] = 0
    return df

def currency_to_usd(df):
    df['currency_value_usd'] = df['currency_value'] / df['exchange_rate']
    return df

# PA - principal
# NS - normal stock
# NV - net capital
# OU - open units
 