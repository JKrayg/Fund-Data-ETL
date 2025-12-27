import yfinance as yf
import pandas as pd
import json
import uuid
from db.db_connection import connect
from db.create_tables import create_tbls, drop_tbls

# Total assets = market value of securities
#   + cash/equivalents + receivables + accrued income

# Total liabilities = accrued expenses
#   + payables + management fees payable
#   + collateral payable + redemptions payable


# drop_tbls()
# create_tbls()

rsp_holdings = pd.read_csv(
    'data\invesco_s&p_500_equal_weight_etf-monthly_holdings.csv',
    index_col=None, na_values='NA')[:-1]

cash_non_equity = rsp_holdings.tail(4)

cash_collat = cash_non_equity.head()['Market value'].iloc[0]
cash_equivalents = cash_non_equity.loc[
    cash_non_equity['Ticker'] == 'USD', 'Market value'
    ].iloc[0]
uninvestible_cash = cash_non_equity.loc[
    cash_non_equity['Class of shares'] == 'Uninvestible Cash', 'Market value'
    ].iloc[0]

rsp_holdings = rsp_holdings[:-4]

samp = rsp_holdings.sample(10)

print(samp)

# tickers = ' '.join(rsp_holdings.['Ticker'].to_list())
# tickers = ' '.join(samp['Ticker'].to_list())
# prices = yf.download(tickers, period='1d')

# closing_prices = prices['Close']




