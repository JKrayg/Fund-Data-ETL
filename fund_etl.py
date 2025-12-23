import yfinance as yf
import pandas as pd
import json

qtum_holdings = pd.read_excel('qtum-12-23-2025.xlsx', 'First Sheet', index_col=None, na_values='NA')[3:-1]
qtum_holdings = qtum_holdings[~qtum_holdings['Identifier'].str.contains(" ")]
previous_day = yf.download(qtum_holdings['Identifier'].to_list(), period='1d')
previous_closes = previous_day.get('Close')

