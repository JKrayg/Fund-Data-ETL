import yfinance as yf
import pandas as pd
import uuid
from db.db_connection import connect
from db.create_tables import create_tbls, drop_tbls


drop_tbls()
create_tbls()

rsp_holdings = pd.read_csv(
    'data\invesco_s&p_500_equal_weight_etf-monthly_holdings.csv',
    index_col=None, na_values='NA')[:-1]


# create pandas df of security data
rsp_holdings = rsp_holdings[:-4]
rsp_sec= rsp_holdings[['Company', 'Ticker']]
rsp_sec = rsp_sec.rename(columns={'Ticker': 'symbol', 'Company': 'name'})
rsp_sec.insert(0, 'security_key', [str(uuid.uuid4()) for _ in range(rsp_sec.shape[0])])

# connect to sql
connection = connect()
cursor = connection.cursor()

# insert security data into securities table
query = "INSERT INTO securities (security_key, name, symbol) VALUES (%s, %s, %s)"
cursor.executemany(query, rsp_sec.values.tolist())
connection.commit()





# tickers = ' '.join(rsp_holdings.['Ticker'].to_list())
# prices = yf.download(tickers, period='1d')
# closing_prices = prices['Close']




