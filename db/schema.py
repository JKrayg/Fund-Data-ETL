# create tables
import pandas as pd
import json
from db.db_connection import connect

connection = connect()
cursor = connection.cursor()
Tables = {}

def tables_from_metadata(metadata):
    urls = {'FUND_REPORTED_HOLDING.tsv':'fact_holdings',
            'IDENTIFIERS.tsv':'dim_assets',
            'FUND_REPORTED_INFO.tsv':'dim_funds'}
    
    tablez = {}

    for i in metadata:
        url = i['url']
        columns = i['tableSchema']['columns']
        c = ' '
        for j in columns:
            if j['datatype']['base'] == 'string':
                j['datatype']['base'] = 'VARCHAR'
                q = j['name'] + ' ' + j['datatype']['base'] + "(" + str(j['datatype']['maxLength']) + ')'
                if 'required' in j:
                    q += ' NOT NULL'
                q += ', '
                c += q
            else:
                j['datatype']['base'] = 'NUMERIC'
                q = j['name'] + ' ' + j['datatype']['base'] + "(" + \
                    str(j['datatype']['dataPrecision']) + ',' + \
                    str(j['datatype']['dataScale']) + ')'
                if 'required' in j:
                    q += ' NOT NULL'
                q += ', '
                c += q

        c += 'PRIMARY KEY (' + ', '.join(i['tableSchema']['PrimaryKey']) + ')'
        
        tablez[urls[url]] = c

    for u in urls.keys():
        Tables[u] = ("CREATE TABLE " + urls[u] + " (" + tablez[urls[u]] + ') ENGINE=InnoDB')

    for t in Tables:
        cursor.execute(Tables[t])
    connection.close()


def drop_tables():
    cursor.execute("DROP TABLES IF EXISTS "
                   "dim_funds, fact_holdings, dim_dates, "
                   "dim_assets, fact_transactions, dim_accounts;")