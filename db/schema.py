# create tables
import pandas as pd
import json
from db.db_connection import connect

connection = connect()
cursor = connection.cursor()
tables = {}

def tables_from_metadata(metadata):
    urls = {'IDENTIFIERS.tsv':'dim_assets',
            'FUND_REPORTED_INFO.tsv':'dim_funds',
            'FUND_REPORTED_HOLDING.tsv':'fact_holdings'}
    
    schemas = {}

    # metadata = metadata[::-1]

    for i in metadata:
        url = i['url']
        columns = i['tableSchema']['columns']
        c = ' '
        # build schema
        for j in columns:
            if j['datatype']['base'] == 'string':
                j['datatype']['base'] = 'VARCHAR'
                q = j['name'].lower() + ' ' + j['datatype']['base'] + "(" + str(j['datatype']['maxLength']) + ')'
                if 'required':
                    q += ' NOT NULL'
                q += ', '
                c += q
            else:
                j['datatype']['base'] = 'NUMERIC'
                q = j['name'].lower() + ' ' + j['datatype']['base'] + "(" + \
                    str(j['datatype']['dataPrecision']) + ',' + \
                    str(j['datatype']['dataScale']) + ')'
                if 'required':
                    q += ' NOT NULL'
                q += ', '
                c += q

        c += 'PRIMARY KEY (' + ', '.join(i['tableSchema']['PrimaryKey']) + ')'
        if url == 'FUND_REPORTED_HOLDING.tsv':
            c += (", FOREIGN KEY (ACCESSION_NUMBER) REFERENCES dim_funds(ACCESSION_NUMBER)"
                  ", FOREIGN KEY (HOLDING_ID, IDENTIFIERS_ID) REFERENCES dim_assets(HOLDING_ID, IDENTIFIERS_ID)")
        
        schemas[urls[url]] = c

    # print(json.dumps(tables, indent=4))
    for u in urls.keys():
        tables[u] = ("CREATE TABLE " + urls[u] + " (" + schemas[urls[u]] + ") ENGINE=InnoDB")

    for t in tables:
        cursor.execute(tables[t])
    connection.close()

    # return tables

def insert(table_name, columns, values):
    in_ = ("INSERT INTO " + table_name +
        " (" + ', '.join(columns) + ") VALUES (" + ', '.join(['%s' for _ in range(len(columns))]) + ")")
    
    # print(values)
    cursor.executemany(in_, values)
    connection.commit()

def drop_tables():
    cursor.execute("DROP TABLES IF EXISTS "
                   "dim_funds, fact_holdings, dim_dates, "
                   "dim_assets, fact_transactions, dim_accounts;")