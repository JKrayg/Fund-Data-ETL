# create tables
import pandas as pd
import json
from db.db_connection import connect

connection = connect()
cursor = connection.cursor()
tables = {}
table_names = {}
col_types = {}
Tables = {}

def create_tables():

    Tables['dim_funds'] = (
        "CREATE TABLE dim_funds ("
        " fund_key CHAR(20) NOT NULL," # accession_number
        " fund_name VARCHAR(128) NULL," # series_name
        " fund_id VARCHAR(20) NULL," # series_id
        " fund_lei CHAR(20) NULL," # series_lei
        " total_assets DECIMAL(20, 6) NOT NULL," # total_assets
        " total_liabilities DECIMAL(20, 6) NOT NULL," # total_liabilities
        " net_assets DECIMAL(20, 6) NOT NULL," # net_assets
        " assets_misc_security DECIMAL NOT NULL," # assets_attrbt_to_misc_security
        " assets_invested DECIMAL NOT NULL," # assets_invested
        " PRIMARY KEY (fund_key)"
        ") ENGINE=InnoDB"
    )

    Tables['dim_assets'] = (
        "CREATE TABLE dim_assets ("
        " asset_key BIGINT NOT NULL," # holding_id
        " asset_id BIGINT NOT NULL," # identifiers_id
        " asset_symbol VARCHAR(32)," # identifier_ticker
        " asset_name VARCHAR(128) NULL," # issuer_name
        " asset_category VARCHAR(9) NOT NULL," # asset_cat
        " asset_type VARCHAR(9) NOT NULL," # issuer_type
        " asset_lei CHAR(20) NULL," # issuer_lei
        " asset_cusip VARCHAR(20) NULL,"
        " PRIMARY KEY (asset_key, asset_id)"
        ") ENGINE=InnoDB"
    )

    # Tables['dim_dates'] = (
    #     "CREATE TABLE dim_dates ("
    #     " date_key BIGINT AUTO_INCREMENT NOT NULL,"
    #     " date DATE NOT NULL,"
    #     " month VARCHAR(9) NOT NULL,"
    #     " quarter TINYINT NOT NULL,"
    #     " year SMALLINT NOT NULL,"
    #     " weekday VARCHAR(9) NOT NULL,"
    #     " PRIMARY KEY (date_key)"
    #     ") ENGINE=InnoDB"
    # )

    Tables['fact_holdings'] = (
        "CREATE TABLE fact_holdings ("
        " holding_key BIGINT AUTO_INCREMENT NOT NULL," 
        " fund_key CHAR(20) NOT NULL," # accession_number
        " asset_key BIGINT NOT NULL," # holding_id
        " asset_id BIGINT NOT NULL," # identifiers_id
        " num_shares DECIMAL NOT NULL," # balance (calculate by unit type)
        " market_value DECIMAL NOT NULL," # calculate by unit type
        " weight DECIMAL NOT NULL," # percentage
        " PRIMARY KEY (holding_key),"
        " FOREIGN KEY (fund_key) REFERENCES dim_funds(fund_key),"
        " FOREIGN KEY (asset_key, asset_id) REFERENCES dim_assets(asset_key, asset_id)"
        # " FOREIGN KEY (date_key) REFERENCES dim_dates(date_key)"
        ") ENGINE=InnoDB"
    )

    for t in Tables:
        cursor.execute(Tables[t])
    connection.close()

def tables_from_metadata(tbl_names, metadata):
    # urls = {'IDENTIFIERS.tsv':'dim_assets',
    #         'FUND_REPORTED_INFO.tsv':'dim_funds',
    #         'FUND_REPORTED_HOLDING.tsv':'fact_holdings'}
    table_names = tbl_names
    schemas = {}

    # metadata = metadata[::-1]

    for i in metadata:
        ct = {}
        url = i['url']
        columns = i['tableSchema']['columns']
        c = ' '
        # build schema
        for j in columns:
            if j['datatype']['base'] == 'string':
                ct[j['name'].lower()] = 'String'
                j['datatype']['base'] = 'VARCHAR'
                q = (j['name'].lower() + ' ' + 
                    j['datatype']['base'] + "(" +
                    str(j['datatype']['maxLength']) + ')')
            else:
                j['datatype']['base'] = 'NUMERIC'
                ct[j['name'].lower()] = 'float'
                q = j['name'].lower() + ' ' + j['datatype']['base'] + "(" + \
                    str(j['datatype']['dataPrecision']) + ',' + \
                    str(j['datatype']['dataScale']) + ')'
            
            if 'required':
                q += ' NOT NULL'
                q += ', '
                c += q

            # ct[j['name']] = j['datatype']['base']
        
        col_types[table_names[url]] = ct

        c += 'PRIMARY KEY (' + ', '.join(i['tableSchema']['PrimaryKey']) + ')'
        if url == 'FUND_REPORTED_HOLDING.tsv':
            c += (", FOREIGN KEY (ACCESSION_NUMBER)"
                  " REFERENCES dim_funds(ACCESSION_NUMBER)"
                  ", FOREIGN KEY (HOLDING_ID, IDENTIFIERS_ID)"
                  " REFERENCES dim_assets(HOLDING_ID, IDENTIFIERS_ID)")
        
        schemas[table_names[url]] = c

    # print(json.dumps(tables, indent=4))
    for u in table_names.keys():
        tables[u] = ("CREATE TABLE " + table_names[u] + " (" + schemas[table_names[u]] + ") ENGINE=InnoDB")

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
    
def get_table_names():
    return table_names
    
def get_col_types():
    return col_types

