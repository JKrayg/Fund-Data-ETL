# create tables

from db.db_connection import connect

connection = connect()
cursor = connection.cursor()
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

def drop_tables():
    cursor.execute("DROP TABLES IF EXISTS "
                   "dim_funds, fact_holdings, dim_dates, "
                   "dim_assets, fact_transactions, dim_accounts;")