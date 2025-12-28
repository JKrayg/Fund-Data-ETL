from db.db_connection import connect

connection = connect()
cursor = connection.cursor()
Tables = {}

def create_tbls():

    Tables['funds'] = (
        "CREATE TABLE funds ("
        " fund_key CHAR(36) NOT NULL,"
        " name VARCHAR(64) NOT NULL,"
        " symbol VARCHAR(6) NOT NULL,"
        " PRIMARY KEY (fund_key)"
        ") ENGINE=InnoDB"
    )

    Tables['securities'] = (
        "CREATE TABLE securities ("
        " security_key CHAR(36) NOT NULL,"
        " name VARCHAR(64) NOT NULL,"
        " symbol VARCHAR(6) NOT NULL,"
        " PRIMARY KEY (security_key)"
        ") ENGINE=InnoDB"
    )

    Tables['dates'] = (
        "CREATE TABLE dates ("
        " date_key CHAR(36) NOT NULL,"
        " date DATE NOT NULL,"
        " month VARCHAR(9) NOT NULL,"
        " quarter TINYINT NOT NULL,"
        " year SMALLINT NOT NULL,"
        " weekday VARCHAR(9) NOT NULL,"
        " PRIMARY KEY (date_key)"
        ") ENGINE=InnoDB"
    )

    Tables['accounts'] = (
        "CREATE TABLE accounts ("
        " account_key CHAR(36) NOT NULL,"
        " holder VARCHAR(64) NOT NULL,"
        " type VARCHAR(16) NOT NULL,"
        " PRIMARY KEY (account_key)"
        ") ENGINE=InnoDB"
    )

    Tables['holdings'] = (
        "CREATE TABLE holdings ("
        " fund_key CHAR(36) NOT NULL,"
        " security_key CHAR(36) NOT NULL,"
        " date_key CHAR(36) NOT NULL,"
        " shares DECIMAL NOT NULL,"
        " market_value DECIMAL NOT NULL,"
        " weight DECIMAL NOT NULL,"
        " FOREIGN KEY (fund_key) REFERENCES funds(fund_key),"
        " FOREIGN KEY (security_key) REFERENCES securities(security_key),"
        " FOREIGN KEY (date_key) REFERENCES dates(date_key)"
        ") ENGINE=InnoDB"
    )

    Tables['transactions'] = (
        "CREATE TABLE transactions ("
        " fund_key CHAR(36) NOT NULL,"
        " account_key CHAR(36) NOT NULL,"
        " date_key CHAR(36) NOT NULL,"
        " type VARCHAR(12) NOT NULL,"
        " amount DECIMAL NOT NULL,"
        " units DECIMAL NOT NULL,"
        " FOREIGN KEY (fund_key) REFERENCES funds(fund_key),"
        " FOREIGN KEY (account_key) REFERENCES securities(security_key),"
        " FOREIGN KEY (date_key) REFERENCES dates(date_key)"
        ") ENGINE=InnoDB"
    )

    for t in Tables:
        cursor.execute(Tables[t])
    connection.close()

def drop_tbls():
    cursor.execute('DROP TABLES IF EXISTS funds, holdings, dates, securities, transactions, accounts;')