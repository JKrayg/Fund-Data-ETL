from db.db_connection import connect

connection = connect()
cursor = connection.cursor()
Tables = {}

def create_tbls():

    Tables['funds'] = (
        "CREATE TABLE funds ("
        " funds_key CHAR(36) NOT NULL,"
        " name VARCHAR(36) NOT NULL,"
        " symbol VARCHAR(6) NOT NULL,"
        " PRIMARY KEY (funds_key)"
        ") ENGINE=InnoDB"
    )

    Tables['securities'] = (
        "CREATE TABLE securities ("
        " securities_key CHAR(36) NOT NULL,"
        " name VARCHAR(36) NOT NULL,"
        " symbol VARCHAR(6) NOT NULL,"
        " PRIMARY KEY (securities_key)"
        ") ENGINE=InnoDB"
    )

    Tables['dates'] = (
        "CREATE TABLE dates ("
        " dates_key CHAR(36) NOT NULL,"
        " date DATE NOT NULL,"
        " month VARCHAR(9) NOT NULL,"
        " quarter TINYINT NOT NULL,"
        " year SMALLINT NOT NULL,"
        " weekday VARCHAR(9) NOT NULL,"
        " PRIMARY KEY (dates_key)"
        ") ENGINE=InnoDB"
    )

    Tables['holdings'] = (
        "CREATE TABLE holdings ("
        " funds_key CHAR(36) NOT NULL,"
        " securities_key CHAR(36) NOT NULL,"
        " dates_key CHAR(36) NOT NULL,"
        " name VARCHAR(36) NOT NULL,"
        " symbol VARCHAR(6) NOT NULL,"
        " FOREIGN KEY (funds_key) REFERENCES funds(funds_key),"
        " FOREIGN KEY (securities_key) REFERENCES securities(securities_key),"
        " FOREIGN KEY (dates_key) REFERENCES dates(dates_key)"
        ") ENGINE=InnoDB"
    )

    for t in Tables:
        cursor.execute(Tables[t])
    connection.close()

def drop_tbls():
    cursor.execute('DROP TABLES IF EXISTS funds, holdings, dates, securities;')