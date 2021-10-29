import sqlite3

def exchange_type_load(conn):
    cursor = conn.cursor()
    ##execute query
    cursor.execute("insert or replace into exchange_type values(1,'spot','direct coins/tokens currency etc.')")
    cursor.execute("insert or replace into exchange_type values(2,'derivatives','futures/options/etf and index form of assets')")
    cursor.execute("select * from exchange_type")
    rows = cursor.fetchall()
    ##for row in rows:
        ##print(row)
    conn.commit()
    return


def main():

    ##create db connection
    conn = sqlite3.connect('/CryptoDataApplication/transactionDB/tradingSchema.db')
    ##generate load queries
    exchange_type_load(conn)


if __name__=="__main__":
    main()