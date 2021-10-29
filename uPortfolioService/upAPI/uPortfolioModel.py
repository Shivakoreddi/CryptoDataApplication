import sqlite3
from sqlite3 import Error

def create_table(conn,table_query):
    try:
        cursor = conn.cursor()
        cursor.execute(table_query)
    except Error as e:
        print(e)
    #cursor.execute("insert into up_coins values(?,?,?)",('btc','bitcoin',63000))
    #cursor.execute("insert into up_coins values(?,?,?)", ('eth', 'ethereum', 4000))
    #cursor.execute("insert into up_coins values(?,?,?)", ('xrp', 'xrp', 1.00))
    cursor.execute("insert into up_coins values(?,?,?,?,?)", ('sk','dodge', 'dodgeCoin', 0.24,'htps'))
    ##cursor.execute("drop table up_coins")
    rows = cursor.execute("select * from up_coins")

    #for row in rows:
    #    print(row)
    conn.commit()


def main():
    db = r"C:/Users/907545/Desktop/CryptoDataApplication/uPortfolioService/uPortfolioDB/uPortfolio.db"

    users = """CREATE TABLE IF NOT EXISTS users(
    username text primary key,
    password text,
    portfolioID text)"""

    up_coins= """CREATE TABLE IF NOT EXISTS up_coins(user,market_id text,market_name text,current_price integer,image)"""

    ##create connection
    conn = sqlite3.connect(db)
    if conn is not None:
        create_table(conn,users)
        create_table(conn,up_coins)
    else:
        print("error,can't create tables")


if __name__=="__main__":
    main()

