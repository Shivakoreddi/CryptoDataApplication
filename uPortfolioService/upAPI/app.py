from flask import Flask,flash,redirect,url_for,session
from flask import render_template
from flask import request
import sqlite3
import pandas as pd




app = Flask(__name__, template_folder=r'C:\Users\907545\Desktop\CryptoDataApplication\uPortfolioService\upAPI\templates')
app.secret_key = "super key"

def insUser(user, password):
    conn = sqlite3.connect(
        "C:/Users/907545/Desktop/CryptoDataApplication/uPortfolioService/uPortfolioDB/uPortfolio.db")
    conn2 = sqlite3.connect("C:/Users/907545/Desktop/CryptoDataApplication/transactionDB/tradingSchema.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users VALUES (?,?,?)", (user, password, 'null'))
    cursor2 = conn2.cursor()
    cursor2.execute("select market_id,market_name,current_price,image from market order by RANDOM() limit 5")
    markets = cursor2.fetchall()
    for market in markets:
        cursor.execute("INSERT INTO up_coins VALUES (?,?,?,?,?)", (user,market[0],market[1],market[2],market[3]))
    conn.commit()
    conn.close()
    conn2.close()


def retriveUser(user):
    conn = sqlite3.connect("C:/Users/907545/Desktop/CryptoDataApplication/uPortfolioService/uPortfolioDB/uPortfolio.db")
    cursor = conn.cursor()
    cursor.execute("select username,password from users where username='{}'".format(user))
    users = cursor.fetchall()
    conn.close()
    if len(users)>0:
        return True
    else:
        return False
def retriveCoins(username):
    conn = sqlite3.connect("C:/Users/907545/Desktop/CryptoDataApplication/uPortfolioService/uPortfolioDB/uPortfolio.db")
    cursor = conn.cursor()
    cursor.execute("select market_id,market_name,current_price from up_coins where user='{}'".format(username))
    coins = cursor.fetchall()
    return coins


class uPortfolio:
    @app.route("/home", methods=['GET'])
    def home():
        if request.method=='GET':
            user = request.args['user']
            user = session['user']
            coins = retriveCoins(user)
            ##print(coins)
            headings = ("coinID","coinName","Price($)")
            coins = tuple(coins)
            return render_template('home.html',headings = headings,coins = coins,user=user)


    @app.route('/', methods=['POST', 'GET'])
    def login():
        if request.method == 'POST':
            user = request.form['username']
            password = request.form['password']
            if retriveUser(user):
                session['user'] = user
                return redirect(url_for('home',user=user))
            else:
                insUser(user, password)
                return f"User ID Created!!"
        else:
            return render_template('index.html')

    @app.route('/logout')
    def logout():
        session.pop('username',None)
        return redirect(url_for('login'))

if __name__ == '__main__':
    app.debug=True
    app.run(debug=False, host='127.0.0.1')