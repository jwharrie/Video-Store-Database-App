from flask import Flask, g, request, jsonify
import pyodbc
from connect_db import connect_db
import sys
import time, datetime


app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'azure_db'):
        g.azure_db = connect_db()
        g.azure_db.autocommit = True
        g.azure_db.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_SERIALIZABLE)
    return g.azure_db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'azure_db'):
        g.azure_db.close()



@app.route('/login')
def login():
    username = request.args.get('username', "")
    password = request.args.get('password', "")
    cid = -1
    #print (username, password)
    conn = get_db()
    #print (conn)
    cursor = conn.execute("SELECT * FROM Customer WHERE username = ? AND password = ?", (username, password))
    records = cursor.fetchall()
    #print records
    if len(records) != 0:
        cid = records[0][0]
    response = {'cid': cid}
    return jsonify(response)




@app.route('/getRenterID')
def getRenterID():
    """
       This HTTP method takes mid as input, and
       returns cid which represents the customer who is renting the movie.
       If this movie is not being rented by anyone, return cid = -1
    """
    mid = int(request.args.get('mid', -1))

    # WRITE YOUR CODE HERE

    cid = -1
    conn = get_db()
    cursor = conn.execute("SELECT * FROM Rental WHERE mid = ?", mid)
    records = cursor.fetchall()
    if len(records) != 0:
        for r in records:
            if r[3] == "open":
                cid = r[0]
                break

    response = {'cid': cid}
    return jsonify(response)



@app.route('/getRemainingRentals')
def getRemainingRentals():
    """
        This HTTP method takes cid as input, and returns n which represents
        how many more movies that cid can rent.

        n = 0 means the customer has reached its maximum number of rentals.
    """
    cid = int(request.args.get('cid', -1))

    conn = get_db()
    # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    # WRITE YOUR CODE HERE

    cursor = conn.execute("SELECT * FROM Customer WHERE cid = ?", cid)
    records = cursor.fetchall()
    pid = records[0][1]
    cursor = conn.execute("SELECT * FROM RentalPlan WHERE pid = ?", pid)
    records = cursor.fetchall()
    n = records[0][3]
    cursor = conn.execute("SELECT * FROM Rental WHERE cid = ?", cid)
    records = cursor.fetchall()
    if len(records) != 0:
        for r in records:
            if r[3] == "open":
                n -= 1
    
    conn.autocommit = True


    response = {"remain": n}
    return jsonify(response)





def currentTime():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


@app.route('/rent')
def rent():
    """
        This HTTP method takes cid and mid as input, and returns either "success" or "fail".

        It returns "fail" if C1, C2, or both are violated:
            C1. at any time a movie can be rented to at most one customer.
            C2. at any time a customer can have at most as many movies rented as his/her plan allows.
        Otherwise, it returns "success" and also updates the database accordingly.
    """
    cid = int(request.args.get('cid', -1))
    mid = int(request.args.get('mid', -1))

    conn = get_db()

     # Tell ODBC that you are starting a multi-statement transaction
    conn.autocommit = False

    # WRITE YOUR CODE HERE

    response = {"rent": "fail"}
    cursor = conn.execute("SELECT * FROM Rental")
    rentalRecords = cursor.fetchall()
    cursor = conn.execute("SELECT * FROM Customer WHERE cid = ?", cid)
    record = cursor.fetchall()
    pid = record[0][1]
    cursor = conn.execute("SELECT * FROM RentalPlan WHERE pid = ?", pid)
    record = cursor.fetchall()
    n = record[0][3]
    
    cursor = conn.execute("INSERT INTO Rental(cid, mid, date_and_time, status) VALUES (?, ?, ?, ?)", cid, mid, currentTime(), "open")
    
    for r in rentalRecords:
        if r[1] == mid and r[3] == "open":
            conn.rollback()
            return jsonify(response)
        if r[0] == cid and r[3] == "open":
            n -= 1
            if n == 0:
                conn.rollback()
                return jsonify(response)
        
    conn.commit()


    conn.autocommit = True


    #response = {"rent": "success"} OR response = {"rent": "fail"}
    response = {"rent": "success"}
    return jsonify(response)

