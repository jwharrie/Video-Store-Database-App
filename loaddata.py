import pyodbc
from connect_db import connect_db


def loadRentalPlan(filename, conn):
    """
        Input:
            $filename: "RentalPlan.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "RentalPlan" in the "VideoStore" database on Azure
            2. Read data from "RentalPlan.txt" and insert them into "RentalPlan"
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE

    # CREATING TABLE

    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE RentalPlan(
        pid INTEGER PRIMARY KEY,
        pname VARCHAR(50),
        monthly_fee FLOAT,
        max_movies INTEGER
    )
    """)

    # COLLECTING AND INSERTING DATA

    f = open(filename, "r")
    data = f.readlines()
    f.close()

    for row in data:
        
        rentalPlan = []
        c = ""

        for i in row:
            if i == '|' or i == '\n':
                rentalPlan.append(c)
                c = ""
            else:
                c += i
            
        cursor.execute("INSERT INTO RentalPlan VALUES (?, ?, ?, ?)", rentalPlan)
    
def loadCustomer(filename, conn):
    """
        Input:
            $filename: "Customer.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Customer" in the "VideoStore" database on Azure
            2. Read data from "Customer.txt" and insert them into "Customer".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE

    # CREATING TABLE

    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE Customer(
        cid INTEGER PRIMARY KEY,
        pid INTEGER,
        username VARCHAR(50),
        password VARCHAR(50),
        FOREIGN KEY (pid) REFERENCES RentalPlan(pid) ON DELETE CASCADE
    )
    """)

    # COLLECTING AND INSERTING DATA
    
    f = open(filename, "r")
    data = f.readlines()
    f.close()

    for row in data:
        
        customer = []
        c = ""

        for i in row:
            if i == '|' or i == '\n':
                customer.append(c)
                c = ""
            else:
                c += i
        
        cursor.execute("INSERT INTO Customer VALUES (?, ?, ?, ?)", customer)
    
def loadMovie(filename, conn):
    """
        Input:
            $filename: "Movie.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Movie" in the "VideoStore" database on Azure
            2. Read data from "Movie.txt" and insert them into "Movie".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE

    # CREATING TABLE

    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE Movie(
        mid INTEGER PRIMARY KEY,
        mname VARCHAR(50),
        year INTEGER
    )
    """)
    
    # COLLECTING AND INSERTING DATA

    f = open(filename, "r")
    data = f.readlines()
    f.close()

    for row in data:
        
        movie = []
        c = ""

        for i in row:
            if i == '|' or i == '\n':
                movie.append(c)
                c = ""
            else:
                c += i
        
        cursor.execute("INSERT INTO Movie VALUES (?, ?, ?)", movie)

def loadRental(filename, conn):
    """
        Input:
            $filename: "Rental.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Rental" in the VideoStore database on Azure
            2. Read data from "Rental.txt" and insert them into "Rental".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE

    # CREATING TABLE

    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE Rental(
        cid INTEGER,
        mid INTEGER,
        date_and_time DATETIME,
        status VARCHAR(6),
        FOREIGN KEY (cid) REFERENCES Customer(cid) ON DELETE CASCADE,
        FOREIGN KEY (mid) REFERENCES Movie(mid) ON DELETE CASCADE
    )
    """)

    # COLLECTING AND INSERTING DATA

    f = open(filename, "r")
    data = f.readlines()
    f.close()

    for row in data:
        
        rental = []
        c = ""

        for i in row:
            if i == '|' or i == '\n':
                rental.append(c)
                c = ""
            else:
                c += i
        
        cursor.execute("INSERT INTO Rental VALUES (?, ?, ?, ?)", rental)
    
def dropTables(conn):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS Rental")
    cursor.execute("DROP TABLE IF EXISTS Customer")
    cursor.execute("DROP TABLE IF EXISTS RentalPlan")
    cursor.execute("DROP TABLE IF EXISTS Movie")



if __name__ == "__main__":
    conn = connect_db()

    dropTables(conn)

    loadRentalPlan("RentalPlan.txt", conn)
    loadCustomer("Customer.txt", conn)
    loadMovie("Movie.txt", conn)
    loadRental("Rental.txt", conn)


    conn.commit()
    conn.close()
