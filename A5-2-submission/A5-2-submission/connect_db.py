import pyodbc

def connect_db():
    ODBC_STR = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:videostoreserver301220132.database.windows.net,1433;Database=VideoStore;Uid=jwharrie@videostoreserver301220132;Pwd={Irasshaimas3};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return pyodbc.connect(ODBC_STR)


if __name__ == '__main__':
    print (connect_db())
