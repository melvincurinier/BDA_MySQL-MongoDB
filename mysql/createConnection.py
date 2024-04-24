import sqlite3

def createConnection():
    path = "db/imdb.db"
    con = sqlite3.connect(path)
    return con