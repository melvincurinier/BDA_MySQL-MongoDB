import sqlite3

def createMySQLConnection(dbname):
    con = sqlite3.connect("../db/" + dbname)
    return con