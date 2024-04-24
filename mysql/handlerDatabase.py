import sqlite3
import csv
import os

def dropTables(cursor : sqlite3.Cursor):
    dropTable(cursor, "movies")
    dropTable(cursor, "episodes")
    dropTable(cursor, "persons")
    dropTable(cursor, "characters")
    dropTable(cursor, "directors")
    dropTable(cursor, "genres")
    dropTable(cursor, "knownformovies")
    dropTable(cursor, "principals")
    dropTable(cursor, "professions")
    dropTable(cursor, "ratings")
    dropTable(cursor, "titles")
    dropTable(cursor, "writers")

def dropTable(cursor : sqlite3.Cursor, tableName : str):
    cursor.execute("DROP TABLE IF EXISTS {}".format(tableName))
    print("...droping table {}".format(tableName))

def insertDataFromCSV(cursor : sqlite3.Cursor, dataset : str, tableName : str):
    file = open(dataset + "/{}.csv".format(tableName), "r", encoding="utf-8")
    content = csv.reader(file, delimiter=',')
    next(content)
    match tableName:
        case "movies":
            cursor.executemany("INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?)", content)
        case "episodes":
            cursor.executemany("INSERT INTO episodes VALUES(?, ?, ?, ?)", content)
        case "persons":
            cursor.executemany("INSERT INTO persons VALUES(?, ?, ?, ?)", content)
        case "characters":
            cursor.executemany("INSERT INTO characters VALUES(?, ?, ?)", content)
        case "directors":
            cursor.executemany("INSERT INTO directors VALUES(?, ?)", content)
        case "genres":
            cursor.executemany("INSERT INTO genres VALUES(?, ?)", content)
        case "knownformovies":
            cursor.executemany("INSERT INTO knownformovies VALUES(?, ?)", content)
        case "principals":
            cursor.executemany("INSERT INTO principals VALUES(?, ?, ?, ?, ?)", content)
        case "professions":
            cursor.executemany("INSERT INTO professions VALUES(?, ?)", content)
        case "ratings":
            cursor.executemany("INSERT INTO ratings VALUES(?, ?, ?)", content)
        case "titles":
            cursor.executemany("INSERT INTO titles VALUES(?, ?, ?, ?, ?, ?, ?, ?)", content)
        case "writers":
            cursor.executemany("INSERT INTO writers VALUES(?, ?)", content)
        case _:
            print("Wrong table name {}".format(tableName))
    file.close()
    print("...inserting dataset {} finished".format(tableName))

def printSizeDatabase():
    path = "db/imdb.db"
    taille_octets = os.path.getsize(path)
    taille_lisible = taille_octets / (1024.0 ** 2)
    print("Size of the database: ", taille_lisible, "MB")