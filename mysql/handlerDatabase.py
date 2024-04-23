import sqlite3

def dropTables(cursor : sqlite3.Cursor):
    dropTable(cursor, "movies")
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

def dropTable(cursor : sqlite3.Cursor, name):
    cursor.execute("DROP TABLE IF EXISTS ".format(name))