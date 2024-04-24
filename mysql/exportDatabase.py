from sqlite3 import Cursor
from pymongo.database import Database

def export_sqliteTable_to_mongoCollection(cursor : Cursor, mongo_db : Database, tableName : str):
    cursor.execute("PRAGMA table_info({})".format(tableName))
    fields = [field[1] for field in cursor.fetchall()]

    collection = mongo_db[tableName]
    collection.delete_many({})

    cursor.execute("SELECT * FROM {}".format(tableName))
    mysql_data = cursor.fetchall()

    for row in mysql_data:
        document = {fields[i]: row[i] for i in range(len(fields))}
        collection.insert_one(document)

    print("...exporting dataset {} finished".format(tableName))