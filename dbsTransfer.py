import sqlite3
from pymongo import MongoClient

def export_sqliteTable_to_mongoCollection(sqlite_path, mongo_url, sqlite_table, mongo_db, mongo_collection):
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute(f"PRAGMA table_info({sqlite_table})")
    sqlite_fields = [field[1] for field in sqlite_cursor.fetchall()]

    mongo_client = MongoClient(mongo_url)
    mongo_db = mongo_client[mongo_db]
    mongo_collection = mongo_db[mongo_collection]
    mongo_collection.delete_many({})

    sqlite_cursor.execute(f"SELECT * FROM {sqlite_table}")
    data_to_insert = sqlite_cursor.fetchall()

    for record in data_to_insert:
        document = {sqlite_fields[i]: record[i] for i in range(len(sqlite_fields))}
        mongo_collection.insert_one(document)

    sqlite_conn.close()
    mongo_client.close()
    print(sqlite_table + " done !")

def insert_data_mongoDB(uri_db, uri_mongodb, mongodbname, type): # './db/imdb.db', 'mongodb://localhost:27017/', 'imdb'
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'movies', mongodbname, 'movies')
    if(type == "full" or type == "medium"):
        export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'episodes', mongodbname, 'episodes')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'persons', mongodbname, 'persons')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'characters', mongodbname, 'characters')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'directors', mongodbname, 'directors')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'genres', mongodbname, 'genres')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'knownformovies', mongodbname, 'knownformovies')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'principals', mongodbname, 'principals')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'professions', mongodbname, 'professions')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'ratings', mongodbname, 'ratings')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'titles', mongodbname, 'titles')
    export_sqliteTable_to_mongoCollection(uri_db, uri_mongodb, 'writers', mongodbname, 'writers')

