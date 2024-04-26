import mysql.createConnection as mysqlconnection
import mysql.createTables as mysqltables
import mysql.handlerDatabase as mysqlhandler
import mysql.exportDatabase as mysqlexport
import mysql.requeteSQL as mysqlrequest

import mongodb.createConnection as mongoconnection
import mongodb.requeteMongoDB as mongodbrequest
import mongodb.dbStructure as mongodbStructure
import mongodb.mongodb_trigger_handler as mongotrigger

import time
import json

from ctypes import *
from ctypes.util import find_library

def createMySQLDatabase(datasetType : str, withIndexes : bool):
    print("connecting to mysql database...")
    # Se connecter à la base de données
    con = mysqlconnection.createConnection()
    cur = con.cursor()

    # Supprimer les tables
    mysqlhandler.dropTables(cur)

    # Créer toutes les tables
    mysqltables.createMoviesTable(cur, withIndexes)
    if datasetType == "medium" or datasetType == "full":
        mysqltables.createEpisodesTable(cur)
    mysqltables.createPersonsTable(cur, withIndexes)
    mysqltables.createCharactersTable(cur, withIndexes)
    mysqltables.createDirectorsTable(cur)
    mysqltables.createGenresTable(cur, withIndexes)
    mysqltables.createKnownformoviesTable(cur, withIndexes)
    mysqltables.createPrincipalsTable(cur)
    mysqltables.createProfessionsTable(cur)
    mysqltables.createRatingsTable(cur)
    mysqltables.createTitlesTable(cur, withIndexes)
    mysqltables.createWritersTable(cur)

    # Insérer les données des fichiers CSV vers la base de données
    dataset = "./imdb-{}".format(datasetType)
    mysqlhandler.insertDataFromCSV(cur, dataset, "movies")
    if datasetType == "medium" or datasetType == "full":
        mysqlhandler.insertDataFromCSV(cur, dataset, "episodes")
    mysqlhandler.insertDataFromCSV(cur, dataset, "persons")
    mysqlhandler.insertDataFromCSV(cur, dataset, "characters")
    mysqlhandler.insertDataFromCSV(cur, dataset, "directors")
    mysqlhandler.insertDataFromCSV(cur, dataset, "genres")
    mysqlhandler.insertDataFromCSV(cur, dataset, "knownformovies")
    mysqlhandler.insertDataFromCSV(cur, dataset, "principals")
    mysqlhandler.insertDataFromCSV(cur, dataset, "professions")
    mysqlhandler.insertDataFromCSV(cur, dataset, "ratings")
    mysqlhandler.insertDataFromCSV(cur, dataset, "titles")
    mysqlhandler.insertDataFromCSV(cur, dataset, "writers")

    # Afficher la taille de la base de données
    mysqlhandler.printSizeDatabase()

    print("closing connection from mysql database...")
    con.commit()
    con.close()

    print("creating database finished !\n")

def export_mysqlDB_to_mongoDB(mongodbname : str, datasetType : str):
    print("connecting to mysql database...")
    # Se connecter à la base de données MySQL
    mysql_con = mysqlconnection.createConnection()
    cur = mysql_con.cursor()

    print("connecting to mongodb client...")
    # Se connecter au client MongoDB et récupérer la base de données
    mongo_client = mongoconnection.createConnection()
    mongo_db = mongo_client[mongodbname]

    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'movies')
    if datasetType == "medium" or datasetType == "full":
        mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'episodes')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'persons')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'characters')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'directors')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'genres')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'knownformovies')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'principals')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'professions')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'ratings')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'titles')
    mysqlexport.export_sqliteTable_to_mongoCollection(cur, mongo_db, 'writers')

    print("closing connection from mysql database...")
    mysql_con.close()

    print("closing connection from mongodb client...\n")
    mongo_client.close()

def mysqlRequest(number : int):
    con = mysqlconnection.createConnection()
    cur = con.cursor()
    start_time = time.time()
    match number:
        case 1:
            res = mysqlrequest.requete1(cur)
        case 2:
            res = mysqlrequest.requete2(cur)
        case 3:
            res = mysqlrequest.requete3(cur)
        case 4:
            res = mysqlrequest.requete4(cur)
        case 5:
            res = mysqlrequest.requete5(cur)
        case _:
            print("Wrong request number {}".format(number))
            res = "null"
    for row in res :
        print(row)
    end_time = time.time()
    con.close()
    print("Temps pour récupérer les informations :", end_time - start_time, "secondes")

def mongodbRequest(number):
   client = mongoconnection.createConnection()
   db = client["imdb"]
   start_time = time.time()
   match number:
        case 1:
            mongodbrequest.requete1(db)
        case 2:
            mongodbrequest.requete2(db)
        case 3:
            mongodbrequest.requete3(db)
        case 4:
            mongodbrequest.requete4(db)
        case 5:
            mongodbrequest.requete5(db)
        case _:
            print("Wrong request number {}".format(number))
   end_time = time.time()
   print("Temps pour récupérer les informations :", end_time - start_time, "secondes")

def mongoDBmoviesStructure():
    films = mongodbStructure.transformStructure()
    # Écrire les données dans un fichier JSON
    with open('films_data.json', 'w') as json_file:
        # Exclure l'identifiant _id lors de la sérialisation JSON
        json.dump(films, json_file, indent=4, default=str)
        print("Données écrites dans un fichier JSON.")

# Define some symbols
SQLITE_DELETE =  9
SQLITE_INSERT = 18
SQLITE_UPDATE = 23
data = []

# Define our callback function
#
# 'user_data' will be the third param passed to sqlite3_update_hook
# 'operation' will be one of: SQLITE_DELETE, SQLITE_INSERT, or SQLITE_UPDATE
# 'db name' will be the name of the affected database
# 'table_name' will be the name of the affected table
# 'row_id' will be the ID of the affected row
def callback(user_data, operation, db_name, table_name, row_id):
    mongo_client = mongoconnection.createConnection()
    mongo_db = mongo_client['imdb']

    sqlite_conn = mysqlconnection.createConnection()
    sqlite_cursor = sqlite_conn.cursor()

    if operation == SQLITE_DELETE:
        optext = 'Deleted row'
        synchronisemongodb(sqlite_cursor, mongo_db, table_name, row_id)
    elif operation == SQLITE_INSERT:
        optext = 'Inserted row'
        synchronisemongodb(sqlite_cursor, mongo_db, table_name, row_id)
    elif operation == SQLITE_UPDATE:
        optext = 'Updated row'
        synchronisemongodb(sqlite_cursor, mongo_db, table_name, row_id)
    else:
        optext = 'Unknown operation on row'
    s = '%s %ld of table "%s" in database "%s"' % (optext, row_id, table_name, db_name)
    print(s)

    mongo_client.close()
    sqlite_conn.close()

def synchronisemongodb(sqlite_cursor, mongo_db, table, row_id):
    sqlite_cursor.execute(f"SELECT * FROM {table} WHERE rowid = {row_id};")
    last_row = sqlite_cursor.fetchone()

    # Convertir la ligne SQLite en un dictionnaire
    column_names = [description[0] for description in sqlite_cursor.description]
    last_row_dict = dict(zip(column_names, last_row))

    # Ajouter à la base de données MongoDB la ligne last_row_dict
    mongo_collection = mongo_db[table]
    mongo_collection.insert_one(last_row_dict)

def mysql_trigger_tomongodb():
    # Translate into a ctypes callback
    c_callback = CFUNCTYPE(c_void_p, c_void_p, c_int, c_char_p, c_char_p, c_int64)(callback)

    # Load sqlite3
    dll = cdll.LoadLibrary(find_library('sqlite3'))

    # Holds a pointer to the database connection
    db = c_void_p()

    # Open a connection to 'imdb.db'
    dll.sqlite3_open('./db/imdb.db', byref(db))

    # Register callback
    dll.sqlite3_update_hook(db, c_callback, None)

    # Create a variable to hold error messages
    err = c_char_p()

    # Now execute some SQL
    # Exemple
    request = b'INSERT INTO movies VALUES ("tt111","movie","BDD","Base de donnees",0,1911,1913,71);'
    dll.sqlite3_exec(db, request, None, None, byref(err))
    if err:
        print(err.value)
    dll.sqlite3_close(db)

    
def mongo_trigger_tomysql():
    sqlite_conn = mysqlconnection.createConnection()
    mongo_client = mongoconnection.createConnection()
    mongo_db = mongo_client["imdb"]

    mongotrigger.sync_mongodb_to_sqlite(mongo_db, sqlite_conn)