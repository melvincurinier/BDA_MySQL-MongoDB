import createConnection as mongodbconnection
import mysql.createConnection as mysqlconnection

from pymongo.database import Database
from sqlite3 import Connection

# Fonction pour surveiller les changements dans MongoDB et les synchroniser avec SQLite
def sync_mongodb_to_sqlite(db : Database, sqlite_conn : Connection):
    # Démarre les changestreams pour surveiller les collections MongoDB
    with db.watch() as stream:
        for change in stream:
            # Récupère le type de modification (insertion, mise à jour, suppression)
            operation_type = change['operationType']
            # Récupère le nom de la collection affectée par la modification
            collection_name = change['ns']['coll']
            # Récupère le document modifié
            document = change['fullDocument']

            # Applique les modifications à la base de données SQLite en fonction du type d'opération
            if operation_type == 'insert':
                handle_insert_operation(collection_name, document, sqlite_conn)
            elif operation_type == 'update':
                handle_update_operation(collection_name, document, sqlite_conn)
            elif operation_type == 'delete':
                handle_delete_operation(collection_name, document, sqlite_conn)

# Fonction pour gérer les opérations d'insertion
def handle_insert_operation(collection_name : str, document, sqlite_conn : Connection):
    if collection_name == 'movies':
        cursor = sqlite_conn.cursor()
        cursor.execute("INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (document['mid'], document['titleType'], document['primaryTitle'], document['originalTitle'],
                        document['isAdult'], document['startYear'], document['endYear'], document['runtimeMinutes']))
        sqlite_conn.commit()
        print("Nouveau film inséré:", document['primaryTitle'])
        cursor.close()

# Fonction pour gérer les opérations de mise à jour
def handle_update_operation(collection_name : str, document, sqlite_conn : Connection):
    if collection_name == 'movies':
        cursor = sqlite_conn.cursor()
        cursor.execute("UPDATE movies SET titleType=?, primaryTitle=?, originalTitle=?, isAdult=?, startYear=?, endYear=?, runtimeMinutes=? WHERE mid=?",
                       (document['titleType'], document['primaryTitle'], document['originalTitle'],
                        document['isAdult'], document['startYear'], document['endYear'], document['runtimeMinutes'], document['mid']))
        sqlite_conn.commit()
        print("Film mis à jour:", document['primaryTitle'])
        cursor.close()

def handle_delete_operation(collection_name : str, document, sqlite_conn : Connection):
    if collection_name == 'movies':
        cursor = sqlite_conn.cursor()
        cursor.execute("DELETE FROM movies WHERE mid=?", (document['mid'],))
        sqlite_conn.commit()
        print("Film supprimé:", document['primaryTitle'])
        cursor.close()

if __name__ == "__main__":
    # Initialisation des connexions à SQLite et MongoDB
    sqlite_conn = mysqlconnection.createConnection()
    mongo_client = mongodbconnection.createConnection()
    mongo_db = mongo_client["imdb"]

    # Synchronisation continue des données entre MongoDB et SQLite
    sync_mongodb_to_sqlite(mongo_db, sqlite_conn)