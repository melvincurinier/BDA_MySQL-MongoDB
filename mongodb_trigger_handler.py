import sqlite3
from pymongo import MongoClient
import time

# Fonction pour initialiser la connexion SQLite
def init_sqlite_connection(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    return conn

# Fonction pour initialiser la connexion MongoDB et les changestreams
def init_mongodb_connection(mongodb_url):
    client = MongoClient(mongodb_url)
    db = client['imdb']  # Remplacer 'imdb' par le nom de ta base de données MongoDB
    return db

# Fonction pour surveiller les changements dans MongoDB et les synchroniser avec SQLite
def sync_mongodb_to_sqlite(db, sqlite_conn):
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
def handle_insert_operation(collection_name, document, sqlite_conn):
    if collection_name == 'movies':
        cursor = sqlite_conn.cursor()
        cursor.execute("INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (document['mid'], document['titleType'], document['primaryTitle'], document['originalTitle'],
                        document['isAdult'], document['startYear'], document['endYear'], document['runtimeMinutes']))
        sqlite_conn.commit()
        print("Nouveau film inséré:", document['primaryTitle'])
        cursor.close()

# Fonction pour gérer les opérations de mise à jour
def handle_update_operation(collection_name, document, sqlite_conn):
    if collection_name == 'movies':
        cursor = sqlite_conn.cursor()
        cursor.execute("UPDATE movies SET titleType=?, primaryTitle=?, originalTitle=?, isAdult=?, startYear=?, endYear=?, runtimeMinutes=? WHERE mid=?",
                       (document['titleType'], document['primaryTitle'], document['originalTitle'],
                        document['isAdult'], document['startYear'], document['endYear'], document['runtimeMinutes'], document['mid']))
        sqlite_conn.commit()
        print("Film mis à jour:", document['primaryTitle'])
        cursor.close()

# Fonction pour gérer les opérations de suppression
def handle_delete_operation(collection_name, document, sqlite_conn):
    if collection_name == 'movies':
        cursor = sqlite_conn.cursor()
        cursor.execute("DELETE FROM movies WHERE mid=?", (document['mid'],))
        sqlite_conn.commit()
        print("Film supprimé:", document['primaryTitle'])
        cursor.close()

if __name__ == "__main__":
    # Initialisation des connexions à SQLite et MongoDB
    sqlite_conn = init_sqlite_connection("./db/imdb.db")  # Remplacer le chemin par le chemin de ta base de données SQLite
    mongodb_url = "mongodb://localhost:27017/"  # Remplacer localhost et le port par les informations de connexion à MongoDB
    db = init_mongodb_connection(mongodb_url)

    # Synchronisation continue des données entre MongoDB et SQLite
    sync_mongodb_to_sqlite(db, sqlite_conn)