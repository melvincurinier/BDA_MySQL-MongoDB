from pymongo import MongoClient
import mysql.createConnection as mysqlconnection
import mongodb.createConnection as mongoconnection

def sqlite_trigger_handler(operation, row_data, table_name):
    # Connexion à MongoDB
    mongo_client = mongoconnection.createConnection()
    mongo_db = mongo_client["imdb"]

    # Correspondance entre les tables SQLite et les collections MongoDB
    collections_mapping = {
        "movies": mongo_db["movies"],
        "episodes": mongo_db["episodes"],
        "persons": mongo_db["persons"],
        "characters": mongo_db["characters"],
        "directors": mongo_db["directors"],
        "genres": mongo_db["genres"],
        "knownformovies": mongo_db["knownformovies"],
        "principals": mongo_db["principals"],
        "professions": mongo_db["professions"],
        "ratings": mongo_db["ratings"],
        "titles": mongo_db["titles"],
        "writers": mongo_db["writers"]
    }

    # Synchronisation des données modifiées avec MongoDB
    if operation == "INSERT":
        collections_mapping[table_name].insert_one(row_data)
    elif operation == "UPDATE":
        # Mise à jour du document correspondant dans MongoDB
        # Remplacez les valeurs des champs modifiés par les nouvelles valeurs
        filter_query = {"mid": row_data["mid"]}  # Utilisez une clé primaire ou un identifiant unique pour la mise à jour
        update_data = {key: value for key, value in row_data.items() if key != "mid"}  # Excluez la clé primaire de la mise à jour
        collections_mapping[table_name].update_one(filter_query, {"$set": update_data})
    elif operation == "DELETE":
        collections_mapping[table_name].delete_one({"mid": row_data["mid"]})  # Supprime le document correspondant dans MongoDB

    # Connecter la fonction de déclencheur à SQLite
    sqlite_conn = mysqlconnection.createConnection()
    sqlite_conn.create_function("sqlite_trigger_handler", 3, sqlite_trigger_handler)
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON movies BEGIN SELECT sqlite_trigger_handler('INSERT', 'movies', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON movies BEGIN SELECT sqlite_trigger_handler('UPDATE', 'movies', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON movies BEGIN SELECT sqlite_trigger_handler('DELETE', 'movies', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON persons BEGIN SELECT sqlite_trigger_handler('INSERT', 'persons', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON persons BEGIN SELECT sqlite_trigger_handler('UPDATE', 'persons', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON persons BEGIN SELECT sqlite_trigger_handler('DELETE', 'persons', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON characters BEGIN SELECT sqlite_trigger_handler('INSERT', 'characters', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON characters BEGIN SELECT sqlite_trigger_handler('UPDATE', 'characters', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON characters BEGIN SELECT sqlite_trigger_handler('DELETE', 'characters', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON directors BEGIN SELECT sqlite_trigger_handler('INSERT', 'directors', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON directors BEGIN SELECT sqlite_trigger_handler('UPDATE', 'directors', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON directors BEGIN SELECT sqlite_trigger_handler('DELETE', 'directors', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON genres BEGIN SELECT sqlite_trigger_handler('INSERT', 'genres', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON genres BEGIN SELECT sqlite_trigger_handler('UPDATE', 'genres', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON genres BEGIN SELECT sqlite_trigger_handler('DELETE', 'genres', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON knownformovies BEGIN SELECT sqlite_trigger_handler('INSERT', 'knownformovies', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON knownformovies BEGIN SELECT sqlite_trigger_handler('UPDATE', 'knownformovies', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON knownformovies BEGIN SELECT sqlite_trigger_handler('DELETE', 'knownformovies', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON principals BEGIN SELECT sqlite_trigger_handler('INSERT', 'principals', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON principals BEGIN SELECT sqlite_trigger_handler('UPDATE', 'principals', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON principals BEGIN SELECT sqlite_trigger_handler('DELETE', 'principals', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON professions BEGIN SELECT sqlite_trigger_handler('INSERT', 'professions', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON professions BEGIN SELECT sqlite_trigger_handler('UPDATE', 'professions', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON professions BEGIN SELECT sqlite_trigger_handler('DELETE', 'professions', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON ratings BEGIN SELECT sqlite_trigger_handler('INSERT', 'ratings', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON ratings BEGIN SELECT sqlite_trigger_handler('UPDATE', 'ratings', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON ratings BEGIN SELECT sqlite_trigger_handler('DELETE', 'ratings', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON titles BEGIN SELECT sqlite_trigger_handler('INSERT', 'titles', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON titles BEGIN SELECT sqlite_trigger_handler('UPDATE', 'titles', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON titles BEGIN SELECT sqlite_trigger_handler('DELETE', 'titles', OLD.rowid); END;")

    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER INSERT ON writers BEGIN SELECT sqlite_trigger_handler('INSERT', 'writers', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER UPDATE ON writers BEGIN SELECT sqlite_trigger_handler('UPDATE', 'writers', NEW.rowid); END;")
    sqlite_conn.execute("CREATE TRIGGER IF NOT EXISTS sync_to_mongodb AFTER DELETE ON writers BEGIN SELECT sqlite_trigger_handler('DELETE', 'writers', OLD.rowid); END;")

    # Fermer les connexions
    sqlite_conn.close()
    mongo_client.close()

if __name__ == "__main__":
    # sqlite_trigger_handler("INSERT", {
    #     "mid": "t0",
    #     "type": "movie",
    #     "primaryTitle": "Title 0",
    #     "originalTitle": "Original Title 0",
    #     "isAdult": 0,
    #     "startYear": "2022-01-01",
    #     "endYear": "2022-01-02",
    #     "runtimeMinutes": 120
    # }, "movies")

    # Show the movie with mid = 0 from MongoDB
    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client["imdb"]
    # print(db.movies.find_one({"mid": "t0"}))

    # je veux tester un autre table 

    sqlite_trigger_handler("INSERT", {
        "pid": "t0",
        "name": "Name 0",
        "birthYear": "1990-01-01",
        "deathYear": "2022-01-01"
    }, "persons")

    # Show the person with pid = 0 from MongoDB
    print(db.persons.find_one({"pid": "t0"}))

    mongo_client.close()