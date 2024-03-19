import database as db
import dbsTransfer as dbsT
import requeteMongoDB as rmdb
import requeteSQL as rsql
import time
import sqlite3
import pymongo

if __name__ == "__main__":
    # Création de la base de données
    # db.createDatabase("imdb.db", "imdb-small", True)

    # MySQL
    # con = sqlite3.connect("./db/imdb.db")
    # start_time = time.time()
    # res = rsql.requete4(con)
    # for row in res :
    #     print(row)
    # end_time = time.time()
    # con.close()
    
    # MongoDB
    # dbsT.insert_data_mongoDB('./db/imdb.db', 'mongodb://localhost:27017/', 'imdb', 'small')

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    collections = client["imdb"]
    start_time = time.time()
    rmdb.requete4(collections)
    end_time = time.time()

    client.close()
    
    print("Temps pour récupérer les informations :", end_time - start_time, "secondes")

