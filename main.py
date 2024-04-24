import json
import database as db
import requeteMongoDB as rmdb
import requeteSQL as rsql
import time
import sqlite3
import pymongo
import dbStructure as dbs
import mysql.sqlite_trigger_handler as sth

import database as db


if __name__ == "__main__":
   #db.createMySQLDatabase("small", True)
   #db.export_mysqlDB_to_mongoDB("imdb", "small")
   

   #  # MySQL
   # con = sqlite3.connect("./db/imdb.db")
   # start_time = time.time()
   # res = rsql.requete4(con)
   # for row in res :
   #   print(row)
   # end_time = time.time()
   # con.close()
    
   # MongoDB
   # dbsT.insert_data_mongoDB('./db/imdb.db', 'mongodb://localhost:27017/', 'imdb', 'small')
   # client = pymongo.MongoClient("mongodb://localhost:27017/")
   # collections = client["imdb"]
   # start_time = time.time()
   # dbs.afficher_tous_les_films()
   # end_time = time.time()
   # print("Temps pour récupérer les informations :", end_time - start_time, "secondes")

   # start_time = time.time()
   # rmdb.afficher_tous_les_films(collections)
   # rmdb.requete2(collections)
   # end_time = time.time()

   # client.close()
   # print("Temps pour récupérer les informations :", end_time - start_time, "secondes")

   # changer le mid
   # sth.sqlite_trigger_handler("INSERT", {
   #       "mid": "t111",
   #       "type": "movie",
   #       "primaryTitle": "Title 111",
   #       "originalTitle": "Original Title 111",
   #       "isAdult": 0,
   #       "startYear": "2022-01-01",
   #       "endYear": "2022-01-02",
   #       "runtimeMinutes": 120
   #    }, "movies")

   # Show the movie with mid = 0 from MongoDB
   # print(collections.movies.find_one({"mid": "t111"}))
