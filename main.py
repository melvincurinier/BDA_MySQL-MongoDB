import mysql.createConnection as mysqlconnection

import database as db

if __name__ == "__main__":
   #db.createMySQLDatabase("small", True)
   #db.export_mysqlDB_to_mongoDB("imdb", "small")
   
   #db.mysqlRequest(1)
   
   #db.mongodbRequest(1)

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
