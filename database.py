import mysql.createConnection as mysqlconnection
import mysql.createTables as mysqltables
import mysql.handlerDatabase as mysqlhandler
import mysql.exportDatabase as mysqlexport

import mongodb.createConnection as mongoconnection

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


