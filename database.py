import csv
import sqlite3
import os

import mysql.createTables as cT
import mysql.handlerDatabase as hD

def createMySQLDatabase(dbname : str, datasetType : str, withIndexes : bool):
    print("connecting to mysql database...\n")
    # Se connecter à la base de données
    con = sqlite3.connect(dbname)
    cur = con.cursor()

    # Supprimer les tables
    hD.dropTables(cur)

    # Créer toutes les tables
    cT.createMoviesTable(cur, withIndexes)
    if datasetType == "medium" or datasetType == "full":
        cT.createEpisodesTable(cur)
    cT.createPersonsTable(cur, withIndexes)
    cT.createCharactersTable(cur, withIndexes)
    cT.createDirectorsTable(cur)
    cT.createGenresTable(cur, withIndexes)
    cT.createKnownformoviesTable(cur, withIndexes)
    cT.createPrincipalsTable(cur)
    cT.createProfessionsTable(cur)
    cT.createRatingsTable(cur)
    cT.createTitlesTable(cur, withIndexes)
    cT.createWritersTable(cur)

    # Insérer les données des fichiers CSV vers la base de données
    dataset = "./imdb-{}".format(datasetType)
    hD.insertDataFromCSV(cur, dataset, "movies")
    if datasetType == "medium" or datasetType == "full":
        hD.insertDataFromCSV(cur, dataset, "episodes")
    hD.insertDataFromCSV(cur, dataset, "persons")
    hD.insertDataFromCSV(cur, dataset, "characters")
    hD.insertDataFromCSV(cur, dataset, "directors")
    hD.insertDataFromCSV(cur, dataset, "genres")
    hD.insertDataFromCSV(cur, dataset, "knownformovies")
    hD.insertDataFromCSV(cur, dataset, "principals")
    hD.insertDataFromCSV(cur, dataset, "professions")
    hD.insertDataFromCSV(cur, dataset, "ratings")
    hD.insertDataFromCSV(cur, dataset, "titles")
    hD.insertDataFromCSV(cur, dataset, "writers")

    # Afficher la taille de la base de données
    hD.printSizeDatabase(dbname)

    print("closing connection from mysql database...\n")
    con.commit()
    con.close()

    print("creating database finished !")
    


