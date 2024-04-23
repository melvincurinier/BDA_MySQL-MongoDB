import csv
import sqlite3
import os

from mysql.createConnection import createMySQLConnection
import mysql.createTables as cT
import mysql.handlerDatabase as hD

def createDatabase(dbname, dataset, isWithIndexes):
    con = createMySQLConnection(dbname)
    cur = con.cursor()

    # DROP TABLES
    hD.dropTables(cur)
    if(dataset == "imdb-medium" or dataset == "imdb-full"):
        hD.dropTable(cur, "episodes")

    # CREATE TABLES
    cT.createMoviesTable(cur, True)
    if(dataset == "imdb-medium" or dataset == "imdb-full"):
        cT.createEpisodesTable(cur)
    cT.createPersonsTable(cur, True)
    cT.createCharactersTable(cur, True)
    cT.createDirectorsTable(cur)
    cT.createGenresTable(cur, True)
    cT.createKnownformoviesTable(cur, True)
    cT.createPrincipalsTable(cur)
    cT.createProfessionsTable(cur)
    cT.createRatingsTable(cur)
    cT.createTitlesTable(cur, True)
    cT.createWritersTable(cur)

    # INSERT MOVIES
    with open(dataset + "/movies.csv", "r", encoding="utf-8") as file:
        content = csv.reader(file, delimiter=',')
        next(content)
        cur.executemany("INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?)", content)

        # INSERT EPISODES
        with open(dataset + "/episodes.csv", "r", encoding="utf-8") as file:
            content = csv.reader(file, delimiter=',')
            next(content)
            cur.executemany("INSERT INTO episodes VALUES (?, ?, ?, ?)", content)

    # INSERT PERSONS
    file = open(dataset + "/persons.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO persons VALUES(?, ?, ?, ?)", content)
    file.close()

    # INSERT CHARACTERS
    file = open(dataset + "/characters.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO characters VALUES(?, ?, ?)", content)
    file.close()

    # INSERT DIRECTORS
    file = open(dataset + "/directors.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO directors VALUES(?, ?)", content)
    file.close()

    # INSERT GENRES
    file = open(dataset + "/genres.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO genres VALUES(?, ?)", content)
    file.close()

    # INSERT KNOWNFORMOVIES
    file = open(dataset + "/knownformovies.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO knownformovies VALUES(?, ?)", content)
    file.close()

    # INSERT PRINCIPALS
    file = open(dataset + "/principals.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO principals VALUES(?, ?, ?, ?, ?)", content)
    file.close()

    # INSERT PROFESSIONS
    file = open(dataset + "/professions.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO professions VALUES(?, ?)", content)
    file.close()


    # INSERT RATINGS
    file = open(dataset + "/ratings.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO ratings VALUES(?, ?, ?)", content)
    file.close()


    # INSERT TITLES
    file = open(dataset + "/titles.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO titles VALUES(?, ?, ?, ?, ?, ?, ?, ?)", content)
    file.close()


    # INSERT WRITERS
    file = open(dataset + "/writers.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO writers VALUES(?, ?)", content)
    file.close()

    # Récupérer la taille du fichier en octets
    taille_octets = os.path.getsize("./db/" + dbname)

    # Convertir la taille en format lisible par l'homme
    taille_lisible = taille_octets / (1024.0 ** 2)  # Convertir en mégaoctets
    print("Taille du fichier de base de données:", taille_lisible, "MB")

    con.commit()

    con.close()


