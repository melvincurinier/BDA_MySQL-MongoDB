import csv
import sqlite3
import os

def createDatabase(dbname, dataset, isWithIndexes):
    con = sqlite3.connect("./db/" + dbname)
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS movies")
    cur.execute("DROP TABLE IF EXISTS persons")
    cur.execute("DROP TABLE IF EXISTS characters")
    cur.execute("DROP TABLE IF EXISTS directors")
    cur.execute("DROP TABLE IF EXISTS genres")
    cur.execute("DROP TABLE IF EXISTS knownformovies")
    cur.execute("DROP TABLE IF EXISTS principals")
    cur.execute("DROP TABLE IF EXISTS professions")
    cur.execute("DROP TABLE IF EXISTS ratings")
    cur.execute("DROP TABLE IF EXISTS titles")
    cur.execute("DROP TABLE IF EXISTS writers")

    # CREATE TABLE MOVIES
    cur.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            mid TEXT,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear DATE,
            endYear DATE,
            runtimeMinutes INTEGER,
            PRIMARY KEY(mid)
        )
        '''
    )

    # INSERT MOVIES
    with open(dataset + "/movies.csv", "r", encoding="utf-8") as file:
        content = csv.reader(file, delimiter=',')
        next(content)
        cur.executemany("INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?)", content)

    if(dataset == "medium" or dataset == "full"):
        cur.execute("DROP TABLE IF EXISTS episodes")

        # CREATE TABLE EPISODES
        cur.execute('''
            CREATE TABLE IF NOT EXISTS episodes (
                mid TEXT,
                parentMid TEXT,
                seasonNumber TEXT,
                episodeNumber TEXT,
                PRIMARY KEY(mid),
                FOREIGN KEY(mid) REFERENCES movies(mid)
            )
            '''
        )

        # INSERT EPISODES
        with open(dataset + "/episodes.csv", "r", encoding="utf-8") as file:
            content = csv.reader(file, delimiter=',')
            next(content)
            cur.executemany("INSERT INTO episodes VALUES (?, ?, ?, ?)", content)

    # CREATE TABLE PERSONS
    cur.execute('''
        CREATE TABLE IF NOT EXISTS persons (
            pid TEXT,
            primaryName TEXT,
            birthYear DATE,
            deathYear DATE,
            PRIMARY KEY(pid)
        )
        '''
    )

    # INSERT PERSONS
    file = open(dataset + "/persons.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO persons VALUES(?, ?, ?, ?)", content)
    file.close()


    # CREATE TABLE CHARACTERS
    cur.execute('''
        CREATE TABLE IF NOT EXISTS characters (
            mid TEXT,
            pid TEXT,
            name TEXT,
            FOREIGN KEY(mid) REFERENCES movies(mid),
            FOREIGN KEY(pid) REFERENCES persons(pid)
        )
        '''
    )

    # INSERT CHARACTERS
    file = open(dataset + "/characters.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO characters VALUES(?, ?, ?)", content)
    file.close()


    # CREATE TABLE DIRECTORS
    cur.execute('''
        CREATE TABLE IF NOT EXISTS directors (
            mid TEXT,
            pid TEXT,
            PRIMARY KEY(mid, pid),
            FOREIGN KEY(mid) REFERENCES movies(mid),
            FOREIGN KEY(pid) REFERENCES persons(pid)
        )
        '''
    )

    # INSERT DIRECTORS
    file = open(dataset + "/directors.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO directors VALUES(?, ?)", content)
    file.close()


    # CREATE TABLE GENRES
    cur.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            mid TEXT,
            genre TEXT,
            PRIMARY KEY(mid, genre),
            FOREIGN KEY(mid) REFERENCES movies(mid)
        )
        '''
    )

    # INSERT GENRES
    file = open(dataset + "/genres.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO genres VALUES(?, ?)", content)
    file.close()


    # CREATE TABLE KNOWNFORMOVIES
    cur.execute('''
        CREATE TABLE IF NOT EXISTS knownformovies (
            pid TEXT,
            mid TEXT,
            FOREIGN KEY(pid) REFERENCES persons(pid),
            FOREIGN KEY(mid) REFERENCES movies(mid)
        )
        '''
    )

    # INSERT KNOWNFORMOVIES
    file = open(dataset + "/knownformovies.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO knownformovies VALUES(?, ?)", content)
    file.close()


    # CREATE TABLE PRINCIPALS
    cur.execute('''
        CREATE TABLE IF NOT EXISTS principals (
            mid TEXT,
            ordering INTEGER,
            pid INTEGER,
            category TEXT,
            job TEXT,
            PRIMARY KEY(mid, ordering, pid),
            FOREIGN KEY(mid) REFERENCES movies(mid),
            FOREIGN KEY(pid) REFERENCES persons(pid)
        )
        '''
    )

    # INSERT PRINCIPALS
    file = open(dataset + "/principals.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO principals VALUES(?, ?, ?, ?, ?)", content)
    file.close()


    # CREATE TABLE PROFESSIONS
    cur.execute('''
        CREATE TABLE IF NOT EXISTS professions (
            pid TEXT,
            jobName TEXT,
            PRIMARY KEY(pid, jobName)
            FOREIGN KEY(pid) REFERENCES persons(pid)
        )
        '''
    )

    # INSERT PROFESSIONS
    file = open(dataset + "/professions.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO professions VALUES(?, ?)", content)
    file.close()


    # CREATE TABLE RATINGS
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            mid TEXT,
            averageRating FLOAT,
            numVotes INTEGER,
            PRIMARY KEY(mid),
            FOREIGN KEY(mid) REFERENCES movies(mid)
        )
        '''
    )

    # INSERT RATINGS
    file = open(dataset + "/ratings.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO ratings VALUES(?, ?, ?)", content)
    file.close()


    # CREATE TABLE TITLES
    cur.execute('''
        CREATE TABLE IF NOT EXISTS titles (
            mid TEXT,
            ordering INTEGER,
            title TEXT,
            region TEXT,
            language TEXT,
            types TEXT,
            attributes TEXT,
            isOriginalTitle INTEGER,
            PRIMARY KEY(mid, ordering),
            FOREIGN KEY(mid) REFERENCES movies(mid)
        )
        '''
    )

    # INSERT TITLES
    file = open(dataset + "/titles.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO titles VALUES(?, ?, ?, ?, ?, ?, ?, ?)", content)
    file.close()


    # CREATE TABLE WRITERS
    cur.execute('''
        CREATE TABLE IF NOT EXISTS writers (
            mid TEXT,
            pid TEXT,
            PRIMARY KEY(mid, pid),
            FOREIGN KEY(mid) REFERENCES movies(mid),
            FOREIGN KEY(pid) REFERENCES persons(pid)
        )
        '''
    )

    # INSERT WRITERS
    file = open(dataset + "/writers.csv", "r", encoding="utf-8")
    content = csv.reader(file, delimiter =',')
    next(content)
    cur.executemany("INSERT INTO writers VALUES(?, ?)", content)
    file.close()

    if isWithIndexes:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_characters_pid ON characters(pid);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_persons_primaryName ON persons(primaryName);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_genres_genre on genres(genre);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_persons_pid ON persons(pid);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_knownformovies_mid ON knownformovies(mid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_primaryTitle ON movies(primaryTitle);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_startYear ON movies(startYear);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_titles_region ON titles(region);")

    # Récupérer la taille du fichier en octets
    taille_octets = os.path.getsize("./db/" + dbname)

    # Convertir la taille en format lisible par l'homme
    taille_lisible = taille_octets / (1024.0 ** 2)  # Convertir en mégaoctets
    print("Taille du fichier de base de données:", taille_lisible, "MB")

    con.commit()

    con.close()


