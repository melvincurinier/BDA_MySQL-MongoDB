import sqlite3

def createMoviesTable(cursor : sqlite3.Cursor, withIndexes : bool):
    request = '''
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
    cursor.execute(request)

    if withIndexes:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_movies_primaryTitle ON movies(primaryTitle);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_movies_startYear ON movies(startYear);")

def createEpisodesTable(cursor : sqlite3.Cursor):
    request = '''
            CREATE TABLE IF NOT EXISTS episodes (
                mid TEXT,
                parentMid TEXT,
                seasonNumber TEXT,
                episodeNumber TEXT,
                PRIMARY KEY(mid),
                FOREIGN KEY(mid) REFERENCES movies(mid)
            )
            '''
    cursor.execute(request)

def createPersonsTable(cursor : sqlite3.Cursor, withIndexes : bool):
    request = '''
            CREATE TABLE IF NOT EXISTS persons (
                pid TEXT,
                primaryName TEXT,
                birthYear DATE,
                deathYear DATE,
                PRIMARY KEY(pid)
            )
            '''
    cursor.execute(request)

    if withIndexes:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_persons_primaryName ON persons(primaryName);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_persons_pid ON persons(pid);")

def createCharactersTable(cursor : sqlite3.Cursor, withIndexes : bool):
    request = '''
            CREATE TABLE IF NOT EXISTS characters (
                mid TEXT,
                pid TEXT,
                name TEXT,
                FOREIGN KEY(mid) REFERENCES movies(mid),
                FOREIGN KEY(pid) REFERENCES persons(pid)
            )
            '''
    cursor.execute(request)

    if withIndexes:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_characters_pid ON characters(pid);")

def createDirectorsTable(cursor : sqlite3.Cursor):
    request = '''
            CREATE TABLE IF NOT EXISTS directors (
                mid TEXT,
                pid TEXT,
                PRIMARY KEY(mid, pid),
                FOREIGN KEY(mid) REFERENCES movies(mid),
                FOREIGN KEY(pid) REFERENCES persons(pid)
            )
            '''
    cursor.execute(request)

def createGenresTable(cursor : sqlite3.Cursor, withIndexes : bool):
    request = '''
            CREATE TABLE IF NOT EXISTS genres (
                mid TEXT,
                genre TEXT,
                PRIMARY KEY(mid, genre),
                FOREIGN KEY(mid) REFERENCES movies(mid)
            )
            '''
    cursor.execute(request)

    if withIndexes:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_genres_genre on genres(genre);")

def createKnownformoviesTable(cursor : sqlite3.Cursor, withIndexes : bool):
    request = '''
            CREATE TABLE IF NOT EXISTS knownformovies (
                pid TEXT,
                mid TEXT,
                FOREIGN KEY(pid) REFERENCES persons(pid),
                FOREIGN KEY(mid) REFERENCES movies(mid)
            )
            '''
    cursor.execute(request)
    if withIndexes:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_knownformovies_mid ON knownformovies(mid)")


def createPrincipalsTable(cursor : sqlite3.Cursor):
    request = '''
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
    cursor.execute(request)

def createProfessionsTable(cursor):
    request = '''
        CREATE TABLE IF NOT EXISTS professions (
            pid TEXT,
            jobName TEXT,
            PRIMARY KEY(pid, jobName)
            FOREIGN KEY(pid) REFERENCES persons(pid)
        )
        '''
    cursor.execute(request)

def createRatingsTable(cursor : sqlite3.Cursor):
    request = '''
            CREATE TABLE IF NOT EXISTS ratings (
                mid TEXT,
                averageRating FLOAT,
                numVotes INTEGER,
                PRIMARY KEY(mid),
                FOREIGN KEY(mid) REFERENCES movies(mid)
            )
            '''
    cursor.execute(request)

def createTitlesTable(cursor : sqlite3.Cursor, withIndexes : bool):
    request = '''
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
    cursor.execute(request)

    if withIndexes:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_titles_region ON titles(region);")

def createWritersTable(cursor : sqlite3.Cursor):
    request = '''
            CREATE TABLE IF NOT EXISTS writers (
                mid TEXT,
                pid TEXT,
                PRIMARY KEY(mid, pid),
                FOREIGN KEY(mid) REFERENCES movies(mid),
                FOREIGN KEY(pid) REFERENCES persons(pid)
            )
            '''
    cursor.execute(request)