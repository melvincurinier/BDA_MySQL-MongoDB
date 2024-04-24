from pymongo.database import Database

def requete1(db : Database):
    # Accès aux collections MongoDB
    persons_collection = db['persons']
    characters_collection = db['characters']
    movies_collection = db['movies']

    # Requête MongoDB pour récupérer les informations sur Jean Reno
    person_query = {"primaryName": "Jean Reno"}
    person_document = persons_collection.find_one(person_query)

    # Récupération de l'ID de la personne Jean Reno
    person_id = person_document['pid']

    # Récupérer les personnages associés à Jean Reno
    characters_query = {"pid": person_id}
    characters_result = list(characters_collection.find(characters_query))

    # Récupérer les IDs des films associés à Jean Reno
    movie_ids = [character['mid'] for character in characters_result]

    # Récupérer les noms des films
    movies_query = {"mid": {"$in": movie_ids}}
    movies_result = movies_collection.find(movies_query, {"primaryTitle": 1})

    # Affichage des résultats
    for movie in movies_result:
        print(movie["primaryTitle"])

def requete2(db : Database):
    # Accès aux collections MongoDB
    genres_collection = db['genres']
    ratings_collection = db['ratings']
    movies_collection = db['movies']
    
    # Requête pour récupérer les documents de genre "Horror"
    genre_query = {"genre": "Horror"}
    genre_document = list(genres_collection.find(genre_query))

    # Extraction des IDs des films d'horreur
    horror_movies_ids = [movie['mid'] for movie in genre_document]

    # Requête pour récupérer les films d'horreur sortis entre 2000 et 2009
    movies_query = {"mid": {"$in": horror_movies_ids}, "startYear": {"$gte": 2000, "$lte": 2009}}
    movies_result = movies_collection.find(movies_query) # recuperer film

    # Extraction des IDs des films d'horreur
    movies_id = [movie['mid'] for movie in movies_result]

    # Requête pour récupérer les évaluations des films
    ratings_query = {"mid": {"$in": movies_id}}
    ratings_result = ratings_collection.find(ratings_query).sort("averageRating", -1).limit(3)
    movies_id = [movie['mid'] for movie in ratings_result]

    # Récupération des noms des films
    movies_query = {"mid": {"$in": movies_id}}
    movies_result = movies_collection.find(movies_query, {"primaryTitle": 1})

    # Affichage des noms des films
    for movie in movies_result:
        print(movie["primaryTitle"])

def requete3(db : Database):
    persons_collection = db['persons']
    writers_collection = db['writers']
    titles_collection = db['titles']

    # Recherche des titres de films dans la région 'ES'
    spanish_titles_result = titles_collection.find({'region': 'ES'})

    # Extraction des identifiants des films en Espagne
    spanish_movie_ids = [movie['mid'] for movie in spanish_titles_result]

    # Recherche des écrivains dont les films ne sont pas encore sortis en Espagne
    unreleased_es_movies_writers = writers_collection.find({'mid': {'$nin': spanish_movie_ids}})

    # Extraction des identifiants des écrivains de films non sortis en Espagne
    writers_ids = [writer['pid'] for writer in unreleased_es_movies_writers]

    # Recherche des noms des écrivains en fonction de leurs identifiants
    writers_names = persons_collection.find({'pid': {'$in': writers_ids}})

    # Affichage des noms des écrivains
    for doc in writers_names:
        print(doc['primaryName'])


def requete4(db : Database):
    persons_collection = db['persons']
    characters_collection = db['characters']
    movies_collection = db['movies']

    # Initialiser un dictionnaire pour stocker le nombre de rôles par acteur par film
    roles_per_person_per_movie = {}

    # Récupérer tous les personnages
    characters = characters_collection.find()

    # Parcourir tous les personnages et compter le nombre de rôles par acteur par film
    for character in characters:
        movie_id = character["mid"]
        person_id = character["pid"]
        # Vérifier si la combinaison personne-film existe déjà dans le dictionnaire
        if (person_id, movie_id) in roles_per_person_per_movie:
            # Si oui, ajouter le rôle à l'ensemble des rôles existants
            roles_per_person_per_movie[(person_id, movie_id)].add(character["name"])
        else:
            # Si non, initialiser un nouvel ensemble de rôles pour cette personne et ce film
            roles_per_person_per_movie[(person_id, movie_id)] = {character["name"]}

    # Identifier l'acteur avec le plus grand nombre de rôles différents dans un même film
    max_roles_count = 0
    for person_movie, roles in roles_per_person_per_movie.items():
        roles_count = len(roles)
        # Trouver le nombre maximal de rôles
        if roles_count > max_roles_count:
            max_roles_count = roles_count

    # Parcourir à nouveau le dictionnaire pour trouver les acteurs avec le nombre maximal de rôles
    for person_movie, roles in roles_per_person_per_movie.items():
        # Si le nombre de rôles est égal au nombre maximal trouvé
        if len(roles) == max_roles_count:
            # Récupérer le nom de l'acteur et le titre du film correspondant
            person_name = persons_collection.find_one({"pid": person_movie[0]})["primaryName"]
            movie_name = movies_collection.find_one({"mid": person_movie[1]})["primaryTitle"]
            # Afficher le résultat
            print(person_name + " - " + movie_name + " - " + str(len(roles)))

def requete5(db : Database):
    movies_collection = db["movies"]
    ratings_collection = db["ratings"]
    knownformovies_collection = db["knownformovies"]
    persons_collection = db["persons"]

    # Récupérer l'année de sortie du film "Avatar"
    avatar_year = movies_collection.find_one({"primaryTitle": "Avatar"})["startYear"]

    # Récupérer les films sortis après "Avatar"
    result_avatar_year = movies_collection.find({"startYear": {"$gte": avatar_year}}, {"mid": 1})

    # Récupérer les films avec plus de 200000 votes
    result_high_votes = ratings_collection.find({"numVotes": {"$gt": 200000}}, {"mid": 1})

    # Obtenir les identifiants des films ayant à la fois un nombre de votes élevé et sortis après "Avatar"
    films_avatar_year = {doc["mid"] for doc in result_avatar_year}
    films_high_votes = {doc["mid"] for doc in result_high_votes}
    hit_movies_after_avatar = films_avatar_year.intersection(films_high_votes)

    # Obtenir les identifiants des films ayant un nombre de votes faible et sortis avant "Avatar"
    movies_id = [movie for movie in hit_movies_after_avatar]
    result_low_votes = ratings_collection.find({'mid': {'$nin': movies_id }, "numVotes": {"$lt": 200000}}, {"mid": 1})

    # Obtenir les identifiants des films connus associés à ces films
    low_ratings_movies_id = [movie["mid"] for movie in result_low_votes]
    movies = movies_collection.find({'mid': {'$in': low_ratings_movies_id }, "startYear": {"$lt": avatar_year}}, {"mid": 1})

    knownformovies_movies = [movie["mid"] for movie in movies]
    knownformovie_persons = knownformovies_collection.find({'mid': {'$in': knownformovies_movies}}, {"pid": 1})

    # Obtenir les identifiants des personnes associées à ces films connus
    persons_id = [person["pid"] for person in knownformovie_persons]
    persons_name = persons_collection.find({'pid': {'$in':  persons_id}}, {"primaryName": 1})
    persons = persons_name.distinct("primaryName")

    # Afficher les noms des personnes associées
    for row in persons:
        print(row)




    