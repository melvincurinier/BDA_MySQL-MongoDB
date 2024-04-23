import json
from pymongo import MongoClient

# Connexion à la base de données MongoDB
client = MongoClient('localhost', 27017)
db = client['imdb']

    # Récupérer les données de différentes collections
    movies_collection = db['movies']
    episodes_collection = db['episodes']
    persons_collection = db['persons']
    characters_collection = db['characters']
    directors_collection = db['directors']
    genres_collection = db['genres']
    knownformovies_collection = db['knownformovies']
    principals_collection = db['principals']
    professions_collection = db['professions']
    ratings_collection = db['ratings']
    titles_collection = db['titles']
    writers_collection = db['writers']

    # Créer une nouvelle collection pour les films avec chaque film représenté par un objet JSON
    film_collection = db['film_collection']

    # Parcourir les documents de la collection de films
    for movie_data in movies_collection.find():
        # Récupérer les données associées à ce film depuis d'autres collections
        episodes_data = list(episodes_collection.find({'mid': movie_data['mid']}))
        directors_data = list(directors_collection.find({'mid': movie_data['mid']}))
        genres_data = list(genres_collection.find({'mid': movie_data['mid']}))
        knownformovies_data = list(knownformovies_collection.find({'mid': movie_data['mid']}))
        principals_data = list(principals_collection.find({'mid': movie_data['mid']}))
        ratings_data = list(ratings_collection.find({'mid': movie_data['mid']}))
        titles_data = list(titles_collection.find({'mid': movie_data['mid']}))
        writers_data = list(writers_collection.find({'mid': movie_data['mid']}))

        # Structurer les données du film avec les données associées
        film_info = {
            'mid': movie_data['mid'],
            'titleType': movie_data['titleType'],
            'primaryTitle': movie_data['primaryTitle'],
            'originalTitle': movie_data['originalTitle'],
            'isAdult': movie_data['isAdult'],
            'startYear': movie_data['startYear'],
            'endYear': movie_data['endYear'],
            'runtimeMinutes': movie_data['runtimeMinutes'],
            'episodes': episodes_data,
            'directors': directors_data,
            'genres': genres_data,
            'knownFor': knownformovies_data,
            'principals': principals_data,
            'ratings': ratings_data,
            'titles': titles_data,
            'writers': writers_data
            # Ajoutez les informations sur chaque personne
        }

        # Insérer le nouvel objet JSON dans la nouvelle collection
        film_collection.insert_one(film_info)
        print("Film ajouté à la collection 'film_collection' :", movie_data['primaryTitle'])

    print("Nouvelle collection 'film_collection' créée avec succès.")
