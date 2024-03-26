import json
from bson import ObjectId  # Ajout de la bibliothèque bson pour gérer les ObjectId
from pymongo import MongoClient

# Connexion à la base de données MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['imdb']

# Créer une nouvelle collection pour les films avec toutes les informations
film_collection = db['film_collection']

# Liste pour stocker tous les films
all_films = []


def createFilmCollection():

    # Récupérer les données de différentes collections
    movies_collection = db['movies']
    directors_collection = db['directors']
    genres_collection = db['genres']
    principals_collection = db['principals']
    ratings_collection = db['ratings']
    titles_collection = db['titles']
    writers_collection = db['writers']

    # Parcourir les documents de la collection de films
    for movie_data in movies_collection.find():
        # Récupérer les données associées à ce film depuis d'autres collections
        directors_data = list(directors_collection.find({'mid': movie_data['mid']}))
        genres_data = list(genres_collection.find({'mid': movie_data['mid']}))
        principals_data = list(principals_collection.find({'mid': movie_data['mid']}))
        ratings_data = list(ratings_collection.find({'mid': movie_data['mid']}))
        titles_data = list(titles_collection.find({'mid': movie_data['mid']}))
        writers_data = list(writers_collection.find({'mid': movie_data['mid']}))

        # Construire un objet JSON avec toutes les informations du film et les données associées
        film_info = {
            'mid': movie_data['mid'],
            'titleType': movie_data['titleType'],
            'primaryTitle': movie_data['primaryTitle'],
            'originalTitle': movie_data['originalTitle'],
            'isAdult': movie_data['isAdult'],
            'startYear': movie_data['startYear'],
            'endYear': movie_data['endYear'],
            'runtimeMinutes': movie_data['runtimeMinutes'],
            'directors': directors_data,
            'genres': genres_data,
            'principals': principals_data,
            'ratings': ratings_data,
            'titles': titles_data,
            'writers': writers_data
            # Ajoutez d'autres champs et données associées selon vos besoins
        }

        # Ajouter le film à la liste
        all_films.append(film_info)
        print("film insére:", movie_data['primaryTitle'])

        # Insérer le document JSON dans la nouvelle collection pour les films
        film_collection.insert_one(film_info)

    # Écrire les données dans un fichier JSON
    with open('films_data.json', 'w') as json_file:
        # Exclure l'identifiant _id lors de la sérialisation JSON
        json.dump(all_films, json_file, indent=4, default=str)
        print("Données écrites dans le fichier JSON.")
        
def afficher_tous_les_films():
    # Récupérer tous les films de la collection
    tous_les_films = film_collection.find()

    # Affichage des résultats
    for film in tous_les_films:
        print(film["primaryTitle"])

# createFilmCollection()
# test_film_collection("The Kid")