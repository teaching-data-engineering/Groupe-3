import os
import json
import time
import google.generativeai as genai
import google.api_core.exceptions
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Configuration des API keys
API_KEYS = [
    "AIzaSyBf6KzyfrVb-LsR4_b_EAajYGa2N_7QZkc",  # Clé API 1
    "AIzaSyBwJduDQg1iguNDnge9p3AhE_8ualBuZGU",  # Clé API 2
    "AIzaSyA_QpccoRmuy0JvOjNxIgqTj0aCXuoZNtE",  # Clé API 3
]

# Configuration du modèle
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 64,
    "max_output_tokens": 8192,
    "response_mime_type": "text/plain",
}

# Créer le modèle
model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    generation_config=generation_config,
)

def configure_api(api_key):
    """Configure l'API avec la clé donnée."""
    genai.configure(api_key=api_key)

def get_artist_nationality(artist_name, api_key, retries=3):
    """Récupère le genre musical d'un artiste en fonction de l'API key donnée."""
    configure_api(api_key)  # Configurer l'API avec la clé appropriée
    
    for attempt in range(retries):
        try:
            print(f"Requête API pour l'artiste : {artist_name} (essai {attempt + 1})")
            chat_session = model.start_chat(
                history=[
                    {
                        "role": "user",
                        "parts": [
                            f"quelle est le genre musicale de {artist_name} ? Donne moi juste le genre musical sans phrase (exemple : Blues, Rap...). Si le genre musical est inconnue mettre Inconnu.",
                        ],
                    }
                ]
            )
            response = chat_session.send_message(
                f"quelle est le genre musicale de {artist_name} ? Donne moi juste le genre musical sans phrase (exemple : Blues, Rap...). Si le genre musical est inconnue mettre Inconnu."
            )
            print(f"Réponse API pour {artist_name} : {response.text.strip()}")
            return response.text.strip()  # Nettoyage des espaces inutiles
        except google.api_core.exceptions.ResourceExhausted:
            print(f"Quota dépassé pour {artist_name}. Attente de 10 secondes avant nouvelle tentative.")
            time.sleep(3)
        except Exception as e:
            print(f"Erreur lors de la requête pour {artist_name} : {e}")
            break
    return "Information non disponible"

def load_all_artist_names_from_directory(json_directory):
    """Charge tous les noms d'artistes depuis des fichiers JSON dans un répertoire."""
    artist_names_set = set()  # Utilisation d'un set pour éviter les doublons
    print(f"Début du chargement des fichiers JSON dans : {json_directory}")
    total_files = len([f for f in os.listdir(json_directory) if f.endswith(".json")])
    print(f"Nombre total de fichiers JSON trouvés : {total_files}")

    for i, filename in enumerate(os.listdir(json_directory), start=1):
        if filename.endswith(".json"):
            file_path = os.path.join(json_directory, filename)
            print(f"[{i}/{total_files}] Chargement du fichier : {filename}")
            try:
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    for event in data.get('events', []):
                        artist_name = event.get('artistName')
                        if artist_name:
                            artist_names_set.add(artist_name)  # Ajout au set (pas de doublons)
            except Exception as e:
                print(f"Erreur lors du chargement du fichier {file_path} : {e}")

    print(f"Nombre total d'artistes uniques trouvés : {len(artist_names_set)}")
    return list(artist_names_set)  # Convertir le set en liste

def process_artists_in_parallel(artist_names, api_keys):
    """Traite les artistes en parallèle avec différentes clés API."""
    artist_nationalities = {}
    total_artists = len(artist_names)
    print(f"Nombre total d'artistes à traiter : {total_artists}")

    def process_artist(artist_name, api_key):
        """Ajoute un délai aléatoire pour éviter de dépasser les quotas."""
        time.sleep(random.uniform(1, 3))  # Délai aléatoire entre 1 et 3 secondes
        return artist_name, get_artist_nationality(artist_name, api_key)


    # Diviser les artistes de manière équitable entre les clés API
    groups = [artist_names[i::len(api_keys)] for i in range(len(api_keys))]

    with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
        future_to_artist = {}
        for api_key, group in zip(api_keys, groups):
            for artist_name in group:
                future = executor.submit(process_artist, artist_name, api_key)
                future_to_artist[future] = artist_name
        
        for future in as_completed(future_to_artist):
            artist_name = future_to_artist[future]
            try:
                artist_name, nationality = future.result()
                artist_nationalities[artist_name] = nationality
            except Exception as e:
                print(f"Erreur lors du traitement de {artist_name} : {e}")

    print("Traitement terminé.")
    return artist_nationalities

# Utilisation du répertoire contenant les fichiers JSON
json_directory = "/Users/titouancabon/Desktop/Université/Cours master/S9/Tech AI/TD 1/Group 3/Groupe_3/json_data"  # Remplace par le chemin de ton répertoire
artist_names = load_all_artist_names_from_directory(json_directory)

# Traiter les artistes en parallèle
artist_nationalities = process_artists_in_parallel(artist_names, API_KEYS)

# Afficher les résultats
print("Genres musicaux des artistes récupérés :")
for artist, nationality in artist_nationalities.items():
    print(f"{artist}: {nationality}")
