import json
import csv
import pandas as pd
from pandas import json_normalize
import os
import requests
from datetime import datetime
import numpy as np
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
import time
#from config import API_KEY


def json_to_dataframe(dossier):
    # Créer une liste pour stocker les DataFrames de chaque page
    dataframes = []

    # Boucler sur tous les fichiers du dossier
    for fichier in os.listdir(dossier):
        # Vérifier si c'est un fichier JSON qui correspond au format attendu
        if fichier.startswith("data_page_") and fichier.endswith(".json"):
            # Extraire le numéro de page à partir du nom du fichier
            try:
                # Extraire le numéro de page
                page_num = int(fichier.split('_page_')[-1].split('.')[0])
                # Extraire la date sécurisée du fichier
                file_safe_date = fichier.split('_page_')[0].split('data_page_')[-1]
            except (IndexError, ValueError):
                print(f"Le fichier {fichier} n'a pas pu être analysé pour le numéro de page.")
                continue
            
            file_path = os.path.join(dossier, fichier)  # Chemin complet du fichier
            
            try:
                # Charger le fichier JSON
                with open(file_path, "r", encoding="utf-8") as f:
                    contenu = json.load(f)
                
                # Créer un DataFrame pour chaque page
                df = json_normalize(contenu["events"])
                
                # Ajouter une colonne pour la date sécurisée
                df['file_safe_date'] = file_safe_date
                
                # Ajouter le DataFrame à la liste
                dataframes.append(df)
                
                # Afficher un message pour indiquer le succès
                print(f"Page {page_num} chargée avec succès à partir de {fichier}.")
                
            except FileNotFoundError:
                print(f"Le fichier {fichier} est introuvable.")
            except KeyError:
                print(f"Problème avec la structure du fichier {fichier}.")
            except json.JSONDecodeError:
                print(f"Erreur lors du chargement du fichier JSON {fichier}.")
    
    # Combiner tous les DataFrames en un seul
    if dataframes:
        df_complet = pd.concat(dataframes, ignore_index=True)
        return df_complet
    else:
        print("Aucun fichier valide n'a été trouvé.")
        return pd.DataFrame()  # Renvoie un DataFrame vide si aucun fichier n'a été trouvé



# Afficher les premières lignes du DataFrame combiné
#print(df_complet.head())

def enrichissement(dossier):
    df_complet = json_to_dataframe(dossier)

    ########################## Enrichissement sur la date ##########################

    # Passage de la date au format DateTime
    df_complet['startsAt'] = pd.to_datetime(df_complet['startsAt'], errors='coerce')

    # week-end 
    df_complet['is_weekend'] = df_complet['startsAt'].dt.weekday >= 5  # 5 pour samedi, 6 pour dimanche

    # Numéro de la semaine de l'année
    df_complet['week_number'] = df_complet['startsAt'].dt.isocalendar().week

    # Mois
    df_complet['month'] = df_complet['startsAt'].dt.month

    # Nombre de jours avant l'événement (par rapport à la date actuelle)
    now = pd.Timestamp.now()
    df_complet['days_before_event'] = (df_complet['startsAt'] - now).dt.days

    # Durée de l'événement si les données de début et de fin sont disponibles
    if 'endsAt' in df_complet.columns:
        df_complet['endsAt'] = pd.to_datetime(df_complet['endsAt'], errors='coerce')
        df_complet['event_duration_hours'] = (df_complet['endsAt'] - df_complet['startsAt']).dt.total_seconds() / 3600
    else:
        df_complet['event_duration_hours'] = None  

    ########################## Segmentation en fonction de la popularité ##########################

    # Créer une nouvelle colonne 'popularite'
    df_complet['popularite'] = pd.cut(df_complet['rsvpCountInt'], bins=[0, 2, 50, 200, float('inf')], labels=['Faible', 'Moyenne', 'Haute', 'Très Haute'], right=False)

    ########################## Nombre de concerts de l'artiste dans cette salle dans la période donnée ##########################

    # Créer une nouvelle colonne pour le nombre de jours de concert
    df_complet['nombre_jours_concert'] = 0

    # Grouper par artiste et lieu
    grouped = df_complet.groupby(['artistName', 'venueName'])

    for name, group in grouped:
        # Compter le nombre de jours uniques
        unique_days = group['startsAt'].dt.date.nunique()  # Compte des jours uniques
        df_complet.loc[group.index, 'nombre_jours_concert'] = unique_days
    return df_complet


########################## Coordonnées géographiques ##########################

def get_coordinates(api_key, location):
    """Obtenir les coordonnées d'une localisation via l'API OpenRouteService."""
    url = "https://api.openrouteservice.org/geocode/search"
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    params = {
        'text': location,
        'size': 1  # Nombre de résultats à renvoyer
    }

    while True:  # Boucle pour gérer les tentatives
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Vérifie les erreurs HTTP
            data = response.json()

            # Vérifiez si des résultats ont été trouvés
            if data['features']:
                coords = data['features'][0]['geometry']['coordinates']
                return coords[0], coords[1]  # Renvoie (longitude, latitude)
            else:
                print(f"Aucun résultat trouvé pour {location}.")
                return None, None  # Aucun résultat trouvé

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Trop de requêtes
                print(f"Erreur 429: Trop de requêtes. Attente de 30 secondes...")
                time.sleep(30) 
            else:
                print(f"Erreur lors de la récupération des coordonnées pour '{location}': {e}")
                return None, None  # Retourne None en cas d'erreur

        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la récupération des coordonnées pour '{location}': {e}")
            return None, None  # Retourne None en cas d'erreur


def add_coordinates_to_dataframe(df, api_key):
    """Ajoute les coordonnées (longitude, latitude) au DataFrame en évitant les doublons."""
    # Créer une colonne pour stocker les coordonnées
    df['longitude'] = None
    df['latitude'] = None

    # Regrouper par 'venueName' et 'locationText' pour obtenir des localisations uniques
    unique_locations = df.groupby(['venueName', 'locationText']).size().reset_index(name='counts')
    unique_locations['full_location'] = unique_locations.apply(lambda row: f"{row['venueName']}, {row['locationText']}", axis=1)

    # Dictionnaire pour stocker les coordonnées récupérées
    coordinates_dict = {}

    for index, row in unique_locations.iterrows():
        location = row['full_location']
        print(f"Recherche des coordonnées pour: {location}")  # Message de débogage

        # Obtenez les coordonnées
        longitude, latitude = get_coordinates(api_key, location)

        # Stockez les coordonnées dans le dictionnaire
        coordinates_dict[location] = (longitude, latitude)

        # Attendre un peu pour éviter d'être bloqué par l'API
        time.sleep(1)


    # Maintenant, associer les coordonnées originales dans le DataFrame
    for index, row in df.iterrows():
        location = f"{row['venueName']}, {row['locationText']}"
        if location in coordinates_dict:
            df.at[index, 'longitude'], df.at[index, 'latitude'] = coordinates_dict[location]

    return df


######################################### genre #################################################

# Configuration du modèle
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 64,
    "max_output_tokens": 8192,
    "response_mime_type": "text/plain",
}

def configure_model(api_key):
    """Configurer l'API avec une clé spécifique."""
    print(f"Configuration de l'API avec la clé : {api_key}")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        generation_config=generation_config,
    )
    print(f"Modèle configuré avec succès pour la clé : {api_key}")
    return model

# Fonction pour charger les noms d'artistes à partir des fichiers JSON
def load_all_artist_names_from_directory(json_directory):
    artist_names_set = set() # Utilisation d'un set pour éviter les doublons
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
                            artist_names_set.add(artist_name) # Ajout au set (pas de doublons)
            except Exception as e:
                print(f"Erreur lors du chargement du fichier {file_path} : {e}")

    print(f"Nombre total d'artistes uniques trouvés : {len(artist_names_set)}")
    return list(artist_names_set) # Convertir le set en liste

def get_artist_genre(artist_name, model):
    """Requête API pour obtenir le genre musical."""
    try:
        print(f"Requête pour l'artiste : {artist_name}")
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
        genre = response.text.strip()
        print(f"Genre obtenu pour {artist_name} : {genre}")
        return genre
    except Exception as e:
        print(f"Erreur pour {artist_name}: {e}")
        return "Information non disponible"

def process_artists_with_key(artists, api_key):
    """Traiter une sous-liste d'artistes avec une clé API."""
    print(f"Début du traitement pour {len(artists)} artistes avec la clé : {api_key}")
    model = configure_model(api_key)
    results = {}
    for i, artist in enumerate(artists):
        print(f"({i+1}/{len(artists)}) Traitement de l'artiste : {artist}")
        results[artist] = get_artist_genre(artist, model)
        # Attendre pour respecter les limites d'API
        time.sleep(7)
    print(f"Fin du traitement pour la clé : {api_key}")
    return results

def split_list(data, num_chunks):
    """Diviser la liste des artistes en morceaux."""
    return np.array_split(data, num_chunks)

def process_all_artists(artist_names, api_keys):
    """Répartir les artistes entre plusieurs clés API et traiter."""
    # Diviser la liste des artistes en morceaux
    chunks = split_list(artist_names, len(api_keys))
    print(f"Nombre total d'artistes : {len(artist_names)}")
    print(f"Division des artistes en {len(chunks)} morceaux, un par clé API.")

    all_results = {}

    # Traiter chaque morceau en parallèle avec une clé API
    with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
        futures = []
        for chunk, api_key in zip(chunks, api_keys):
            print(f"Soumission d'un lot de {len(chunk)} artistes à la clé : {api_key}")
            futures.append(executor.submit(process_artists_with_key, chunk, api_key))
        
        # Récupérer les résultats
        for future in futures:
            result = future.result()
            all_results.update(result)

    print("Traitement terminé pour tous les artistes.")
    return all_results




if __name__ == "__main__":

    dossier = "json_data"
    df_complet = enrichissement(dossier)
    API_KEY = '5b3ce3597851110001cf6248f4b62f4a46614677ab25ada4590c91c2'
    df_complet = add_coordinates_to_dataframe(df_complet, API_KEY)

    json_directory = "C:/Users/techia/Groupe_3/json_data" #chemin a modifié
    
    API_KEYS = [
    "AIzaSyDECea5UFUnF7p_mxWGC_Yb4fScxgFg57Y",
    "AIzaSyDB0prOCmBubBBkXi3tfaqBkuCXjRhCgmw",
    "AIzaSyDFZcHhsX3fzIJfmgN9_coCZ0C8692O_D4",
    ]
    
    artist_names = load_all_artist_names_from_directory(json_directory)
    results = process_all_artists(artist_names, API_KEYS)

    output_file = "artist_genres.csv"
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["artist", "genre"])  # En-têtes des colonnes
        for artist, genre in results.items():
            writer.writerow([artist, genre])

    print(f"Résultats sauvegardés dans le fichier : {output_file}")


    #rajouter les quartiers
    
    df_genres = pd.read_csv("artist_genres.csv")
    df_genres.rename(columns={'artist': 'artistName'}, inplace=True)
    # Effectuer la jointure (left join) entre les deux DataFrames
    df_merged = pd.merge(df_complet, df_genres, on="artistName", how="left")
    # Remplacer "Information non disponible" par "Inconnu" dans la colonne 'genre'
    df_merged['genre'] = df_merged['genre'].replace("Information non disponible", "Inconnu")

    df_merged.to_csv("df2_Mahe_Lucas_Leroux_Cabon.csv", index=False, encoding='utf-8')



