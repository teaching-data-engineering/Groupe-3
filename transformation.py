import json
import pandas as pd
from pandas import json_normalize
import os
import requests
from datetime import datetime

"""
# Supprimer les anciens fichiers
def supprimer_fichiers(data_folder):
    # Boucler de 1 à 29
    for i in range(1, 30):
        # Construire le nom du fichier à supprimer
        nom_fichier = f"data_page_{i}.json"
        chemin_fichier = os.path.join(data_folder, nom_fichier)

        try:
            # Vérifier si le fichier existe
            if os.path.isfile(chemin_fichier):
                # Supprimer le fichier
                os.remove(chemin_fichier)
                print(f"Fichier supprimé : {nom_fichier}")
            else:
                print(f"Le fichier {nom_fichier} n'existe pas.")
        except Exception as e:
            print(f"Erreur lors de la suppression du fichier {nom_fichier}: {e}")

# Utilisation de la fonction
dossier_json = "json_data"  # Remplacez par le chemin de votre dossier
supprimer_fichiers(dossier_json)


# Supprimer les fichiers qui bloquent
def supprimer_fichiers_bloquent(data_folder, start_date, end_date):
    # Convertir les dates en objets datetime pour comparaison
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    # Parcourir tous les fichiers dans le dossier
    for nom_fichier in os.listdir(data_folder):
        # Vérifier si le nom du fichier commence par "data_page_" et contient "_page_"
        if nom_fichier.startswith("data_page_") and "_page_" in nom_fichier and nom_fichier.endswith(".json"):
            # Extraire la date du nom du fichier
            date_str = nom_fichier.split('_')[2]  # Suppose que la date est à l'index 2
            try:
                # Convertir la date extraite en objet datetime
                date_fichier = datetime.strptime(date_str, "%Y-%m-%d")
                
                # Vérifier si la date du fichier est dans la plage
                if start_date_obj <= date_fichier <= end_date_obj:
                    chemin_fichier = os.path.join(data_folder, nom_fichier)
                    try:
                        # Supprimer le fichier
                        os.remove(chemin_fichier)
                        print(f"Fichier supprimé : {nom_fichier}")
                    except Exception as e:
                        print(f"Erreur lors de la suppression du fichier {nom_fichier}: {e}")
            except ValueError:
                print(f"Date non valide dans le fichier {nom_fichier}. Ignoré.")

# Utilisation de la fonction
dossier_json = "json_data"
supprimer_fichiers_bloquent(dossier_json, "2024-10-08", "2024-10-17")
"""



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

# Utilisation de la fonction
dossier = "json_data"
df_complet = json_to_dataframe(dossier)

# Afficher les premières lignes du DataFrame combiné
#print(df_complet.head())

"""
def enrichissement(df_complet):
    geocode_api_url = "https://maps.googleapis.com/maps/api/geocode/json"
    location = df_complet.get("venueName")
    
    response = requests.get(f"{geocode_api_url}?address={location}")
    if response.ok:
        coords = response.json()
        print(coords)
        df_complet['latitude'] = coords['lat']
        df_complet['longitude'] = coords['lng']


def enrichissement_date(df):
    
    # Assurer que la colonne 'startsAt' est bien au format datetime
    df['startsAt'] = pd.to_datetime(df['startsAt'], errors='coerce')

    # Indiquer si l'événement est un week-end (samedi ou dimanche)
    df['is_weekend'] = df['startsAt'].dt.weekday >= 5  # 5 pour samedi, 6 pour dimanche

    # Numéro de la semaine de l'année
    df['week_number'] = df['startsAt'].dt.isocalendar().week

    # Mois de l'événement
    df['month'] = df['startsAt'].dt.month

    # Nombre de jours avant l'événement (par rapport à la date actuelle)
    now = pd.Timestamp.now()
    df['days_before_event'] = (df['startsAt'] - now).dt.days

    # Durée de l'événement si les données de début et de fin sont disponibles
    # Supposons qu'une colonne 'endsAt' existe (sinon cette partie peut être adaptée)
    if 'endsAt' in df.columns:
        df['endsAt'] = pd.to_datetime(df['endsAt'], errors='coerce')
        df['event_duration_hours'] = (df['endsAt'] - df['startsAt']).dt.total_seconds() / 3600
    else:
        df['event_duration_hours'] = None  # Si 'endsAt' n'existe pas, on met None

    return df


print(df_complet)
df_enrichi = enrichissement_date(df_complet)
#print(df_enrichi.head())
"""