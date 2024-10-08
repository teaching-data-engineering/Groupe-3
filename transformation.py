import json
import pandas as pd
from pandas import json_normalize
import os
import requests

def json_to_dataframe(dossier):

    # Compter le nombre de fichiers dans le dossier
    nombre_de_fichiers = len([f for f in os.listdir(dossier) if os.path.isfile(os.path.join(dossier, f))])

    # Créer une liste pour stocker les DataFrames de chaque page
    dataframes = []

    # Boucler sur toutes les pages disponibles
    for page_num in range(1, nombre_de_fichiers + 1):  # Les pages vont de 1 à nombre_de_fichiers inclus
        file_path = f"{dossier}/data_page_{page_num}.json"  # Chemin du fichier
        
        try:
            # Charger le fichier JSON
            with open(file_path, "r", encoding="utf-8") as f:
                fichier = json.load(f)
            
            # Créer un DataFrame pour chaque page
            df = json_normalize(fichier["events"])
            
            # Ajouter le DataFrame à la liste
            dataframes.append(df)
            
            # Afficher un message pour indiquer le succès
            print(f"Page {page_num} chargée avec succès.")
            
        except FileNotFoundError:
            print(f"Le fichier pour la page {page_num} est introuvable.")
        except KeyError:
            print(f"Problème avec la structure du fichier à la page {page_num}.")

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


def enrichissement(df_complet):
    geocode_api_url = "https://maps.googleapis.com/maps/api/geocode/json"
    location = df_complet.get("venueName")
    
    response = requests.get(f"{geocode_api_url}?address={location}")
    if response.ok:
        coords = response.json()
        print(coords)
        df_complet['latitude'] = coords['lat']
        df_complet['longitude'] = coords['lng']

enrichissement(df_complet)