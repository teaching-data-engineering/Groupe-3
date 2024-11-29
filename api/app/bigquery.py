from google.cloud import bigquery
from google.oauth2 import service_account
from typing import List, Dict
import os
import warnings

# Remplacez ce chemin par le chemin vers votre fichier de compte de service
SERVICE_ACCOUNT_PATH = "/Users/titouancabon/Desktop/Université/Cours master/S9/Tech AI/TD 1/Group 3/sa-key-group-3.json"

# Fonction pour se connecter à BigQuery
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

# Fonction pour exécuter une requête SQL dans BigQuery
def execute_query(query: str) -> List[Dict]:
    client = get_bigquery_client()
    query_job = client.query(query)  # Exécute la requête SQL
    results = query_job.result()  # Récupère les résultats
    return [dict(row) for row in results]  # Convertit les résultats en liste de dictionnaires