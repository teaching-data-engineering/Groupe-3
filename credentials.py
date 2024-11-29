from google.oauth2 import service_account
import pandas as pd
import pandas_gbq

# Charger les credentials de Google Cloud
credentials = service_account.Credentials.from_service_account_file(
    '../sa-key-group-3.json',
)

# Lire le fichier CSV en DataFrame
df = pd.read_csv("df_Mahe_Lucas_Leroux_Cabon.csv")

# Exporter le DataFrame vers BigQuery
pandas_gbq.to_gbq(df, "dataset_group_3.ma_table", project_id="ai-technologies-ur2", credentials=credentials)

print("DataFrame exporté avec succès vers BigQuery.")

