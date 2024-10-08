import json
import pandas as pd
from pandas import json_normalize

# Charger le fichier JSON
with open("json_data/data_page_1.json", "r", encoding="utf-8") as f:
    fichier = json.load(f)

#print(fichier)
df = json_normalize(fichier["events"])  # Results contain the required data
print(df)