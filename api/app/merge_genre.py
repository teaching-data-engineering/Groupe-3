import pandas as pd

df_main = pd.read_csv("df_Mahe_Lucas_Leroux_Cabon_with_quartier.csv")
df_genres = pd.read_csv("artist_genres.csv")
df_genres.rename(columns={'artist': 'artistName'}, inplace=True)
# Effectuer la jointure (left join) entre les deux DataFrames
df_merged = pd.merge(df_main, df_genres, on="artistName", how="left")
# Remplacer "Information non disponible" par "Inconnu" dans la colonne 'genre'
df_merged['genre'] = df_merged['genre'].replace("Information non disponible", "Inconnu")

df_merged.to_csv("df2_Mahe_Lucas_Leroux_Cabon.csv", index=False, encoding='utf-8')