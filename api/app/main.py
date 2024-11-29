######## Fichier de création des endpoints ########

### Packages utilisés

from typing import Union, List
from fastapi import FastAPI, Query, Request, Depends, HTTPException, status
from pydantic import BaseModel
import sys
import os
from api.app.bigquery import execute_query
from .security import add_token, verify_token
from fastapi.responses import JSONResponse
import random
import string
from datetime import datetime, timedelta
import requests


## Initialisation de l'API
app = FastAPI()

@app.exception_handler(Exception)
async def all_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"message": f"Internal Server Error: {exc}"}
    )


## Création du token temporaire
@app.get("/generate-token/")
def generate_token():
    token = add_token()  # Ajoute un token à la liste des tokens valides
    return {"generated_token": token}


# Endpoint avec pagination et metadata
@app.get("/bigquery_data/")
async def get_bigquery_data(
    query: str,
    token: str = Depends(verify_token)
):
    try:
        # Exécution de la requête SQL via BigQuery
        results = execute_query(query)

        return {"data": results}
    except Exception as e:
        return {"error": str(e)}


# Endpoint Event
@app.get("/events/")
async def get_events(
    request: Request,
    token: str = Depends(verify_token),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
):
    query = """
    SELECT *
    FROM ai-technologies-ur2.dataset_groupe_3.ma_table
    """
    
    try:
        results = execute_query(query)

        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        formatted_results = [
            {
                "event_name": event["title"],
                "artist_name": event["artistName"],
                "venue": event["venueName"],
                "location": event["locationText"],
                "starts_at": event["startsAt"],
                "days_before_event": event["days_before_event"],
            }
            for event in paginated_results
        ]

        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": formatted_results
        }
    except Exception as e:
        return {"error": str(e)}


# Endpoint qui compte le nombre d'événement pour chaque jour de la semaine
@app.get("/events/by-day-of-week/")
async def events_by_day_of_week(
    token: str = Depends(verify_token),
    week: Union[int, None] = Query(None, ge=1, le=53),
):
    """
    Retourne le nombre d’événements pour chaque jour de la semaine.
    """
    query = """
    SELECT 
        EXTRACT(DAYOFWEEK FROM TIMESTAMP(startsAt)) AS day_of_week,
        COUNT(*) AS event_count
    FROM ai-technologies-ur2.dataset_groupe_3.ma_table
    """
    
    if week is not None:
        query += f" WHERE EXTRACT(WEEK FROM TIMESTAMP(startsAt)) = {week}"
    
    query += " GROUP BY day_of_week ORDER BY day_of_week"
    
    print(f"Executing query: {query}")  # Affiche la requête avant l'exécution
    
    try:
        results = execute_query(query)
        # Transformation des résultats en format lisible (1=Lundi, etc.)
        day_mapping = {1: "Dimanche", 2: "Lundi", 3: "Mardi", 4: "Mercredi", 5: "Jeudi", 6: "Vendredi", 7: "Samedi"}
        readable_results = [{"day": day_mapping[int(r["day_of_week"])], "event_count": r["event_count"]} for r in results]
        return {"data": readable_results}
    
    except Exception as e:
        print(f"Error: {str(e)}")  # Affiche l'erreur si elle survient
        return {"error": str(e)}



# Endpoint qui liste les événement filtrés par artiste, salle ou date
@app.get("/events/search/")
async def search_events(
    request: Request,
    token: str = Depends(verify_token),
    artistName: Union[str, None] = Query(None),
    venueName: Union[str, None] = Query(None),
    date_range: Union[str, None] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    query = "SELECT * FROM ai-technologies-ur2.dataset_groupe_3.ma_table WHERE 1=1"
    
    if artistName:
        query += f" AND LOWER(artistName) LIKE '%{artistName.lower()}%'"
    if venueName:
        query += f" AND LOWER(venueName) LIKE '%{venueName.lower()}%'"
    if date_range:
        start_date, end_date = date_range.split(",")
        query += f" AND DATE(TIMESTAMP(startsAt)) BETWEEN '{start_date}' AND '{end_date}'"

    try:
        results = execute_query(query)

        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        formatted_results = [
            {
                "event_name": event["title"],
                "artist_name": event["artistName"],
                "venue": event["venueName"],
                "location": event["locationText"],
                "starts_at": event["startsAt"],
                "days_before_event": event["days_before_event"],
            }
            for event in paginated_results
        ]

        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": formatted_results
        }
    except Exception as e:
        return {"error": str(e)}


# Endpoint qui retourne les événement en fonction de la popularité
@app.get("/events/by-popularity/")
async def get_events_by_popularity(
    request: Request,
    token: str = Depends(verify_token),
    popularity: List[str] = Query(
        ...,
        description="Une ou plusieurs modalités de popularité à filtrer. Valeurs possibles : Faible, Moyenne, Haute, Très haute."
    ),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    valid_popularity = {"Faible", "Moyenne", "Haute", "Très haute"}

    invalid_values = [p for p in popularity if p not in valid_popularity]
    if invalid_values:
        raise HTTPException(
            status_code=422,
            detail=f"Valeurs invalides pour 'popularité': {invalid_values}. "
                   f"Valeurs acceptées : {valid_popularity}."
        )

    popularity_values = ', '.join([f"'{p}'" for p in popularity])

    query = f"""
    SELECT title, artistName, venueName, locationText, startsAt, genre
    FROM ai-technologies-ur2.dataset_groupe_3.ma_table
    WHERE popularite IN ({popularity_values})
    """

    try:
        results = execute_query(query)

        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": paginated_results
        }
    except Exception as e:
        return {"error": str(e)}



# End point par durée de l'événement
@app.get("/events/by-duration/")
async def get_events_by_duration(
    request: Request,
    token: str = Depends(verify_token),
    min_duration: float = Query(0, ge=0, description="Durée minimale des concerts (en heures)"),
    max_duration: float = Query(sys.float_info.max, ge=0, description="Durée maximale des concerts (en heures)"),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
):
    """
    Retourne les événements dont la durée est comprise entre min_duration et max_duration.
    """
    # Construire la requête SQL
    query = f"""
    SELECT title, artistName, venueName, locationText, startsAt, genre
    FROM ai-technologies-ur2.dataset_groupe_3.ma_table
    WHERE event_duration_hours BETWEEN {min_duration} AND {max_duration}
    """

    print(f"Executing query: {query}")  # Debug : affiche la requête

    try:
        # Exécuter la requête
        results = execute_query(query)

        # Pagination
        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        # Générer l'URL pour la page suivante (s'il y a une page suivante)
        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        # Retourner les résultats avec les métadonnées
        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": paginated_results
        }

    except Exception as e:
        print(f"Error: {str(e)}")  # Debug : affiche l'erreur
        return {"error": str(e)}

# Endpoint qui retourne les événements qui se déroulent le weekend ou qui se déroule la semaine
@app.get("/events/by-weekend/")
async def get_events_by_weekend(
    request: Request,
    token: str = Depends(verify_token),
    is_weekend: bool = Query(..., description="Filtre pour n'afficher que les concerts du week-end (True ou False)."),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
):
    """
    Retourne les événements qui se déroulent le week-end (samedi ou dimanche) si is_weekend=True,
    ou ceux qui ont lieu en semaine sinon.
    """
    # Construction de la requête SQL
    query = f"""
    SELECT title, artistName, venueName, locationText, startsAt, genre
    FROM ai-technologies-ur2.dataset_groupe_3.ma_table
    WHERE is_weekend = {'TRUE' if is_weekend else 'FALSE'}
    """

    print(f"Executing query: {query}")  # Debug : affiche la requête

    try:
        # Exécution de la requête
        results = execute_query(query)

        # Pagination
        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        # Générer l'URL pour la page suivante (s'il y a une page suivante)
        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        # Retourner les résultats avec les métadonnées
        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": paginated_results
        }

    except Exception as e:
        print(f"Error: {str(e)}")  # Debug : affiche l'erreur
        return {"error": str(e)}


## Endpoint qui liste les événements à venir dans les n jours prévus par l'artiste
@app.get("/events/upcoming/")
async def get_upcoming_events(
    request: Request,
    token: str = Depends(verify_token),
    days: int = Query(..., ge=1, description="Nombre de jours avant le concert (minimum 1)"),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
):
    """
    Retourne tous les événements ayant lieu dans les prochains days jours, avec pagination.
    """
    try:

        # Requête SQL pour filtrer les événements dans la plage spécifiée
        query = f"""
        SELECT title, artistName, venueName, locationText, startsAt, genre
        FROM ai-technologies-ur2.dataset_groupe_3.ma_table
        WHERE days_before_event BETWEEN 0 AND {days}
        ORDER BY days_before_event ASC
        """
        print(f"Executing query: {query}")  # Debug : affiche la requête

        # Exécuter la requête
        results = execute_query(query)

        # Pagination
        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        # Générer l'URL pour la page suivante (s'il y a une page suivante)
        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        # Retourner les résultats avec les métadonnées
        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": paginated_results
        }

    except Exception as e:
        print(f"Erreur : {str(e)}")  # Debug : affiche l'erreur
        return {"error": str(e)}



# Fonction de géocodage inversé (Nominatim)
def get_coordinates_from_district(district_name: str):
    url = f"https://nominatim.openstreetmap.org/search?q={district_name}&format=json"
    
    # Ajout d'un User-Agent
    headers = {
        "User-Agent": "MyApp/1.0 (titouan.cabon@gmail.com)" 
    }
    
    response = requests.get(url, headers=headers)
    
    # Vérifier si la requête a réussi
    if response.status_code == 200:
        try:
            data = response.json()
            if data:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                return lat, lon
            return None, None
        except ValueError:
            return None, None
    else:
        return None, None

    
# Endpoint qui liste les événements par genre
@app.get("/events/by-genre/")
async def get_events_by_genre(
    request: Request,
    token: str = Depends(verify_token),
    genre: List[str] = Query(..., description="Liste des genres d'événements à filtrer, séparés par des virgules."),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    # Construire la requête SQL avec les genres spécifiés
    genre_values = ', '.join([f"'{g}'" for g in genre])

    query = f"""
    SELECT title, artistName, venueName, locationText, startsAt, genre
    FROM ai-technologies-ur2.dataset_groupe_3.ma_table
    WHERE genre IN ({genre_values})
    """

    try:
        # Exécution de la requête
        results = execute_query(query)

        # Pagination
        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        # Générer l'URL pour la page suivante (s'il y a une page suivante)
        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": paginated_results
        }
    except Exception as e:
        return {"error": str(e)}
    

# Endpoint qui liste les événements par quartiers
@app.get("/events/by-district/")
async def get_events_by_district(
    request: Request,
    token: str = Depends(verify_token),
    district: List[str] = Query(..., description="Liste des quartiers d'événements à filtrer, séparés par des virgules."),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100)
):
    # Construire la requête SQL avec les genres spécifiés
    district_values = ', '.join([f"'{g}'" for g in district])

    query = f"""
    SELECT title, artistName, venueName, locationText, startsAt, genre
    FROM ai-technologies-ur2.dataset_groupe_3.ma_table
    WHERE quartier IN UNNEST([{district_values}])
    """

    try:
        # Exécution de la requête
        results = execute_query(query)

        # Pagination
        total_results = len(results)
        total_pages = (total_results + size - 1) // size
        start = (page - 1) * size
        end = start + size
        paginated_results = results[start:end]

        # Générer l'URL pour la page suivante (s'il y a une page suivante)
        next_page_url = None
        if page < total_pages:
            next_page_url = str(request.url).replace(f"page={page}", f"page={page + 1}")

        return {
            "metadata": {
                "page": page,
                "total_pages": total_pages,
                "results_per_page": size,
                "total_results": total_results,
                "next_page_url": next_page_url,
            },
            "data": paginated_results
        }
    except Exception as e:
        return {"error": str(e)}