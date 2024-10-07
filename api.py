import requests

def get_json_from_url(mon_url,params=None):
    # Définir l'agent utilisateur
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/35.0.1916.47 Safari/537.36'}

    try:  # Gestion des exceptions avec un bloc try/except
        response = requests.get(mon_url, headers=headers, params=params)
        response.raise_for_status()  # Génère une exception pour les codes d'erreur HTTP
        return response.json()  # Renvoie directement la réponse au format JSON
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"Other error occurred: {err}")
    return None


def param(date= "2024-10-07T14:01:04,2024-10-31T23:00:00",longitude= -0.12574,latitude= 51.50853,salle=None, page=1, ville=2643743, genres='all-genres'):
    params ={ 'city_id': ville,  # ID de la ville de Londres
        'date': date,  # Plage de dates
        'page': page,  # Numéro de la page
        'longitude': longitude,  # Longitude de Londres
        'latitude': latitude,
        'genre_query': genres
    }
    return params



###########################test###############################

 
if __name__ == '__main__':
    url = "https://www.bandsintown.com/choose-dates/fetch-next/upcomingEvents"

    data = get_json_from_url(url,param(page=2))
    if data and 'events' in data:
        # Afficher le nom du premier artiste dans la liste des événements
        first_event = data['events'][0]
        artist_name = first_event.get('artistName', 'Artiste inconnu')
        print(f"Le premier artiste est : {artist_name}")
    else:
        print("Aucun événement trouvé ou données mal formatées.")
    
    if data and 'events' in data:
        # Afficher tous les artistes entre le 8 et le 9 octobre 2024
        for event in data['events']:
            artist_name = event.get('artistName', 'Artiste inconnu')
            event_date = event.get('startsAt', 'Date inconnue')
            venue_name = event.get('venueName', 'Lieu inconnu')
            venue_city = event.get('locationText', 'Ville inconnue')
            print(f"Artiste : {artist_name}, Date : {event_date}, Lieu : {venue_name}, {venue_city}")
    else:
        print("Aucun événement trouvé ou données mal formatées.")