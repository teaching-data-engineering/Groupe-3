import requests

url = "https://www.bandsintown.com/choose-dates/fetch-next/upcomingEvents?date=&page=2&longitude=-0.12574&latitude=51.50853&genre_query=all-genres"

req = Request(url,headers={'User-Agent':user_agent})