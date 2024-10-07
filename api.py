from urllib.request import Request

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/35.0.1916.47 Safari/537.36'
url = "https://www.bandsintown.com/choose-dates/fetch-next/upcomingEvents?date=&page=2&longitude=-0.12574&latitude=51.50853&genre_query=all-genres"

req = Request(url,headers={'User-Agent':user_agent})