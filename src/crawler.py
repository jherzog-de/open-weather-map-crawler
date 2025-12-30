from contexts import SQLiteContext
from api import OpenWeatherMapApi, OpenWeatherMapApiConfig
from services import OpenWeatherMapCrawler, create_station
from typing import List
from threading import Thread
from models import WeatherStation
import configparser, json, logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CFG = configparser.ConfigParser()
CFG.read("data/config.ini")

# ------------------------------------------------------------------------------------------------------------------- #
# API endpoint configuration
# ------------------------------------------------------------------------------------------------------------------- #
API_CONFIG = OpenWeatherMapApiConfig()
API_CONFIG.api_key = CFG['api']['api_key']
API_CONFIG.base_url = CFG['api']['base_url']
API_CONFIG.geo_url_ext = CFG['api']['geo_url_ext']
API_CONFIG.weather_url_ext = CFG['api']['weather_url_ext']

API = OpenWeatherMapApi(API_CONFIG)

DB = SQLiteContext(CFG['db']['path'])

def init() -> List[WeatherStation]:
    with open("data/stations.json", "r") as f:
        app_stations = json.load(f)
    if app_stations is None:
        logging.error(f"No stations provided in 'stations.json'.")
        exit(-1)
    st : List[WeatherStation] = []
    for app_station in app_stations:
        station = create_station(DB, API, app_station['city'], app_station['location'])
        if station is None:
            raise RuntimeError(f"OpenWeatherMap Crawler stopped because of runtime error occurred.")
        st.append(station)
    return st

def main() -> None:
    stations = init()
    crawlers = []
    threads = []

    for station in stations:
        crawlers.append(OpenWeatherMapCrawler(station, DB, API))
        th = Thread(target=crawlers[-1].run, daemon=True)
        threads.append(th)
    for th in threads:
        th.start()
    for th in threads:
        th.join()


if __name__ == "__main__":  
    main()
    exit(0)
