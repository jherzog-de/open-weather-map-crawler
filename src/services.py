import time, logging
from contexts import SQLiteContext
from api import OpenWeatherMapApi
from models import WeatherRequest, WeatherStation, GeolocationRequest, GeolocationResult
from datetime import datetime
from prometheus_client import Histogram, Gauge, start_http_server


def create_station(db: SQLiteContext, api: OpenWeatherMapApi, city: str, country: str) -> WeatherStation | None:

    result = db.get_station(city, country)
    
    if result is not None:
        return result
            
    # Retry delay increment between requests in seconds
    retry_delay_increment = 120

    # Current configured request delay
    process_delay = 0

    # Internal request retry counter
    retry_counter = 1

    # Maximum amount of request retries before abort cycle
    max_retries = 3

    # Geolocation object
    geolocation = None

    while geolocation is None and retry_counter < max_retries:
        try: 
            location = api.get_geolocation(GeolocationRequest(city, country))
            if location.code != 200:
                retry_counter += 1
                process_delay += retry_delay_increment
                time.sleep(process_delay)
                raise RuntimeError()
            geolocation = location    
        except ValueError:
            logging.error("Not implemented response structure for Geolocation API.")
            return None
        except RuntimeError:
            logging.error(f"Failed to fetch geolocation information from API for {city},{country}.")
            retry_counter += 1
            process_delay += retry_delay_increment
            time.sleep(process_delay)
    
    if not isinstance(geolocation, GeolocationResult):
        return None
    
    db.insert_station(city, country, geolocation.latitude, geolocation.longitude)
    return db.get_station(city, country)
    

class OpenWeatherMapCrawler:

    __api_letency_histogram = Histogram(
        "api_response_time_last",
        "Weather API response time in seconds of last request"
    )

    __process_latency_histogram = Histogram(
        "owmc_proc_duration",
        "OpenWeatherMap Crawler process cycle time"
    )

    __last_api_call = Gauge(
        "api_last_call",
        "Weather API last call timestamp",
        labelnames=["station"]
    )

    __last_api_data = Gauge(
        "owmc_new_data_from_api_tstamp",
        "OpenWeatherMap API latest weather data timestamp",
        labelnames=["station"]
    )

    __prometheus_initialized : bool = False


    def __init__(self, station: WeatherStation, db: SQLiteContext, api: OpenWeatherMapApi) -> None:
        self.__station_id : int = station.id
        self.__station_name : str = station.name
        self.__station_country : str = station.country
        self.__station_geolocation_latitude : float = station.latitude
        self.__station_geolocation_longitude : float = station.longitude
        self.__service_cancelled : bool = False
        self.__last_timestamp : datetime = datetime.min
        self.__database_context : SQLiteContext = db
        self.__api : OpenWeatherMapApi = api
        
        if not OpenWeatherMapCrawler.__prometheus_initialized:
            start_http_server(8080)
            OpenWeatherMapCrawler.__prometheus_initialized = True
    
    def cancel(self) -> None:
        self.__service_cancelled = True
    
    def run(self) -> int:
        
        logging.info((f"OpenWeatherMap Crawler monitoring for station "
                      f"{self.__station_name},{self.__station_country} started."))
        logging.info((f"Searching for already existing measurements in database for station " 
                     f"{self.__station_name},{self.__station_country}."))

        latest_measurement = self.__database_context.get_latest_measurement(self.__station_id)
        if latest_measurement is not None:
            self.__last_timestamp = latest_measurement.tstamp
            logging.info((f"Measurement for station "
                          f"{self.__station_name},{self.__station_country} with timestamp "
                          f"{self.__last_timestamp} available."))
        
        weather_request = WeatherRequest(
            longitude=self.__station_geolocation_longitude,
            latitude=self.__station_geolocation_latitude
        )

        # Observe weather station measurements every 10 minutes
        monitoring_interval_sec = 60 * 10

        # Basic retry delay between requests in seconds
        basic_retry_delay = 60
        
        # Retry delay increment between requests in seconds
        retry_delay_increment = 120

        # Current configured request delay
        process_delay = monitoring_interval_sec

        # Internal request retry counter
        retry_counter = 1

        # Maximum amount of request retries before abort cycle
        max_retries = 3

        while not self.__service_cancelled:
            
            logging.info((f"Starting {retry_counter}/{max_retries} measurement request for station " 
                         f"{self.__station_name},{self.__station_country}."))

            start = time.time()
            measurement = self.__api.get_weather(weather_request)
            duration = time.time() - start
            self.__api_letency_histogram.observe(duration)

            # --- Request was successfull
            if measurement.code == 200:
                process_delay = monitoring_interval_sec
                self.__last_api_call.labels(station=self.__station_name).set(time.time())
            
            # --- Request rejected because of invalid API key
            elif measurement.code == 401:
                logging.info(f"Bad authorization - Check API key")
                self.__service_cancelled = True
                break

            # --- Request failed - retry!
            elif retry_counter < max_retries:
                logging.info((f"Failed to fetch weather informations for station "
                              f"{self.__station_name},{self.__station_country} "
                              f"with http error code: {measurement.code}"))
                process_delay = basic_retry_delay if retry_counter == 1 else process_delay + retry_delay_increment
                retry_counter += 1
            
            # --- Maximum amount of retries reached - Reset cycle
            elif retry_counter >= max_retries:
                process_delay = monitoring_interval_sec
                retry_counter = 1
                logging.info((f"Maximum amount of request retries reached. Aborting current cycle for station " 
                             f"{self.__station_name},{self.__station_country}."))
            
            # --- Insert new measurement into database
            if measurement.code == 200 and self.__last_timestamp < datetime.fromtimestamp(measurement.timestamp):
                self.__last_api_data.labels(station=self.__station_name).set(time.time())
                self.__database_context.insert_measurement(
                    self.__station_id,
                    datetime.fromtimestamp(measurement.timestamp),
                    measurement.payload
                )
                self.__last_timestamp = datetime.fromtimestamp(measurement.timestamp)
            
            duration = time.time() - start
            self.__process_latency_histogram.observe(duration)
            time.sleep(process_delay)


        return 0x00
