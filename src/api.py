from models import OpenWeatherMapApiConfig, GeolocationRequest, GeolocationResult, WeatherResult, WeatherRequest
from requests import exceptions
import requests


class OpenWeatherMapApi:

    def __init__(self, cfg: OpenWeatherMapApiConfig) -> None:
        if not isinstance(cfg, OpenWeatherMapApiConfig):
            raise TypeError("Type of OpenWeatherMapApiConfig wrong.")
        self.__cfg = cfg

    def get_geolocation(self, request: GeolocationRequest) -> GeolocationResult:
        
        url_base = self.__cfg.base_url
        url_ext = self.__cfg.geo_url_ext
        api_key = self.__cfg.api_key
        
        try:
            uri = str(f"{url_base}{url_ext}?q={request.city},{request.country_code}&limit=1&appid={api_key}")
            result = requests.get(uri)
        except requests.ConnectionError as e:
            raise RuntimeError(e) # DNS Error or refused connection
        except requests.ConnectTimeout as e:
            raise RuntimeError(e) # Request timeout
        except requests.RequestException as e:
            raise RuntimeError(e) # Catch unhandled exceptions of requests library
        try:
            json = result.json()
            if type(json) is dict and not "cod" in json:
                raise ValueError()
            elif type(json) is list and len(json) < 1:
                raise ValueError()
            elif type(json) is list and "name" not in json[0] or not "lat" in json[0] or not "lon" in json[0]:
                raise ValueError()
        except exceptions.JSONDecodeError:
            raise RuntimeError("Can't parse HTTP response into JSON format.")
        except ValueError:
            print(f"HTTP response contains not implemented structure.")
        
        if type(json) is dict:
            return GeolocationResult(json["cod"], "", 0.0, 0.0)
        
        return GeolocationResult(200, json[0]["name"], json[0]["lat"], json[0]["lon"])


    def get_weather(self, request: WeatherRequest) -> WeatherResult:

        url_base = self.__cfg.base_url
        url_ext = self.__cfg.weather_url_ext
        api_key = self.__cfg.api_key
        lat = request.latitude
        lon = request.longitude

        try:
            uri = str(f"{url_base}{url_ext}?lat={lat}&lon={lon}&appid={api_key}&units=metric")
            result = requests.get(uri)
        except requests.ConnectionError as e:
            raise RuntimeError(e) # DNS Error or refused connection
        except requests.ConnectTimeout as e:
            raise RuntimeError(e) # Request timeout
        except requests.RequestException as e:
            raise RuntimeError(e) # Catch unhandled exceptions of requests library

        try:
            json = result.json()
            if type(json) is not dict:
                raise ValueError()
            elif "cod" in json and json["cod"] != 200:
                return WeatherResult(int(json["cod"]), "", "", "")
            elif "name" not in json or "dt" not in json:
                raise ValueError()
        except exceptions.JSONDecodeError:
            raise RuntimeError("Can't parse HTTP response into JSON format.")
        except ValueError:
            raise RuntimeError("HTTP response contains not implemented structure.")

        return WeatherResult(int(json["cod"]), json["name"], json["dt"], json)
