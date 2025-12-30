from threading import Lock
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from models import WeatherMeasurement, WeatherStation, Base
from datetime import datetime
from typing import List


class SQLiteContext:

    def __init__(self, path: str) -> None:
        self.__engine = create_engine(
            f"sqlite:///{path}",
            connect_args = {"autocommit": False}
        )
        self.__mutex = Lock()
        Base.metadata.create_all(self.__engine)
        self.__session = sessionmaker(bind=self.__engine)()

    def insert_measurement(self, station: int, tstamp: datetime, attr: dict) -> None:
        measurement = WeatherMeasurement(
            station_id=station,
            tstamp=tstamp,
            attr=attr
        )
        self.__mutex.acquire()
        self.__session.add(measurement)
        self.__session.commit()
        self.__mutex.release()

    def insert_station(self, name: str, country_code: str, lat: float, lon: float) -> None:
        self.__mutex.acquire()
        existing_station = self.__session.query(WeatherStation).filter_by(name=name, country=country_code).first()
        if not existing_station:
            station = WeatherStation(name=name, country=country_code, latitude=lat, longitude=lon)
            self.__session.add(station)
            self.__session.commit()
        self.__mutex.release()

    def get_station(self, name: str, country: str) -> WeatherStation | None:
        self.__mutex.acquire()
        result = self.__session.query(WeatherStation).filter_by(name=name, country=country).first()
        self.__mutex.release()
        return result

    def get_all_stations(self) -> List[WeatherStation]:
        self.__mutex.acquire()
        result = self.__session.query(WeatherStation).all()
        self.__mutex.release()
        return result

    def get_location_by_station_id(self, station_id: int) -> tuple[float, float] | None:
        self.__mutex.acquire()
        station = self.__session.query(WeatherStation).filter_by(id=station_id).first()
        if not station:
            self.__mutex.release()
            return None
        self.__mutex.release()
        return float(station.latitude), float(station.latitude)

    def get_latest_measurement(self, station: int) -> WeatherMeasurement | None:
        self.__mutex.acquire()
        measurement = self.__session.query(WeatherMeasurement).filter_by(station_id=station).order_by(
            desc(WeatherMeasurement.tstamp))
        if measurement is not None:
            measurement = measurement.first()
        self.__mutex.release()
        return measurement
