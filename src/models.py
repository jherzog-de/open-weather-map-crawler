from sqlalchemy.orm import declarative_base, relationship, mapped_column, Mapped
from sqlalchemy import Integer, String, JSON, TIMESTAMP, PrimaryKeyConstraint, ForeignKey
from dataclasses import dataclass, field
from sqlalchemy.sql.sqltypes import Float

Base = declarative_base()


class WeatherStation(Base):

    __tablename__ = 'weather_stations'

    id : Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    name : Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    country : Mapped[str] = mapped_column(String(4), nullable=False, default="de")

    latitude : Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    longitude : Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    entries = relationship("WeatherMeasurement", back_populates="station")

    def __repr__(self) -> str:
        return f"<WeatherStation({self.id}, name={self.name}, country={self.country})>"


class WeatherMeasurement(Base):

    __tablename__ = 'weather_measurements'

    tstamp = mapped_column(TIMESTAMP, nullable=False)

    station_id = mapped_column(Integer, ForeignKey('weather_stations.id'), nullable=False)

    attr = mapped_column(JSON, nullable=False)

    station = relationship("WeatherStation", back_populates="entries")

    __table_args__ = (
        PrimaryKeyConstraint('tstamp', 'station_id'),
    )

    def __repr__(self) -> str:
        return f"<Weather(tstamp={self.tstamp}, station={self.station_id}, attr={self.attr})>"


@dataclass
class BaseHttpResult:
    code : int = 500

@dataclass
class GeolocationResult(BaseHttpResult):
    station_name : str = ""
    latitude : float = 0.0
    longitude : float = 0.0

@dataclass
class WeatherRequest:
    longitude : float = 0.0
    latitude : float = 0.0

@dataclass
class WeatherResult(BaseHttpResult):
    station_name : str = ""
    timestamp : int = 0
    payload : dict = field(default_factory=dict)

@dataclass
class GeolocationRequest:
    city : str = ""
    country_code : str = ""

@dataclass
class OpenWeatherMapApiConfig:
    api_key: str = ""
    base_url: str = ""
    geo_url_ext : str = ""
    weather_url_ext : str = ""
