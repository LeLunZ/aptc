from dataclasses import dataclass
from typing import Tuple, List

from Models.agency import Agency
from Models.route import Route


@dataclass(repr=False)
class PageDTO:
    agency: Agency
    route: Route
    calendar_data: Tuple
    first_station: str
    dep_time: str
    stop_names: List
    stop_ids: List
    stop_links: List
    page: str


@dataclass
class StopDTO:
    stop_name: str
    stop_url: str
    location_type: int
    stop_id: int

    def __eq__(self, other):
        return self.stop_id == other.stop_id
