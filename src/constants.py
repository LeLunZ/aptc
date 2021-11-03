import logging
from pathlib import Path

from Functions.config import getConfig

logger = logging.getLogger(__name__)

fiona_geometry = None
fiona_available: bool = False
fiona_geometry_available: bool = False
try:
    import fiona
    from shapely.geometry import shape

    fiona_available = True
except (ImportError, AttributeError):
    logger.debug('Gdal not installed. Shapefile wont work')

data_path = Path('../Data')
shapes_path = Path('../shapes')
db_path = Path('../db')
logs_path = Path('../logs')

pickle_path = data_path / 'pickle'

config_path = data_path / 'config.json'
state_path = pickle_path / 'state.pkl'
chrome_driver_path = Path('../chromedriver')

crawlStopOptions = None

southLatBorder: float = None
northLatBorder: float = None
westLonBorder: float = None
eastLonBorder: float = None

try:
    crawlStopOptions = 'crawlStopOptions' in getConfig()
    if fiona_available:
        try:
            shapefile = getConfig('crawlStopOptions.shapefile')
            fiona_shape = fiona.open(str(shapes_path / shapefile))
            fiona_iteration = iter(fiona_shape)
            fiona_geometry = []
            for r in fiona_iteration:
                fiona_geometry.append(shape(r['geometry']))
            del fiona_shape
            del fiona_iteration
            fiona_geometry_available = len(fiona_geometry) > 0
        except (KeyError, FileNotFoundError, Exception) as e:
            logger.exception(e)

    if crawlStopOptions:
        northLatBorder = getConfig('crawlStopOptions.northLatBorder')
        southLatBorder = getConfig('crawlStopOptions.southLatBorder')
        westLonBorder = getConfig('crawlStopOptions.westLonBorder')
        eastLonBorder = getConfig('crawlStopOptions.eastLonBorder')
except KeyError as e:
    crawlStopOptions = False


def stop_is_to_crawl_geometry(stop_to_check) -> bool:
    if (not fiona_geometry_available and not crawlStopOptions) or (
            crawlStopOptions and southLatBorder < stop_to_check.stop_lat < northLatBorder and westLonBorder < stop_to_check.stop_lon < eastLonBorder):
        return True
    elif fiona_geometry_available:
        point = shape({'type': 'Point', 'coordinates': [stop_to_check.stop_lon, stop_to_check.stop_lat]})
        for k in fiona_geometry:
            if point.within(k):
                return True
    return False
