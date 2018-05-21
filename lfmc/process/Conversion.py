import json

import fiona
import fiona.crs
from fiona.crs import from_epsg
import sys
import logging
from pyproj import Proj, transform

from pathlib2 import Path

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class Conversion:

    @staticmethod
    def convert(shp):

        # Translate the path for this Docker Container's Volume context
        shp = shp.replace('/mnt/queries/', '/FuelModels/queries/')

        geoj = shp.replace('.shp', '.json')

        if Path(geoj).is_file():
            contents = open(geoj, 'r')
            return json.loads(contents.read())

        if not Path(shp).is_file():
            raise FileNotFoundError("File not found.")
        else:
            with fiona.open(shp, 'r') as source:
                original = Proj(source.crs)
                destination = Proj(init='EPSG:4326')
                with fiona.open(
                        geoj,
                        'w',
                        driver='GeoJSON',
                        crs=fiona.crs.from_epsg(4326),
                        schema=source.schema.copy()) as sink:
                    for feat in source:
                        # sink.write(feat)
                        out_linear_ring = []
                        for point in feat['geometry']['coordinates'][0]:
                            long, lat = point
                            x, y = transform(original, destination, long, lat)
                            out_linear_ring.append((x, y))
                        feat['geometry']['coordinates'] = [out_linear_ring]
                        sink.write(feat)
        # Remove the default WGS84 crs to avoid GeoJSON import error on UI
        return geoj.replace('"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },', '')


if __name__ == '__main__':
    Conversion.convert(sys.argv[1])
