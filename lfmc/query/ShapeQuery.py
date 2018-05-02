from lfmc.query.Query import Query, QuerySchema
from marshmallow import Schema, fields
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.query.TemporalQuery import TemporalQuery, TemporalQuerySchema
from lfmc.query.SpatialQuery import SpatialQuery, SpatialQuerySchema

import geopandas as gp
import numpy as np
import pandas as pd
import xarray as xr
import regionmask

import json
import glob
import time
import cv2

from numpy import asarray
from scipy.spatial import ConvexHull

import cartopy.feature as cfeature
import cartopy.crs as ccrs

import shapely
from shapely.wkt import dumps, loads
from shapely.geometry import Polygon, mapping, shape
from shapely import affinity

from affine import Affine

import fiona
from fiona.crs import from_epsg

import rasterio
import rasterio.mask
from rasterio import features
from rasterio.features import rasterize
import logging
logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class ShapeQuery(SpatioTemporalQuery):

    def __init__(self, start, finish, geo_json: json, weighted=False):

        self.weighted = weighted
        self.geo_json = geo_json

        logger.debug(geo_json["type"])

        # A regionmask Object
        # A list of the Shapely.Polygons
        selections = list()
        count = 0
        numbers = []
        names = []
        abbrevs = []
        if geo_json["type"] == "FeatureCollection":
            logger.debug("Found a feature collection...")
            for p in geo_json["features"]:

                logger.debug("Found a Feature.")
                if p["geometry"]["type"] == "Polygon":
                    logger.debug("Found Polygon #%s" % count)
                    points = p["geometry"]["coordinates"]
                    s = shapely.geometry.Polygon(*points)
                    selections.append(s)
                    numbers.append(count)
                    names.append("Selection_%s" % count)
                    abbrevs.append("SEL_%s" % count)

                    count += 1
        # else:
        #     if geo_json["geometry"]["type"] is "Polygon":
        #         points = geo_json["geometry"]["coordinates"]
        #         s = shapely.geometry.Polygon(*points)
        #         selections.append(s)
        #
        #         numbers.append(count)
        #         if geo_json["properties"] == {}:
        #             names.append("Selection_%s" % count)
        #             abbrevs.append("SEL_%s" % count)
        #         else:
        #             names.append(geo_json["properties"]["FIRENAME"])
        #             abbrevs.append("SEL_%s" % count)
        #         count += 1

        logger.debug("Making Region Mask with %s Polygons." % count)
        logger.debug("numbers: %s" % numbers)
        logger.debug("names: %s" % names)
        logger.debug("abbrevs: %s" % abbrevs)
        logger.debug("selections: %s" % selections)

        self.rmask = regionmask.Regions_cls(
            0, numbers, names, abbrevs, selections)

        self.selections = selections
        logger.debug(["%s" % sel for sel in selections])

        # Do once and store
        self.mask = self.get_super_sampled_mask()

        hull = ShapeQuery.get_query_hull(self.rmask)
        lat1, lon2, lat2, lon1 = hull.bounds

        logger.debug(lat1, lon1, lat2, lon2)

        self.spatio_temporal_query = SpatioTemporalQuery(
            lat1, lon1, lat2, lon2, start, finish)
        self.temporal = self.spatio_temporal_query.temporal
        self.spatial = self.spatio_temporal_query.spatial

    def get_selections(self):
        return self.selections

    def weighted(self):
        return self.weighted

    def spatio_temporal_query(self):
        return self.spatio_temporal_query

    def geo_json(self):
        return self.geo_json

    @staticmethod
    def get_bbox(poly: shapely.geometry.Polygon):
        return list((poly.bounds[0], poly.bounds[2], poly.bounds[1], poly.bounds[3]))

    @staticmethod
    def get_query_hull(regionmask):
        points = np.concatenate(regionmask.coords, axis=0)

        hull = ConvexHull(points)

        return shapely.geometry.Polygon([hull.points[vertex] for vertex in hull.vertices])

    @staticmethod
    def get_corners(poly: shapely.geometry.Polygon):
        bb = bounds_to_bbox(poly)
        return [(bb[0], bb[2]), (bb[0], bb[3]), (bb[1], bb[3]), (bb[1], bb[2])]

    @staticmethod
    def get_buffered_coords(poly, kilometers):
        return transform_to_meters(poly).buffer(kilometers)

    @staticmethod
    def transform_to_meters(poly):
        new_points = [~affine * (point) for point in asarray(poly.exterior)]
    #     print(new_points)
        return shapely.geometry.Polygon(new_points)

    @staticmethod
    def transform_to_latlong(poly):
        return shapely.geometry.Polygon([affine * (point) for point in asarray(poly.exterior)])

    @staticmethod
    def as_buffered(poly, kilometers):
        return transform_to_latlong(get_buffered_coords(poly, kilometers))

    # def transform_selection(regionmask_obj):
    #     aff = Affine(0.05, 0.0, 111.975, 0.0, -0.05, -9.974999999999994)
    #     points = np.concatenate(regionmask_obj.coords, axis=0)
    #     all_points = [(u,v) for [u,v] in points]
    #     _pixel_coords = [~aff * (coord) for coord in all_points]
    #     return _pixel_coords

    def get_super_sampled_mask(self, scaled_transform=[0.005, 0.0, 111.975, 0.0, -0.005, -9.974999999999994], out_shape=(886, 691)):
        rparams = dict(
            transform=scaled_transform,
            out_shape=(10 * out_shape[1], 10 * out_shape[0])
        )

        # selection rasterization using rasterio!
        raster = rasterize(self.selections, **rparams)
        # Use OpenCV to interpolate back to the correct dimensions
        # This effectively 'super-samples' the mask
        resized = cv2.resize(raster.astype(float), out_shape,
                             interpolation=cv2.INTER_AREA)
        # Greyscale channel
        resized = np.array(np.asarray(resized) * 255 + 0.5, np.uint8)
        return resized

    def apply_mask_to(self, result_cube: xr.DataArray) -> xr.DataArray:

        if self.weighted:
            fuel_moistures = result_cube.where(
                self.mask != 0) * self.mask / 255
        else:
            # Binary Masking
            mask = self.rmask.mask(
                result_cube['longitude'], result_cube['latitude'], xarray=True)
            mask = mask.rename({'lat': 'latitude', 'lon': 'longitude'})
            region = np.ma.filled(mask.astype(float), np.nan)

            # Set all values in the mask layer to either zero or NaN
            # Otherwise the selection indexes will determine the results
            region = region * 0

            # Can then use WHERE to get Binary True/False masking for all parts
            # of the selection as a unified group...
            fuel_moistures = result_cube.where(region == 0)

        logger.debug("Masked: ")
        logger.debug(fuel_moistures)

        return fuel_moistures
