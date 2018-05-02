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


class ShapeQuery(SpatioTemporalQuery):

    def __init__(self, spatio_temporal_query: SpatioTemporalQuery, geo_json: json, weighted=False):
        self.weighted = weighted
        self.spatio_temporal_query = spatio_temporal_query

        # A regionmask Object
        self.rmask = get_regionmask_from_geojson(geo_json)

        # A list of the Shapely.Polygons
        self.selections = list()

        # Do once and store
        self.mask = self.get_super_sampled_mask()

    def weighted(self):
        return self.weighted

    def spatio_temporal_query(self):
        return self.spatio_temporal_query

    def geo_json(self):
        return self.geo_json

    def get_regionmask_from_geojson(self):
        # Convenience
        gj = self.geo_json

        count = 0
        numbers = []
        names = []
        abbrevs = []
        if gj["type"] == "FeatureCollection":
            for p in gj["features"]:
                if p["geometry"]["type"] is "Polygon":
                    points = p["geometry"]["coordinates"]
                    s = shapely.geometry.Polygon(*points)
                    self.selections.append(s)

                    numbers.append(count)
                    if p["properties"] == {}:
                        names.append("Selection_%s" % count)
                        abbrevs.append("SEL_%s" % count)

                    count += 1
        else:
            if gj["geometry"]["type"] is "Polygon":
                points = gj["geometry"]["coordinates"]
                s = shapely.geometry.Polygon(*points)
                self.selections.append(s)

                numbers.append(count)
                if gj["properties"] == {}:
                    names.append("Selection_%s" % count)
                    abbrevs.append("SEL_%s" % count)
                else:
                    names.append(gj["properties"]["FIRENAME"])
                    abbrevs.append("SEL_%s" % count)
                count += 1

        return regionmask.Regions_cls('Selections', numbers, names, abbrevs, self.selections)

    @staticmethod
    def get_bbox(poly: shapely.geometry.Polygon) -> Tuple:
        return list((poly.bounds[0], poly.bounds[2], poly.bounds[1], poly.bounds[3]))

    @staticmethod
    def get_query_hull(regionmask):
        points = np.concatenate(regionmask.coords, axis=0)
        return ConvexHull(points)

    @staticmethod
    def get_corners(poly: shapely.geometry.Polygon):
        bb = bounds_to_bbox(poly)
        return [(bb[0], bb[2]), (bb[0], bb[3]), (bb[1], bb[3]), (bb[1], bb[2])]

    def get_buffered_coords(poly, kilometers):
        return transform_to_meters(poly).buffer(kilometers)

    def transform_to_meters(poly):
        new_points = [~affine * (point) for point in asarray(poly.exterior)]
    #     print(new_points)
        return shapely.geometry.Polygon(new_points)

    def transform_to_latlong(poly):
        return shapely.geometry.Polygon([affine * (point) for point in asarray(poly.exterior)])

    def as_buffered(poly, kilometers):
        return transform_to_latlong(get_buffered_coords(poly, kilometers))

    # def transform_selection(regionmask_obj):
    #     aff = Affine(0.05, 0.0, 111.975, 0.0, -0.05, -9.974999999999994)
    #     points = np.concatenate(regionmask_obj.coords, axis=0)
    #     all_points = [(u,v) for [u,v] in points]
    #     _pixel_coords = [~aff * (coord) for coord in all_points]
    #     return _pixel_coords

    def get_super_sampled_mask(scaled_transform=[0.005, 0.0, 111.975, 0.0, -0.005, -9.974999999999994], out_shape=(886, 691)):
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
            mask = regionmask_obj.mask(
                result_cube['longitude'], result_cube['latitude'], xarray=True)
            mask = mask.rename({'lat': 'latitude', 'lon': 'longitude'})
            region = np.ma.filled(mask.astype(float), np.nan)

            # Set all values in the mask layer to either zero or NaN
            # Otherwise the selection indexes will determine the results
            region = region * 0

            # Can then use WHERE to get Binary True/False masking for all parts
            # of the selection as a unified group...
            fuel_moistures = result_cube.where(self.mask == 0)

        return fuel_moistures
