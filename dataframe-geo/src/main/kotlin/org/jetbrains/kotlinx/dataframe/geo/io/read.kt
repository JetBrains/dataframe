package org.jetbrains.kotlinx.dataframe.geo.io

import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.geojson.feature.FeatureJSON
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.geo.GeoDataFrame
import org.jetbrains.kotlinx.dataframe.geo.geotools.toGeoDataFrame
import org.jetbrains.kotlinx.dataframe.io.asURL
import java.net.URL

fun GeoDataFrame.Companion.readGeoJson(path: String): GeoDataFrame<*> = readGeoJson(asURL(path))

fun GeoDataFrame.Companion.readGeoJson(url: URL): GeoDataFrame<*> =
    (FeatureJSON().readFeatureCollection(url.openStream()) as SimpleFeatureCollection).toGeoDataFrame()

fun DataFrame.Companion.readGeoJson(path: String): GeoDataFrame<*> = GeoDataFrame.readGeoJson(path)

fun DataFrame.Companion.readGeoJson(url: URL): GeoDataFrame<*> = GeoDataFrame.readGeoJson(url)

fun GeoDataFrame.Companion.readShapefile(path: String): GeoDataFrame<*> = readShapefile(asURL(path))

fun GeoDataFrame.Companion.readShapefile(url: URL): GeoDataFrame<*> =
    ShapefileDataStoreFactory().createDataStore(url).featureSource.features.toGeoDataFrame()

fun DataFrame.Companion.readShapefile(path: String): GeoDataFrame<*> = GeoDataFrame.readShapefile(path)

fun DataFrame.Companion.readShapefile(url: URL): GeoDataFrame<*> = GeoDataFrame.readShapefile(url)
