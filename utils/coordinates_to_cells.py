import s2sphere

def _limit_coordinates(spatial_range, coordinates):
    """
    Limit the coordinates DataFrame to those falling within the specified spatial range.

    :param spatial_range: A tuple containing the spatial range (N, S, E, W) defining the bounding box.
    :param coordinates: DataFrame containing latitude and longitude coordinates.
    :return: DataFrame containing coordinates within the specified spatial range.
    """
    n, s, e, w = spatial_range
    coordinates = coordinates[(coordinates.lat <= n) & (coordinates.lat >= s) &
                              (coordinates.lon <= e) & (coordinates.lon >= w)]
    return coordinates


def prepare_coordinates(coordinates, spatial_range, level):
    """
    Prepare coordinates for data retrieval, limiting them to the specified spatial range and assigning S2Cell IDs.

    :param spatial_range: A tuple containing the spatial range (N, S, E, W) defining the bounding box.
    :param level: S2Cell level.
    :return: DataFrame containing coordinates within the specified spatial range and their corresponding S2Cell IDs.
    """
    coordinates.lat = coordinates.lat.astype('float32')
    coordinates.lon = coordinates.lon.astype('float32')
    coordinates = _limit_coordinates(spatial_range=spatial_range, coordinates=coordinates)
    coordinates['S2CELL'] = coordinates.apply(lambda x:
                                              s2sphere.CellId.from_lat_lng(
                                                  s2sphere.LatLng.from_degrees(x.lat, x.lon)).parent(level).id(),
                                              axis=1)
    return coordinates