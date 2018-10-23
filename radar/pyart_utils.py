from collections import namedtuple
import math

import numpy as np
import pyart
import pandas as pd
from scipy.interpolate import griddata

  
# maximum DBZH -- above this, definitely not birds
MAX_DBZH = 35

# maximum correlation coefficient -- above this,  
# most likely clouds
MAX_RHOV = .95

# we take the complement of this interval
BAD_VELOCITY_WINDOW = (-1, 1)

# interpolation scale (in meters)
INTERP_SCALE = 100

min_bad_v, max_bad_v = BAD_VELOCITY_WINDOW

def gen_radar_df(radar):
    
    lats = radar.gate_latitude['data']
    lons = radar.gate_longitude['data']
    reflects = radar.fields['reflectivity']['data']
    rhovs = radar.fields['cross_correlation_ratio']['data']
    velocities = radar.fields['velocity']['data']
    alts = radar.gate_altitude['data']
    xs = radar.gate_x['data']
    ys = radar.gate_y['data']
    zs = radar.gate_z['data']
    ends = (radar.sweep_end_ray_index['data'] + 1) * radar.ngates
    starts = radar.sweep_start_ray_index['data'] * radar.ngates
    sweep_indices = np.zeros(len(lons.reshape(-1)))
    for i, (a, b) in enumerate(zip(starts, ends)):
        sweep_indices[a:b] = i
        
    radar_df = pd.DataFrame({"lon": lons.reshape(-1), 
                             "lat": lats.reshape(-1),
                             "alt": alts.reshape(-1),
                             "x": xs.reshape(-1),
                             "y": ys.reshape(-1),
                             "z": zs.reshape(-1),
                             "dbzh": reflects.reshape(-1),
                             "rhov": rhovs.reshape(-1),
                             "velocity": velocities.reshape(-1),
                             "sweep": sweep_indices
                             })
    return radar_df


RadarResult = namedtuple('RadarResult', ['dbzh', 'x', 'y', 'lon', 'lat', 'radar_x', 'radar_y'])
def load_filter_dbzh(radar_f, filter_dbzh=True,
                     filter_rhov=True, filter_v=True, return_radar=False):
    """
        params: 
            radar_f: The file to load.
            filter_dbzh: Whether observations with DBZH {max_dbzh} 
                         should be set to zero. (default True)
            filter_rhov: Whether observations with RHOV > {max_rhov} 
                         should be set to zero. (default True)
            filter_v: Whether observations with radial velocity within
                      the interval {bad_velocity} should be set to zero.
                      (default True)
            return_radar: Whether to return the raw radar object. (default False)
    """.format(max_dbzh=MAX_DBZH, max_rhov=MAX_RHOV, 
               bad_velocity=BAD_VELOCITY_WINDOW)
    
    
    radar = pyart.io.read_nexrad_archive(radar_f).extract_sweeps([0, 1])
    
    reflects = radar.get_field( 0, 'reflectivity')
    # nothing back from the radar? use 0 as the value, which is good
    reflects.set_fill_value(0)
    
    
    # TODO: is 0 the correct null value here?
    if filter_rhov:
        rhovs = radar.get_field(0, 'cross_correlation_ratio')
        _filter_rhovs(reflects, rhovs)
    if filter_v:
        velocities = radar.get_field(0, 'velocity')
        _filter_v(reflects, velocities)
    if filter_dbzh:
        _filter_dbzh(reflects) 
    
    x, y, z = radar.get_gate_x_y_z(0)
    radar_x = radar.longitude['data']
    radar_y = radar.latitude['data']
    lon, lat = pyart.core.transforms.cartesian_to_geographic_aeqd(x, y, 
                                                                  radar_x, 
                                                                  radar_y)
    
    if return_radar:
        return RadarResult(reflects, x, y, lon, lat, radar_x, radar_y), radar
    else:
        return RadarResult(reflects, x, y, lon, lat, radar_x, radar_y)

def _filter_rhovs(reflects, rhovs):
    reflects[rhovs > MAX_RHOV] = 0.

def _filter_dbzh(reflects):
    reflects[reflects > MAX_DBZH] = 0.

def _filter_v(reflects, velocities):
    reflects[(min_bad_v < velocities) & (velocities < max_bad_v)] = 0.


MN_INIT_CUT = {'lat_min': 40.68, 
               'lat_max': 40.85,
               'lon_min': -74.05,
               'lon_max': -73.85}

LOWER_MN = {'lat_min': 40.696417,
            'lat_max': 40.766791,
            'lon_min': -74.022494,
            'lon_max': -73.959454}

def interp_radar_values(res, init_bounds, tight_bounds):
    
    values = res.dbzh.reshape(-1)
    # set up space to interpolate onto
    lon_max, lon_min = init_bounds['lon_max'], init_bounds['lon_min']
    lat_max, lat_min = init_bounds['lat_max'], init_bounds['lat_min']
    lon, lat = res.lon, res.lat
    filt = (lon >= lon_min) & (lon <= lon_max) & (lat >= lat_min) & (lat <= lat_max)
    x = np.extract(filt, res.x)
    y = np.extract(filt, res.y)
    values = np.extract(filt, values)
    # set up space to interpolate onto
    lon_max, lon_min = tight_bounds['lon_max'], tight_bounds['lon_min']
    lat_max, lat_min = tight_bounds['lat_max'], tight_bounds['lat_min']
    filt = (lon >= lon_min) & (lon <= lon_max) & (lat >= lat_min) & (lat <= lat_max)
    tight_x = np.extract(filt, res.x)
    tight_y = np.extract(filt, res.y)
    tight_xmin, tight_xmax = tight_x.min(), tight_x.max()
    tight_ymin, tight_ymax = tight_y.min(), tight_y.max()
    interp_x = np.linspace(tight_xmin, tight_xmax, math.floor((tight_xmax - tight_xmin) / INTERP_SCALE))
    interp_y = np.linspace(tight_ymin, tight_ymax, math.floor((tight_ymax - tight_ymin) / INTERP_SCALE))
    xx, yy = np.meshgrid(interp_x, interp_y)
    
    result = griddata((x, y),
                      values.filled(0),
                      (xx, yy))
    transformed = pyart.core.transforms.cartesian_to_geographic_aeqd(xx, yy, 
                                                                     res.radar_x, 
                                                                     res.radar_y)
    return result, transformed, xx, yy



#AR2V_FILE = "data/09/29/KOKX20170929_000041_V06.ar2v"
AR2V_FILE = 'data/09/01/KOKX20170901_000243_V06.ar2v'
def test_interp_scales():
    """
    Confirms the interpolator works equally well on x/y (m) vs. lat/long (deg)
    """
    res = load_filter_dbzh(AR2V_FILE, False, False, False)
    interp1 = interp_radar_values(res.dbzh, res.x, res.y)
    interp2 = interp_radar_values(res.dbzh, res.lon, res.lat)
    assert np.all(interp1 == interp2)

# careful, this test is resource-intensive
def test_sweep_indices():
    radar, df = load_radar_file(AR2V_FILE)
    for i in range(radar.nsweeps):
        reflects = radar.get_field(i, 'reflectivity')
        assert np.all(df[df.sweep == i].dbzh.values.reshape(reflects.shape) == reflects)
