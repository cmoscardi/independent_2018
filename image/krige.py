import sys
sys.path.append("../radar")
import pyart_utils as pu
rad = pu.load_filter_dbzh("../radar/data/2017-10/01/KOKX20171001_000015_V06.ar2v", False, False, False, False)

_, _, xx, yy, transformed = pu.get_tight_bounds(pu.LOWER_MN, rad)
interp_x, interp_y = transformed
xmin, xmax = interp_x.min(), interp_x.max()
ymin, ymax = interp_y.min(), interp_y.max()
get_filt = lambda df: (df.geometry.centroid.x > xmin) & (df.geometry.centroid.x < xmax)\
                    & (df.geometry.centroid.y > ymin) & (df.geometry.centroid.y < ymax)

import joblib
import numpy as np
import geopandas as gpd
import pandas as pd
import pykrige.kriging_tools as kt
from pykrige.ok import OrdinaryKriging
from pykrige.uk import UniversalKriging


try:
    d6_bbls = np.load("data/bimg_labels_d6.npy")
    d9_bbls = np.load("data/bimg_labels_d9.npy")

    # we will impute using surface area as our distance metric
    pluto = gpd.read_file("data/pluto/MNMapPLUTO.shp")
except FileNotFoundError:
    d6_bbls = np.load("../image/data/bimg_labels_d6.npy")
    d9_bbls = np.load("../image/data/bimg_labels_d9.npy")

    # we will impute using surface area as our distance metric
    pluto = gpd.read_file("../image/data/pluto/MNMapPLUTO.shp")
pluto["surface_area"] = pluto["NumFloors"] * pluto.geometry.length

in_frame = pluto[(pluto.BBL.isin(np.unique(d6_bbls.ravel()))) | pluto.BBL.isin(np.unique(d9_bbls.ravel()))].to_crs(epsg=4326)
out_frame = pluto[(pluto.geometry.centroid.y < 215000) & ~pluto.index.isin(in_frame.index)]

out_reset = out_frame.reset_index().to_crs(epsg=4326)
nearest_neighbors = [np.abs(in_frame.surface_area - osa).sort_values()[:10].index\
                     for osa in out_frame.surface_area]
out_reset["nearest_ix"] = nearest_neighbors


def krige(means):
    means_f = means.to_frame()
    in_frame_bgt = in_frame.merge(means_f, left_on='BBL', right_index=True)
    in_frame_bgt['brightness'] = in_frame_bgt[means_f.columns[0]]
    in_frame_bgt['scaled_bgt'] = in_frame_bgt['brightness'] * in_frame_bgt['surface_area']
    medians = []
    for i, indices in enumerate(out_reset["nearest_ix"]):
        cbbl = in_frame.loc[indices].BBL
        median = means_f.loc[cbbl].median()
        medians.append((i, median))
    medians = np.array(medians, dtype=[('ix', int), ('median', np.float64)])
    med_ix = pd.Series(medians['median'], index=out_reset.index)
    out_bgt = med_ix
    out_scaled_bgt = out_bgt * out_reset['surface_area']
    in_considered = in_frame_bgt[get_filt(in_frame_bgt)]
    out_considered = out_scaled_bgt[get_filt(out_reset)]
    out_reset_considered = out_reset[get_filt(out_reset)]
    full_x = np.concatenate((in_considered.geometry.centroid.x.values, out_reset_considered.geometry.centroid.x.values))
    full_y = np.concatenate((in_considered.geometry.centroid.y.values, out_reset_considered.geometry.centroid.y.values))
    full_z = np.concatenate((in_considered.scaled_bgt.values, out_considered.values))
    full_z = np.log((full_z - full_z.min()) + 1)

    OK = OrdinaryKriging(full_x, full_y,  full_z, variogram_model='gaussian', nlags=25,
                         verbose=True)
    z, ss = OK.execute('points', interp_x.ravel(), interp_y.ravel(), n_closest_points=50, backend='C')
    return z


def load_zeropoint(zpf):
    zeropoint = pd.read_csv(zpf, names=["ts", "avg"], header=0)
    zeropoint["ts"] = pd.to_datetime(zeropoint["ts"])
    zeropoint = zeropoint.sort_values("ts").set_index("ts")
    return zeropoint.resample('10T').mean()

def main(night):
    final_light_d6 = pd.read_csv("data/{}_d6_10m.csv".format(night, night))
    final_light_d9 = pd.read_csv("data/{}_d9_10m.csv".format(night, night))
    for df in (final_light_d6, final_light_d9):
        del df["Unnamed: 0"]
        del df["lat"]
        del df["lng"]
    final_light_d6 = final_light_d6.set_index("BBL").swapaxes(1, 0)
    final_light_d9 = final_light_d9.set_index("BBL").swapaxes(1, 0)
    zpo_d6 = load_zeropoint("weather_results/d6/{}.csv".format(night))
    zpo_d9 = load_zeropoint("weather_results/d9/{}.csv".format(night))
    subbed_d6 = final_light_d6.apply(lambda x: x - zpo_d6["avg"])
    subbed_d9 = final_light_d9.apply(lambda x: x - zpo_d9["avg"])

    concatd = pd.concat((subbed_d6, subbed_d9))
    final_light = concatd.groupby(concatd.index).mean()
    p = joblib.Parallel(n_jobs=64, backend='multiprocessing', verbose=1)
    jobs = [joblib.delayed(krige)(l) for ix, l in final_light.iterrows()]
    results = zip(final_light.index, p(jobs))
    for ts, result in results:
        df = pd.DataFrame({"x": interp_x.ravel(), "y": interp_y.ravel(), "z": result.ravel()})
        df.to_csv("kriged/{}/{}.csv".format(night, ts))
    
    return krige(final_light.iloc[0])
    
    


    
if __name__ == "__main__":
    import sys
    night = sys.argv[1]
    main(night)
