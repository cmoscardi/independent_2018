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

import shapely.geometry
import geopandas as gpd

filt = (out_reset.geometry.centroid.x > -74.03) & (out_reset.geometry.centroid.y > 40.7)
ch = gpd.GeoSeries(pd.concat([in_frame.geometry, out_reset[filt].geometry])).unary_union.convex_hull
ch_df = gpd.GeoDataFrame({"geometry": gpd.GeoSeries([ch])})


def krige(means, min_val):
    means_f = means.to_frame().apply(lambda x: x - min_val)
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
    full_z = np.log(full_z + 1)

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
#    subbed_d6 = final_light_d6.apply(lambda x: x - zpo_d6["avg"])
#    subbed_d9 = final_light_d9.apply(lambda x: x - zpo_d9["avg"])

    subbed_d6 = final_light_d6
    subbed_d9 = final_light_d9
    concatd = pd.concat((subbed_d6, subbed_d9))
    final_light = concatd.groupby(concatd.index).mean()
    min_val = final_light.min().min()
    p = joblib.Parallel(n_jobs=64, backend='multiprocessing', verbose=1)
    jobs = [joblib.delayed(krige)(l, min_val) for ix, l in final_light.iterrows()]
    results = zip(final_light.index, p(jobs))
    for ts, result in results:
        df = pd.DataFrame({"x": interp_x.ravel(), "y": interp_y.ravel(), "z": result.ravel()})
        df.to_csv("kriged/{}/{}.csv".format(night, ts))
    
    return "OK"
    

def filter_to_manhattan(df):
    df["geometry"] = df[["x", "y"]].apply(shapely.geometry.Point, axis=1)
    df = gpd.GeoDataFrame(df)
    joined = gpd.sjoin(df, ch_df)
    return joined
    
    
def plot_video(frame):
    fig, ax = plt.subplots(figsize=(10, 6))
    divider = make_axes_locatable(ax)

    cax = divider.append_axes('right', size='5%', pad=0.05)

    scatter = ax.scatter([frame["x"].min(), frame["x"].max()], 
                         [frame["y"].min(), frame["y"].max()],
                         c=[frame["z"].min(), frame["z"].max()])
    ax.set_xlim([frame["x"].min(), frame["x"].max()])
    ax.set_ylim([frame["y"].min(), frame["y"].max()])

    
    dates = np.sort(frame.datetime.unique())
    def update_scatter(i):
        xy = frame[frame.datetime == dates[i]]
        scatter.set_offsets(xy[['x', 'y']])
        scatter.set_array(xy['z'])
        return scatter,

    anim = animation.FuncAnimation(fig, update_scatter,
                                   frames=len(dates), interval=400)
    fig.colorbar(scatter, cax=cax)

    return anim


def process_light_csv(fname):
    df = pd.read_csv(fname)
    df["timestamp"] = pd.to_datetime(fname.split("/")[-1].split(".")[-2])
    return df

def main2():
    """
    the purpose of this function is to filter all files in the kriged/
    directory
    """
    import glob
    import os

    nights = glob.glob("kriged/*")
    nights = [n for n in nights if 'filtd' not in n or 'csv' in n]
    for night in nights:
        print("on night {}".format(night))
        light_levels_files = glob.glob("{}/*.csv".format(night))
        fname = "{}_filtd.csv".format(night)
        if len(light_levels_files) == 0 or os.path.exists(fname):
            continue
        light_levels = pd.concat((process_light_csv(f) for f in light_levels_files), ignore_index=True)
        filtd = filter_to_manhattan(light_levels)
        filtd.to_csv(fname)
    
if __name__ == "__main__":
    import sys
    night = sys.argv[1]
    if night == 'filter':
        main2()
        sys.exit(0)
    else:
        main(night)
