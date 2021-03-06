from collections import namedtuple, OrderedDict
from datetime import datetime as dt
from datetime import timedelta as td
from datetime import time
import glob
import os
import sys

import joblib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

ImageResult = namedtuple("ImageResult", ["mean", "std", "n"])

try:
    with open("/home/cusp/clm633/uofs_dir") as uofs_dir:
        UOFS_DIR = uofs_dir.read().strip()
except KeyError:
    if 'UOFS_DIR' not in os.environ:
        print("You need a UOFS_DIR environment variable.")
        sys.exit(1)
    else:
        UOFS_DIR = os.environ['UOFS_DIR']

WEATHER_RESULTS_DIR = "weather_results"
try:
    os.mkdir(WEATHER_RESULTS_DIR)
except FileExistsError:
    pass

p = joblib.Parallel(n_jobs=64, verbose=1, backend='multiprocessing')
# slices for d6
x1_d6 = 200
x2_d6 = 1000
y1_d6 = 200
y2_d6 = 1000
d6_skypatch = [y1_d6, y2_d6, x1_d6, x2_d6]

# slices for d9
x1_d9 = 2800
x2_d9 = 3600
y1_d9 = 3800
y2_d9 = 4600
d9_skypatch = [y1_d9, y2_d9, x1_d9, x2_d9]
y1_d9_v2 = 500
y2_d9_v2 = 1300
x1_d9_v2 = 4000
x2_d9_v2 = 4800
d9_skypatch_v2 = [y1_d9_v2, y2_d9_v2, x1_d9_v2, x2_d9_v2]

y1_d9_v3 = 500
y2_d9_v3 = 1300
x1_d9_v3 = 4500
x2_d9_v3 = 4900
d9_skypatch_v3 = [y1_d9_v3, y2_d9_v3, x1_d9_v3, x2_d9_v3]

y1_d9_911 = 750
y2_d9_911 = 850
x1_d9_911 = 2900
x2_d9_911 = 4900
d9_911 = [ y1_d9_911, y2_d9_911, x1_d9_911, x2_d9_911 ]

# format: x1, x2, y1, y2
# for d6
squares_d6 = [(1080, 1500, 2450, 2700),
           (3050, 3180, 1020, 1250),
           (3480, 3610, 820, 1010),
           (1500 + 600, 1500 + 900, 650, 800),
           (1500 + 600 + 130, 1500 + 600 + 140, 250, 310),
           (2440, 2480, 3720, 3750),
           (2470, 2490, 20, 40),
           (2000 + 940, 2970, 300, 330),
           (2000 + 680, 2000 + 760, 1500 + 300, 1500 + 380),
           (2000 + 370 + 10, 2000 + 410, 1500 + 400, 1500 + 450)]

squares_d9 = None

def get_patch_brightness(path, ix, which):
    y1, y2, x1, x2 = d6_skypatch if which == 'd6' else d9_911
    if ix is None:
        return plt.imread(path)[y1:y2, x1:x2].mean()
    else:
        squares = squares_d6 if which == 'd6' else squares_d9
        six = squares[ix]
        return plt.imread(path)[six[2]:six[3],six[0]:six[1]].mean()

def process(directory, ix, which='d6'):
    if which == 'd6':
        image_files = glob.glob(directory + "/*d6*.png")
        for f in image_files:
            assert ("_d6_" in f or "_d6-" in f)
    else:
        image_files = glob.glob(directory + "/*d9*.png")
        for f in image_files:
            assert ("_d9_" in f or "_d9-" in f)
    futs = [joblib.delayed(get_patch_brightness)(f, ix, which) for f in image_files]
    results = p(futs)
    timestamps = [dt.fromtimestamp(int(f.split("_")[-1].split(".")[0])) for f in image_files]
    return timestamps, results

def main(test=True, spring_or_fall="fall", ix=None, which='d6'):
    spring_dirs = glob.glob(UOFS_DIR + "2017-0[3-6]*_night")
    fall_dirs = glob.glob(UOFS_DIR + "2017-10*_night") + glob.glob(UOFS_DIR + "2017-09*_night")
    sortfunc = lambda x: tuple([int(a) for a in x.split("/")[-1].split("_")[0].split("-")[1:3]])
    spring_dirs.sort(key=sortfunc)
    fall_dirs.sort(key=sortfunc)
    night_dirs = spring_dirs if spring_or_fall == "spring" else fall_dirs
    if spring_or_fall == '911':
        night_dirs = [n for n in night_dirs if '2017-09-12' in n]
        print(night_dirs)
    if test:
        night_dirs = night_dirs[:1]

    for d in night_dirs:
        date = d.split("/")[-1].split("_")[0]
        out_f = WEATHER_RESULTS_DIR + "/{}".format(which) + "/{}.csv".format(date) + ("" if spring_or_fall != "911" else "911")
        out_f = out_f + (str(ix) if ix is not None else "")
        if os.path.isfile(out_f):
            continue
        print("on night {}, ix {}".format(date, ix))
        timestamps, results = process(d, ix=ix, which=which)
        df = pd.DataFrame(results, index=timestamps)
        df.to_csv(out_f)

def process_csv(f):
    df = pd.read_csv(f)
    df["night"] = pd.to_datetime(f.split("/")[1].split(".")[0])
    return df

def process_dfs(before_4=True):
    """
    :param before_4: only return images before 4am. avoids sunrise
    """
    files = glob.glob(WEATHER_RESULTS_DIR + "/*.csv")
    big_df = pd.concat(process_csv(f) for f in files)
    big_df["time"] = pd.to_datetime(big_df["Unnamed: 0"].str.split(" ").str[1])
    big_df["offset_time"] = (big_df["time"] + td(hours=4)).apply(lambda x: x.time())
    big_df["ts"] = pd.to_datetime(big_df["Unnamed: 0"])
    big_df.set_index("ts", inplace=True)
    if before_4:
        # we use 8 because 'offset_time' has a 4hr timedelta
        big_df = big_df[big_df["offset_time"] < time(8)]
    big_df["avg"] = big_df["0"]
    big_df["avg_norm"] = big_df.groupby("night")["avg"].apply(lambda g: g / g.max())
    big_df["std_norm"] = big_df.groupby("night")["avg"].apply(lambda g: (g - g.mean()) / g.std())
    big_df["max_norm"] = big_df.groupby("night")["avg"].apply(lambda g: (g - g.mean()) / g.max())
    del big_df["0"]
    del big_df["Unnamed: 0"]
    return big_df.sort_index()

def images_for_night(night, which=None):
    base = UOFS_DIR + night + "_night/"
    kf = lambda x: x.split("_")[-1]
    if which:
        return sorted(glob.glob(base + "*{}*.png".format(which)), key=kf)
    else:
        return sorted(glob.glob(base + "*.png"), key=kf)

def plot_image(image_file, cmap=None):
    im = plt.imread(image_file)
    if cmap:
        im = 1 - im
    plt.imshow(np.rot90(im, 3 if 'd6' in image_file else 1), cmap=cmap)


if __name__ == "__main__":
    main(test=False, spring_or_fall="911", which='d9')
