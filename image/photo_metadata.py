from datetime import datetime as dt
import glob
import os

import pandas as pd

"""
USAGE: This script is meant to be used interactively.

1. Run 'get_files_by_dir' to get a dictionary of filenames for each night.
2. Functions below either accept 'files_by_dir' to generate various pieces of information.
3. OR, they accept 'photo_list', which is a list of the photo filepaths (values of the files_by_dir dict).
"""

# 2 cameras * 1 photo / 10 s * 8 hours * 3600 seconds / hour
theoretical_ideal_n = 2 * 1/10 * 8 * 3600

try:
    UOFS_DIR = os.environ["UOFS_DIR"]
except KeyError:
    raise KeyError("You need a UOFS_DIR environment variable.")

def get_files_by_dir():
    """
    returns: a dict of type {date_str: list_of_filepaths}
    """
    photo_dirs = sorted(d for d in os.listdir(UOFS_DIR) if d.endswith("_night"))
    photo_dates = [p.split("_")[0] for p in photo_dirs]
    files_by_dir = {date: glob.glob(UOFS_DIR + f + "/*.png")\
                    for f, date in zip(photo_dirs, photo_dates)}
    return files_by_dir

def photo_counts_by_night(files_by_dir):
    len_df = pd.DataFrame(((k, len(v)) for k, v in files_by_dir.items()),
                          columns=["date", "n_photos"])
    len_df["datetime"] = pd.to_datetime(len_df["date"])
    len_df = len_df.set_index("datetime")

    # dates here are arbitrary. maybe should line up with walkers?
    spring_df = len_df["2017-03-01":"2017-6-15"]
    fall_df = len_df["2017-08-15":"2017-11-30"]
    return spring_df, fall_df


def photo_list_df(photo_list):
    """
    Given a list of photo filenames, give back a DF of the metadata for each
    photo in nice format.
    """
    photo_mtimes = [int(os.stat(f).st_mtime) for f in photo_list]
    photos_df = pd.DataFrame(photo_attrs, columns=["i", "camera", "ts", "mtime"])
    photo_filenames = [p.split("/")[-1] for p in photo_list]
    photo_attrs = [fname.split(".")[0].split("_") + [mtime] for fname, mtime in zip(photo_filenames, photo_mtimes)]

    # time that the file is created and the shutter is hit
    photos_df["f_ts"] = photos_df["ts"].apply(lambda x: datetime.fromtimestamp(int(x)))
    # mtime from os.stat -- last time the file was written to
    photos_df["m_ts"] = photos_df["mtime"].apply(datetime.fromtimestamp)

    return photos_df

def meta_info(photo_list):
    photo_filenames = [p.split("/")[-1] for p in photo_list]
    photo_attrs = [fname.split(".")[0].split("_") for fname in photo_filenames]
    n_d6 = len([a for a in photo_attrs if a[1] == "d6"])
    n_d9 = len([a for a in photo_attrs if a[1] == "d9"])
    # there was 1 directory where a file had _last as the timestamp?
    timestamps = sorted(int(a[-1]) for a in photo_attrs if 'last' not in a[-1])
    min_t = dt.fromtimestamp(timestamps[0]).time() if timestamps else None
    max_t = dt.fromtimestamp(timestamps[-1]).time() if timestamps else None
    return len(photo_list), n_d6, n_d9, min_t, max_t

def gen_meta_csv(files_by_dir):
    cols = ['date', 'n_photos', 'n_d6', 'n_d9', 'min_t', 'max_t']
    meta = pd.DataFrame(((k,) + n_photos.meta_info(v)\
                        for k, v in files_by_dir.items()), columns=cols)
    return meta
