import os

from matplotlib import animation
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def load_scans(night_dirs):
    # for processing each file
    csv_df = pd.DataFrame()
    # final df
    df = pd.DataFrame()


    for d in night_dirs:
        for filename in os.listdir(d):
            if filename.endswith(".csv"):
                csv_df = pd.read_csv(d + "/" + filename,index_col=0)


                #csv_df = csv_df.drop(['Unnamed: 0'], axis=1)
                csv_df['Date_Time'] = pd.to_datetime(filename[4:19],format='%Y%m%d_%H%M%S')
                df = df.append(csv_df)
    return df

def plot_video(frame):
    fig, ax = plt.subplots(figsize=(10, 6))
    divider = make_axes_locatable(ax)

    cax = divider.append_axes('right', size='5%', pad=0.05)

    scatter = ax.scatter([frame["s1"].min(), frame["s2"].min()], 
                         [frame["s1"].min(), frame["s2"].min()],
                         c=[frame["DBZH"].min(), frame["DBZH"].max()])
    ax.set_xlim([frame["s1"].min(), frame["s1"].max()])
    ax.set_ylim([frame["s2"].min(), frame["s2"].max()])
    def init():
        return scatter
    
    dates = np.sort(frame.Date_Time.unique())
    def update_scatter(i):
        xy = frame[frame.Date_Time == dates[i]]
        scatter.set_offsets(xy[['s1', 's2']])
        scatter.set_array(xy['DBZH'])
        return scatter,

    anim = animation.FuncAnimation(fig, update_scatter,
                                   frames=len(dates), interval=400)
    fig.colorbar(scatter, cax=cax)

    return anim


def impute(df):
    full_index = pd.DataFrame(np.array(np.meshgrid(df.Date_Time.unique(), df.s1.unique(),
                                                   df.s2.unique())).T.reshape(-1, 3),
                              columns=["Date_Time", "s1", "s2"])
    df = df.set_index(["Date_Time", "s1", "s2"])
    full_df = pd.concat([full_index, pd.DataFrame(np.zeros((full_index.shape[0], 1)))], axis=1)

    columns = ["Date_Time", "s1", "s2"] + list(df.columns)
    full_df = pd.concat([full_index, pd.DataFrame(np.zeros((full_index.shape[0], 5)),
                       columns=df.columns)], axis=1)
    full_df["Date_Time"] = pd.to_datetime(full_df["Date_Time"])
    full_indexed = full_df.set_index(["Date_Time", "s1", "s2"])
    joined = full_indexed.join(df, lsuffix='_delete')
    joined = joined[[j for j in joined.columns if "_delete" not in j]]
    imputed = joined.fillna(0)
    imputed = imputed.reset_index()
    return imputed

def cut_weather(df):
    return df[df['DBZH'] < 35]

def cut_insects(df, intermediate=False):
    # isolate birds
    no_insect_1 = df[df['RHOHV'] <= 0.95]
    no_insect_2 = no_insect_1[(no_insect_1['VRADH'] >= 1 )| \
                          (no_insect_1['VRADH'] <= -1)]
    return no_insect_2


def plot_ts(radar_df):
    fig, axes = plt.subplots(2, 2, figsize=(16, 15))
    axes = axes.flatten()
    for func, ax in zip(['mean', 'median', 'std', 'max'], axes):
        x = radar_df if func != 'median' else radar_df[radar_df.DBZH > 0]
        res = getattr(x.groupby("Date_Time").DBZH, func)()
        res.plot(ax=ax, style='-o')
        ax.set_title(func.title() if func != 'median' else "Median of values > 0")

