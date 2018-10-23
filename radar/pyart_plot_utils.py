import matplotlib.pyplot as plt
from matplotlib import animation

from mpl_toolkits.axes_grid1 import make_axes_locatable
import numpy as np


def plot_ts(radar_df):
    fig, axes = plt.subplots(2, 2, figsize=(16, 15))
    axes = axes.flatten()
    for func, ax in zip(['mean', 'median', 'std', 'max'], axes):
        x = radar_df if func != 'median' else radar_df[radar_df.dbzh > 0]
        res = getattr(x.groupby("datetime").dbzh, func)()
        res.plot(ax=ax, style='-o')
        ax.set_title(func.title() if func != 'median' else "Median of values > 0")
    return fig
        
        
def plot_video(frame):
    fig, ax = plt.subplots(figsize=(10, 6))
    divider = make_axes_locatable(ax)

    cax = divider.append_axes('right', size='5%', pad=0.05)

    scatter = ax.scatter([frame["xx"].min(), frame["xx"].max()], 
                         [frame["yy"].min(), frame["yy"].max()],
                         c=[frame["dbzh"].min(), frame["dbzh"].max()])
    ax.set_xlim([frame["xx"].min(), frame["xx"].max()])
    ax.set_ylim([frame["yy"].min(), frame["yy"].max()])

    
    dates = np.sort(frame.datetime.unique())
    def update_scatter(i):
        xy = frame[frame.datetime == dates[i]]
        scatter.set_offsets(xy[['xx', 'yy']])
        scatter.set_array(xy['dbzh'])
        return scatter,

    anim = animation.FuncAnimation(fig, update_scatter,
                                   frames=len(dates), interval=400)
    fig.colorbar(scatter, cax=cax)

    return anim
