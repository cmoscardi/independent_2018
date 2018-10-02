"""
A quick script to show overlaid images to confirm registration.
"""

import os
import sys

import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import imread

try:
    UOFS_DIR = os.environ["UOFS_DIR"]
except KeyError:
    print("You need to set a UOFS_DIR environment variable.")
    sys.exit(1)

first_d6_path = UOFS_DIR + "2017-03-30_night" + "/9_d6_1490922100.png"
first_d9_path = UOFS_DIR + "2017-03-30_night" + "/9_d9_1490922101.png"

last_d6_path = UOFS_DIR + "2017-12-30_night" + "/0_d6_1514685614.png"
last_d9_path = UOFS_DIR + "2017-12-30_night" + "/0_d9_1514685617.png"

def main(d6_or_d9):
    first_d6 = imread(first_d6_path)
    first_d9 = imread(first_d9_path)
    last_d6 = imread(last_d6_path)
    last_d9 = imread(last_d9_path)
    if d6_or_d9 == "d6":
        first = np.rot90(first_d6, 3)
        last = np.rot90(last_d6, 3)
    else:
        first = np.rot90(first_d9, 3)
        last = np.rot90(last_d9, 3)

    rgb = np.zeros(first.shape + (3,), dtype=first.dtype)
    rgb[:, :, 0] = first
    rgb[:, :, 2] = last
    plt.imshow(rgb)
    plt.show()


if __name__ == "__main__":
    try:
        d6_or_d9 = sys.argv[1]
        main(d6_or_d9)
    except IndexError:
        print("Usage: python registration.py [d6_or_d9]")
        sys.exit(1)

