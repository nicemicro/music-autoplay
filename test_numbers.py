#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 09:26:24 2021

@author: nicemicro
"""

import engine as e
import numpy as np

tests = {"sumonly": np.array([[10, 0, 0], [0, 5, 0], [0, 0, 1]]),
        "sumlong": np.array([[10, 0, 0, 0, 0],
            [0, 5, 0, 0, 0],
            [0, 0, 2, 0, 0],
            [0, 0, 0, 1, 0],
            [0, 0, 0, 0, 1]]),
        "multisuper": np.array([[5, 0, 0], [10, 2, 0], [8, 3, 1]]),
        "multiheavy": np.array([[10, 0, 0], [10, 2, 0], [8, 3, 1]]),
        "multionly": np.array([[10, 0, 0], [10, 0, 0], [8, 3, 0]]),
        "multibal": np.array([[10, 0, 0], [8, 2, 0], [5, 3, 1]]),
        "sumheavy": np.array([[10, 0, 0], [5, 5, 0], [3, 2, 2]]),
        "long": np.array([[10, 0, 0, 0, 0],
            [7, 5, 0, 0, 0],
            [5, 3, 2, 0, 0],
            [3, 2, 0, 1, 0],
            [1, 1, 0, 0, 1]]),
        }

songlist, artists, albums, playlist = e.load_data()


for name, points in tests.items():
    print(f"processing {name}")
    e.cumul_similar(songlist, playlist[755:764], points=points).to_csv(f"testres/{name}.csv")
