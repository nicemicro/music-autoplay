#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  6 13:44:07 2022

@author: nicemicro
"""

import engine as e

v1_array = [1, 0.1, 0.01, 0.001]
v2_array = [1, 0.1, 0.01, 0.001]

def test(v1, v2, playlist, songlist):
    for index in playlist.index:
        if index % 3 != 2:
            continue
        artist = playlist.at[index, "Artist"]
        album = playlist.at[index, "Album"]
        title = playlist.at[index, "Title"]
        print()
        print(f"* {artist} - {album} - {title} *")
        print( "------------------------------------")
        similar_one = e.find_similar(songlist, artist, title, album)
        similar_more = e.cumul_similar(songlist, playlist[0:index+1])
        similar_more = similar_more.sort_values(["SumP"], ascending=False)
        print("Based on only last song:")
        print(similar_one[0:15][["Artist", "Title", "Point"]])
        print()
        print("Based last 3 songs:")
        print(similar_more[0:15][["Artist", "Title", "SumP"]])

def main():
    songlist, artists, albums, playlist = e.load_data("test")
    
    for v1 in v1_array:
        for v2 in v2_array:
            print( "====================================")
            print(f"** TEST CASE v1 = {v1}, v2 = {v2} **")
            print( "====================================")
            test(v1, v2, playlist, songlist)
            print( "====================================")
            print()
            print()
#%%
if __name__ == "__main__":
    main()
