#!/usr/bin/env python3 
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 09:46:50 2019

@author: nicemicro
"""

import engine as e

def plist_end(playlist, where=1, debug=False):
    """
    Parameters
    ----------
    playlist : pandas DataFrame 
        Containins the active playlist with colums Artist, Album, Title, Date
        added, Place, Trial (text, text, text, datetime, number, number).
    where : number, optional
        How much ×10+1 songs from the back of the playlist we want to see.
        The default is 1 (the last 10+1 songs are shown).
    debug: boolean, optional
        If True, all columns are returned. If false, only the relevant columns
        are returned.
        The default is False
    Returns
    -------
    cut_list : pandas DataFrame
        The truncated playlist for display purpose.
    """
    cut_list = playlist[where*(-10)-1:].copy()
    cut_list = cut_list[0:11]
    cut_list['Artist l'] = cut_list['Artist'].apply(len)
    cut_list.loc[(cut_list['Album'].isnull()), ['Album']] = ""
    cut_list['Album l'] = cut_list['Album'].apply(len)
    cut_list['Title l'] = cut_list['Title'].apply(len)
    w_artist = 18
    w_album = 10
    w_title = 28
    width = w_artist + w_album + w_title
    if max(cut_list['Artist l']) + max(max(cut_list['Album l']), 5) + \
        max(cut_list['Title l']) > width:
        w_album = max(max(w_album, 5), width - max(cut_list['Artist l']) - \
                      max(cut_list['Title l']))
        if max(max(cut_list['Album l']), 5) > w_album:
            for i in cut_list[(cut_list['Album l'])>w_album].index:
                cut_list.loc[i, ['Album']] = \
                    cut_list.at[i, 'Album'][:w_album-4] + "…" + \
                        cut_list.at[i, 'Album'][-3:]
            width-=w_album
        else:
            width-=max(max(cut_list['Album l']), 5)
        if max(cut_list['Artist l']) + max(cut_list['Title l']) > width:
            w_artist = max(w_artist, width - max(cut_list['Title l']))
            if max(cut_list['Artist l']) > w_artist:
                for i in cut_list[(cut_list['Artist l'])>w_artist].index:
                    cut_list.loc[i, ['Artist']] = \
                        cut_list.at[i, 'Artist'][:w_artist-1] + "…"
                width-=w_artist
            else:
                width-=max(cut_list['Artist l'])
            if max(cut_list['Title l']) > width:
                for i in cut_list[(cut_list['Title l'])>width].index:
                    cut_list.loc[i, ['Title']] = \
                        cut_list.at[i, 'Title'][:width-6] + "…" + \
                        cut_list.at[i, 'Title'][-5:]
    if debug:
        return cut_list
    return cut_list[["Artist", "Album", "Title", "Place", "Trial"]]

def not_played(songlist, playlist, artist='', album='', **kwargs):
    page = 0
    debug = False;
    for key, value in kwargs.items():
        if key == 'page':
            page = value
        if key == 'debug':
            debug = value
    songs = e.find_not_played(songlist, playlist, artist, album, **kwargs)
    songs = songs[page*10:(page+1)*10]
    if len(songs.index) == 0:
        return songs[["Artist", "Album", "Title", "Played"]]
    songs['Artist l'] = songs['Artist'].apply(len)
    songs.loc[(songs['Album'].isnull()), ['Album']] = ""
    songs['Album l'] = songs['Album'].apply(len)
    songs['Title l'] = songs['Title'].apply(len) 
    if artist == "":
        w_artist = 15
        w_album = 10
        w_title = 26
    elif album == "":
        w_artist = 6
        w_album = 12
        w_title = 33
    else:
        w_artist = 6
        w_album = 5
        w_title = 40
    width = w_artist + w_album + w_title
    songs["Last"] = ""
    for i in songs.index:
        songs.loc[i, ["Last"]] = songs.at[i, 'Played last'].strftime( \
            "%y-%m-%d")
    if max(songs['Artist l']) + max(max(songs['Album l']), 5) +\
        max(songs['Title l']) > width:
        w_album = max(w_album, width - max(songs['Artist l']) - \
                      max(songs['Title l']))
        if max(max(songs['Album l']), 5) > w_album:
            for i in songs[(songs['Album l'])>w_album].index:
                songs.loc[i, ['Album']] = \
                    songs.at[i, 'Album'][:w_album-4] + "…" + \
                        songs.at[i, 'Album'][-3:]
            width-=w_album
        else:
            width-= max(max(songs['Album l']), 5)
        if max(songs['Artist l']) + max(songs['Title l']) > width:
            w_artist = max(w_artist, width - max(songs['Title l']))
            if max(songs['Artist l']) > w_artist:
                for i in songs[(songs['Artist l'])>w_artist].index:
                    songs.loc[i, ['Artist']] = \
                        songs.at[i, 'Artist'][:w_artist-1] + "…"
                width-=w_artist
            else:
                width-=max(songs['Artist l'])
            if max(songs['Title l']) > width:
                for i in songs[(songs['Title l'])>width].index:
                    songs.loc[i, ['Title']] = \
                        songs.at[i, 'Title'][:width-6] + "…" + \
                        songs.at[i, 'Title'][-5:]
    if debug:
        return songs
    return songs[["Artist", "Album", "Title", "Played", "Last"]]

#%%
def unique(playlist):
    playlist2 = playlist.copy()
    playlist2["artist_low"] = playlist2["Artist"].str.lower()
    playlist2["title_low"] = playlist2["Title"].str.lower()
    return len(playlist2.groupby(["artist_low", "title_low"], axis=0) \
               .agg({"Date added": "count"}).index)
   
#%%
def main_loop():
    global songlist, artists, albums, playlist
    print('Select a command')
    
if __name__ == '__main__':
    print('Welcome to the CLI of the playlist generator program.')
    cmd = input('Press y to load files. ')
    if cmd in ('y', 'Y'):
        fname = input('Loading files. Enter file name (default: data.pckl): ')
        if fname == '':
            songlist, artists, albums, playlist = e.load_data()
        else:
            songlist, artists, albums, playlist = e.load_data(fname)
        print('Loaded.')
    main_loop()
    print('')
    print('Bye')

