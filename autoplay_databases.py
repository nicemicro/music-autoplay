#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  3 20:35:00 2021

@author: nicemicro
"""

import pandas as pd
import numpy as np
import engine as e
from datetime import datetime
from mpd_wrapper import MPD
#%%

class DataBases:
    def __init__(self, filename):
        pd.options.display.max_colwidth = 25
        pd.options.display.width = 120
        e.pd.options.display.expand_frame_repr = False

        self.songlist = pd.DataFrame([])
        self.artists = pd.DataFrame([])
        self.albums = pd.DataFrame([])
        self.playlist = pd.DataFrame([])
        self.songs = pd.DataFrame([])
        self.similars_cache = e.Cache()
        self.load_file(filename)
        self.currentplayed: int
        if self.playlist.empty:
            self.currentplayed = -1
        else:
            self.currentplayed = self.playlist.index[-1]
        self.sugg_cache: dict[int, pd.DataFrame] = {}
        self.playablelist = pd.DataFrame([])
        self.plstartindex = 0
        self.plendindex = 0
        self.music = MPD()                    # create music object
        self.music.timeout = 100              # network timeout in seconds
        self.music.idletimeout = None         # for fetching the result of idle command
        self.music.connect("localhost", 6600)
    
    def search_song(self, artist, album, title, strict=True):
        artist = artist.replace(",", "")
        album = album.replace(",", "")
        title = title.replace(",", "")
        s_strings = []
        
        for word in [w for w in title.split(" ") if len(w) > 2]:
            s_strings.append("title")
            s_strings.append(word)
        for word in [w for w in artist.split(" ") if len(w) > 2]:
            s_strings.append("artist")
            s_strings.append(word)
        if len(s_strings) == 0:
            return pd.DataFrame()
        s_strings2 = s_strings.copy()
        if len(s_strings) > 30:
            s_strings = s_strings[0:30]
        if len(s_strings2) > 40:
            s_strings = s_strings[0:40]
        for word in [w for w in album.split(" ") if len(w) > 2]:
            s_strings.append("album")
            s_strings.append(word)
        if len(s_strings) > 40:
            s_strings = s_strings[0:40]
        
        result = self.music.search(*s_strings)
        if len(result) == 0:
            result = self.music.search(*s_strings2)
        if len(result) == 0:
            return pd.DataFrame(result)
        elif len(result) == 1:
            #print(f"Search: {artist}-{album}, found {len(result)}")
            if not "album" in result[0]:
                result[0]["album"] = ""
            result = pd.DataFrame(result)
            if strict:
                if result["artist"].str.lower().str.replace(",", "")[0] != artist.lower():
                    return pd.DataFrame()
                if album and result["album"].str.lower().str.replace(",", "")[0] != album.lower():
                    return pd.DataFrame()
                if result["title"].str.lower().str.replace(",", "")[0] != title.lower():
                    return pd.DataFrame()
            return result
        #print(f"Search: {artist}-{album}, found {len(result)}")
        if not "album" in result[0]:
            result[0]["album"] = ""
        result = pd.DataFrame(result)
        result = result[(result["title"].str.replace(",", "").str.lower() == title.lower())]
        result = result[(result["artist"].str.replace(",", "").str.lower() == artist.lower())]
        return result.reset_index(drop=True)

    def new_songlist(self, *args, **kwargs):
        self.playablelist = e.find_not_played(self.songlist, self.playlist,
                *args, **kwargs)
        self.plstartindex = 0
        self.plendindex = 0

    def mergesongdata(self, music_file_list: pd.DataFrame) -> pd.DataFrame:
        """
        Merges the song list coming from MPD (music_file_list) with the list coming from the
        database (songs).
        """
        if music_file_list.empty: return pd.DataFrame([])
        music_file_list = music_file_list.reset_index(drop=True)
        music_file_list = music_file_list.reset_index(drop=False)
        for dictkey in ["artist", "album", "title"]:
            if dictkey not in music_file_list.columns:
                music_file_list[dictkey] = ""
                music_file_list[dictkey+"_l"] = ""
                continue
            music_file_list[dictkey+"_l"] = music_file_list[dictkey].str.lower()
            music_file_list[dictkey+"_l"] = music_file_list[dictkey+"_l"].str.replace(",", "")
        #music_file_list.to_csv("music_file_list.csv")
        withalbum = self.songs[(self.songs["album_l"]) != ""]
        noalbum = self.songs[(self.songs["album_l"]) == ""]
        withalbum = pd.merge(music_file_list, withalbum, how="inner", on=["artist_l", "album_l" ,"title_l"])
        withalbum = withalbum.set_index("index")
        noalbum = pd.merge(music_file_list, noalbum, how="inner", on=["artist_l", "title_l"])
        noalbum = noalbum.set_index("index")
        all_found = pd.concat([noalbum, withalbum])
        result = pd.merge(
            music_file_list.set_index("index"),
            all_found[["Played last", "Added first", "Played"]],
            how="left",
            left_index=True,
            right_index=True
        )
        result["Artist"] = result["artist"].str.replace(",", "")
        result["Album"] = result["album"].str.replace(",", "")
        result["Title"] = result["title"].str.replace(",", "")
        return result

    def list_songs_fwd(self, num: int):
        index = self.plendindex
        songs = self.playablelist
        new_list = pd.DataFrame([])
        while len(new_list.index) < num and index < len(songs.index):
            song = self.search_song(songs.at[index, "Artist"],
                                    songs.at[index, "Album"],
                                    songs.at[index, "Title"])
            if len(song.index) > 0:
                new_list = pd.concat([new_list, song[0:1]])
            index += 1
        if len(new_list.index) > 0:
            self.plstartindex = self.plendindex
            self.plendindex = index
        new_list = self.mergesongdata(new_list)
        return new_list
    
    def list_songs_bck(self, num):
        index = self.plstartindex
        songs = self.playablelist
        new_list = pd.DataFrame([])
        while len(new_list.index) < num and index > 0:
            index -= 1
            song = self.search_song(songs.at[index, "Artist"],
                                    songs.at[index, "Album"],
                                    songs.at[index, "Title"])
            if len(song.index) > 0:
                new_list = pd.concat([song[0:1], new_list])
        if len(new_list.index) > 0:
            self.plendindex = self.plstartindex
            self.plstartindex = index
        new_list = self.mergesongdata(new_list)
        return new_list
    
    def suggest_song(self) -> pd.DataFrame:
        song = pd.DataFrame()
        last_index = max(self.playlist.index)
        print()
        print(f" -- Looking for similars list after {last_index} --")
        if last_index not in self.sugg_cache:
            print("      >> Generating list")
            self.sugg_cache[last_index] = (
                e.cumul_similar(self.songlist, self.playlist, cache=self.similars_cache)
            )
        print(self.playlist[-5:])
        index = last_index
        while song.empty and index >= 0:
            if (
                index not in self.sugg_cache or
                e.remove_played(self.sugg_cache[index], self.playlist).empty
            ):
                print(f" Either {index} is not listed in the playlist or is out of suggestions")
                index -= 1
                continue
            place, trial = e.choose_song(self.sugg_cache[index], self.playlist)
            print(f" -- Selecting from the list with length {len(self.sugg_cache[index])}: --")
            print(
                self.sugg_cache[index][
                    0:max(5, list(self.sugg_cache[index].index).index(place)+2)
                ]
            )
            print(f"  selected from list:  {place}")
            song = self.search_song(
                self.sugg_cache[index].at[place, "Artist"],
                self.sugg_cache[index].at[place, "Album"],
                self.sugg_cache[index].at[place, "Title"]
            )
            if not song.empty:
                song["Place"] = self.sugg_cache[index].at[place, "Place"]
                song["Last"] = self.sugg_cache[index].at[place, "Last"]
                song["Trial"] = trial
            self.sugg_cache[index] = self.sugg_cache[index].drop(place)
        if song.empty:
            lastsong = len(self.playlist) - 1
            song = self.search_song(self.playlist.at[lastsong, "Artist"],
                                    self.playlist.at[lastsong, "Album"],
                                    self.playlist.at[lastsong, "Title"])
            song["Place"] = np.NaN
            song["Last"] = np.NaN
            song["Trial"] = np.NaN
        song = self.mergesongdata(song)
        newline = (song[0:1][["Artist", "Album", "Title", "Place", "Last", "Trial"]])
        newline = newline.rename(
            {0: last_index+1},
            axis="index"
        )
        newline["Date added"] = datetime.utcnow()
        self.playlist = pd.concat([self.playlist, newline])
        print("  Selected song: ")
        print(newline)
        return song

    def search_string(self, string: str, hide_played: bool) -> pd.DataFrame:
        keys: list[str] = string.split(" ")
        result_list: list[dict[str, str]]
        if len(keys) == 1 and len(keys[0]) <= 2:
            result_list = self.music.find("any", keys[0])
        else:
            search_list: list[str] = []
            for key in keys:
                search_list += ["any", key]
            result_list = self.music.search(*search_list)
        if not result_list:
            return pd.DataFrame([])
        result = self.mergesongdata(pd.DataFrame(result_list))
        if hide_played:
            #result.to_csv("result.csv")
            #e.remove_played(result, self.playlist).to_csv("result_cut.csv")
            return e.remove_played(result, self.playlist)
        return result

    def search_artist(self, search_string):
        result = pd.DataFrame([])
        if not search_string:
            return result
        search_string = search_string.lower()
        artists = self.artists.copy()
        artists["Artist_l"] = artists["Artist"].str.lower()
        artist_match = artists[(artists["Artist_l"].str.\
                                contains(search_string))]
        if len(artist_match.index) == 0:
            return result 
        full = pd.DataFrame([])
        for index in artist_match.index:
            partial = e.find_not_played(self.songlist, self.playlist,
                                        artist_match.at[index, "Artist"],
                                        sort_by="rarely")
            full = pd.concat([full, partial])
            for index2 in partial.index:
                song = self.search_song(partial.at[index2, "Artist"],
                                        partial.at[index2, "Album"],
                                        partial.at[index2, "Title"])
                result = pd.concat([result, song[0:1]])
        new_list = self.mergesongdata(result)
        new_list = new_list.reset_index(drop=True)
        return new_list
    
    def songlist_append(self, artist, album, title):
        return
        # TODO fix this, whatever's wrong with it
        if (
            (self.songlist.at[0, "artist"].lower() == artist.lower()) and 
            (self.songlist.at[0, "title"].lower() == title.lower())
        ):
            return
        new_line = pd.DataFrame([[artist, album, title, datetime.utcnow()]],
                                columns=["Artist", "Album", "Title",
                                         "Scrobble time"])
        self.songlist = pd.concat([new_line, self.songlist]).reset_index(drop=True)
    
    def playlist_append(self, artist, album, title):        
        last_index = self.playlist.index[-1]
        if ((self.playlist.at[last_index, "Artist"].lower() == artist.lower()) and \
                (self.playlist.at[last_index, "Title"].lower() == title.lower())):
            return
        new_line = pd.DataFrame(
            [[artist, album, title, datetime.utcnow()]],
            columns=["Artist", "Album", "Title", "Date added"],
            index=[max(self.playlist.index)+1]
        )
        self.playlist = pd.concat([self.playlist, new_line])
    
    def add_song(self, position, filedata, jump=False):
        #print(f"add_song position={position}, jump={jump}")
        if position != -1:
            ret_data = self.delete_song(position, -1, jump)
        else:
            ret_data = pd.DataFrame([{"delfrom": -1, "delto": -1,
                                      "jump": jump}])
        #print("current suggestion list:")
        #for i, a, t in zip([line["index"] for line in self.suggestion],
        #                [line["artist"] for line in self.suggestion],
        #                [line["title"] for line in self.suggestion]):
        #    print(f"    {i}. {a} - {t}")
        filename, artist, album, title = \
            filedata[0], filedata[1], filedata[2], filedata[3]
        new_line = pd.DataFrame(
            [[artist, album, title, datetime.utcnow(), np.NaN, np.NaN]],
            columns=["Artist", "Album", "Title", "Date added", "Place", "Trial"],
            index=[max(self.playlist.index)+1]
        )
        self.playlist = pd.concat([self.playlist, new_line])
        ret_data["file"] = filename
        return ret_data
    
    def delete_song(self, delfrom, delto, jump=False):
        print(f"delete_song delfrom={delfrom}, delto={delto}, jump={jump}")
        if self.playlist.empty:
            return
        self.db_maintain()
        indeces: list[int] = list(
            self.playlist[self.playlist.index >= self.currentplayed].index
        )
        if delto == -1:
            dellist = list(
                self.playlist[
                    self.playlist.index >= indeces[delfrom]
                ].index
            )
        else:
            dellist = list(
                self.playlist[
                    (self.playlist.index >= indeces[delfrom]) &
                    (self.playlist.index < indeces[delto])
                ].index
            )
        print(f"  Deleting indexes {dellist}")
        for todel in dellist:
            if todel not in self.sugg_cache.keys():
                continue
            self.sugg_cache.pop(todel)
        self.playlist = self.playlist.drop(dellist)
        if delfrom == 0:
            self.currentplayed = (
                max(self.playlist[self.playlist.index < self.currentplayed].index)
            )
        return pd.DataFrame([{"delfrom": delfrom, "delto": delto,
                              "jump": jump}])
    
    def db_maintain(self):
        status = self.music.status()
        if status["state"] != "play":
            return
        #print("Start DB maintain...")
        currentsong = self.music.currentsong()
        duration = float(status["duration"])
        elapsed = float(status["elapsed"])
        c_artist = currentsong["artist"].replace(",", "")
        if "album" in currentsong:
            c_album = currentsong["album"].replace(",", "")
        else:
            c_album = ""
        c_title = currentsong["title"].replace(",", "")
        line = self.currentplayed
        while line < max(self.playlist.index) and line > -1:
            if line not in self.playlist.index:
                line += 1
                continue
            if not isinstance(self.playlist.at[line, "Album"], str):
               self.playlist.at[line, "Album"] = ""
            if (
                c_artist.lower() == self.playlist.at[line, "Artist"].lower() and
                c_title.lower() == self.playlist.at[line, "Title"].lower() and
                c_album.lower() == self.playlist.at[line, "Album"].lower()
            ):
                    self.currentplayed = line
                    break
            line += 1
        # Registering the currently playing song on the songs list
        if elapsed > 180 or elapsed > duration / 2:
            self.songlist_append(c_artist, c_album, c_title)
        #print("End DB maintain...")
            
    def load_file(self, fname=""):
        if fname == "":
            fname = "data"
        self.songlist, self.artists, self.albums, self.playlist = \
            e.load_data(fname)
        self.songs = e.summarize_songlist(self.songlist)
    
    def save_file(self, fname=""):
        if fname == "":
            fname= "data"
        #print("Start file save")
        e.save_data(
            self.songlist, self.artists, self.albums, self.playlist, fname
        )
        #print("Save complete")
