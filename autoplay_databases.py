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
        self.songlist = pd.DataFrame([])
        self.artists = pd.DataFrame([])
        self.albums = pd.DataFrame([])
        self.playlist = pd.DataFrame([])
        self.songs = pd.DataFrame([])
        self.similars_cache = e.Cache()
        self.load_file(filename)
        if self.playlist.empty:
            self.currentplayed = -1
        else:
            self.currentplayed = self.playlist.index[-1]
        self.suggestion = []
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

    def mergesongdata(self, new_list: pd.DataFrame, songs: pd.DataFrame) -> pd.DataFrame:
        if new_list.empty or songs.empty: return pd.DataFrame([])
        for dictkey in ["artist", "album", "title"]:
            if dictkey not in new_list.columns:
                new_list[dictkey] = ""
                new_list[dictkey+"_l"] = ""
                continue
            new_list[dictkey+"_l"] = new_list[dictkey].str.lower()
            new_list[dictkey+"_l"] = new_list[dictkey+"_l"].str.replace(",", "")
        for dictkey in ["Artist", "Album", "Title"]:
            if dictkey not in songs.columns:
                songs[dictkey] = ""
                songs[dictkey.lower()+"_l"] = ""
                continue
            songs[dictkey.lower()+"_l"] = songs[dictkey].str.lower()
        #new_list.to_csv("new_list.csv")
        #songs.to_csv("songs.csv")
        #pd.merge(new_list, songs, how="left", on=["artist_l", "title_l"]).to_csv("merged.csv")
        result = pd.merge(new_list, songs, how="left", on=["artist_l", "album_l" ,"title_l"])
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
        new_list = self.mergesongdata(new_list, songs)
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
        new_list = self.mergesongdata(new_list, songs)
        return new_list
    
    def make_suggestion(self) -> pd.DataFrame:
        song = pd.DataFrame([])
        index: int = len(self.suggestion) - 1
        print("-----------------------\n make_suggestion")
        suggestionlist = pd.DataFrame([])
        while song.empty and index >= 0:
            suggestionlist = self.suggestion[index]["suggestions"]
            print("  Checked suggestions for after ",
                  self.suggestion[index]["artist"],
                  " - ", self.suggestion[index]["title"],
                  f" index: {index}")
            print(f"    remained in suggestionlist: {len(suggestionlist)}")
            if len(suggestionlist.index) > 0:
                print(suggestionlist[["Artist", "Title", "Point", "Place", "Last"]].head())
            #print("\nPlaylist last elements:")
            #print(self.playlist[-2:][["Artist", "Title"]])
            if len(suggestionlist.index) == 0:
                print("    suggestion list empty, index decremented")
                index -= 1
                continue
            place, trial = e.choose_song(suggestionlist, self.playlist)      
            song = self.search_song(suggestionlist.at[place, "Artist"],
                                    suggestionlist.at[place, "Album"],
                                    suggestionlist.at[place, "Title"])
            print(f"  selected from list:  {place}")
            self.suggestion[index]["suggestions"] = \
                suggestionlist.drop(place).reset_index(drop=True)
        if song.empty:
            lastsong = len(self.playlist) - 1
            song = self.search_song(self.playlist.at[lastsong, "Artist"],
                                    self.playlist.at[lastsong, "Album"],
                                    self.playlist.at[lastsong, "Title"])
        if self.songs.empty:
            self.songs = e.summarize_songlist(self.songlist)
        song = self.mergesongdata(song, self.songs)
        newline = (song[0:1][["Artist", "Album", "Title"]])
        if not song.empty:
            newline["Place"] = suggestionlist.at[place, "Place"]
            newline["Last"] = suggestionlist.at[place, "Last"]
            newline["Trial"] = trial
        newline = newline.rename({0: self.playlist.index[-1]+1},
                                 axis="index")
        newline["Date added"] = datetime.utcnow()
        self.playlist = pd.concat([self.playlist, newline])
        print("  Selected song: ")
        print(newline)
        #print("--------------------------")
        #print(self.playlist[-5:][["Artist", "Title", "Place", "Trial"]])
        #print("========================\n")
        return song
    
    def suggest_song(self) -> pd.DataFrame:
        lastind = self.playlist.index[-1]
        artist = self.playlist.at[lastind, "Artist"]
        album = self.playlist.at[lastind, "Album"]
        title = self.playlist.at[lastind, "Title"]
        print("============================")
        print(f"suggest_song after {artist}, {album}, {title}")
        if (
            not self.suggestion or
            artist != self.suggestion[-1]["artist"] or
            album != self.suggestion[-1]["album"] or
            title != self.suggestion[-1]["title"]
        ):
            currsugg = {"index": lastind, "artist": artist,
                        "album": album, "title": title}
            #currsugg["suggestions"] = e.find_similar(self.songlist, artist,
            #                                         title, album)
            currsugg["suggestions"] = e.cumul_similar(self.songlist, self.playlist, cache=self.similars_cache)
            self.suggestion.append(currsugg)
            print(f"      (info) suggestion list added for {artist} - {title}")
            if len(self.suggestion) > 10:
                self.suggestion.pop(0)
                #print("suggestion list too long: first element popped")
        #print("----------------------------------")
        print("current suggestion list:")
        for i, a, t, l in zip([line["index"] for line in self.suggestion],
                        [line["artist"] for line in self.suggestion],
                        [line["title"] for line in self.suggestion],
                        [len(line["suggestions"]) for line in self.suggestion]):
            print(f"    {i}. {a} - {t} (sugg: {l} pcs.)")
        return self.make_suggestion()

    def search_string(self, string: str, hide_played: bool) -> pd.DataFrame:
        if self.songs.empty:
            self.songs = e.summarize_songlist(self.songlist)
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
        result = self.mergesongdata(pd.DataFrame(result_list), self.songs)
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
        new_list = self.mergesongdata(result, full)
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
        new_line = pd.DataFrame([[artist, album, title, datetime.utcnow()]],
                                columns=["Artist", "Album", "Title",
                                         "Date added"])
        self.playlist = pd.concat([self.playlist, new_line]).reset_index(drop=True)
    
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
        new_line = pd.DataFrame([[artist, album, title, datetime.utcnow(),
                                  np.NaN, np.NaN]],
                                columns=["Artist", "Album", "Title",
                                         "Date added", "Place", "Trial"])
        self.playlist = pd.concat([self.playlist, new_line]).reset_index(drop=True)
        ret_data["file"] = filename
        return ret_data
    
    def delete_song(self, delfrom, delto, jump=False):
        #print(f"delete_song delfrom={delfrom}, delto={delto}, jump={jump}")
        if self.playlist.empty:
            return
        self.db_maintain()
        #print(self.playlist[-10:][["Artist", "Title", "Place", "Trial"]])
        if delto == -1:
            dellist = list(range(delfrom+self.currentplayed,
                                 max(self.playlist.index)+ 1))
        else:
            dellist = list(range(delfrom+self.currentplayed,
                                 delto+self.currentplayed))
        #print(" database deletion ", dellist)
        #print(f" currently played: {self.currentplayed}")
        #print("current suggestion list:")
        #for i, a, t in zip([line["index"] for line in self.suggestion],
        #                [line["artist"] for line in self.suggestion],
        #                [line["title"] for line in self.suggestion]):
        #    print(f"    {i}. {a} - {t}")
        #print(f"  to delete {delfrom}-{delto}:", dellist)
        newsugg = [element for element in self.suggestion if
                   not element["index"] in dellist]
        if len(newsugg) > 0:
            index = newsugg[0]["index"]
            for element in newsugg[1:]:
                element["index"] = index + 1
                index += 1
            self.suggestion = newsugg
        else:
            self.suggestion = []
        #print("new suggestion list:")
        #for i, a, t in zip([line["index"] for line in newsugg],
        #                [line["artist"] for line in newsugg],
        #                [line["title"] for line in newsugg]):
        #    print(f"    {i}. {a} - {t}")
        self.playlist = self.playlist.drop(dellist).reset_index(drop=True)
        #print(self.playlist[-10:][["Artist", "Title", "Place", "Trial"]])
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
        #print("Assumed current played: ", self.currentplayed)
        #print(self.playlist[-5:][["Artist", "Title", "Place", "Trial"]])
        #print(f"Currently played: {c_artist} - {c_title}")
        while line < max(self.playlist.index) and line > -1:
            if not isinstance(self.playlist.at[line, "Album"], str):
               self.playlist.at[line, "Album"] = ""
            if c_artist.lower() == self.playlist.at[line, "Artist"].lower() and \
                c_title.lower() == self.playlist.at[line, "Title"].lower() and \
                c_album.lower() == self.playlist.at[line, "Album"].lower():
                    self.currentplayed = line
                    #print("Currently played set to: ", self.currentplayed)
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
    
    def save_file(self, fname=""):
        if fname == "":
            fname= "data"
        #print("Start file save")
        e.save_data(self.songlist, self.artists, self.albums, self.playlist,
                    fname)
        #print("Save complete")
