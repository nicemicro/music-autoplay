#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  3 20:35:00 2021

@author: nicemicro
"""

import pandas as pd
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
    
    def search_song(self, artist, album, title):
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
            # TODO what the hell happens if the string searched doesn't
            # actually match what we found???
            if not "album" in result[0]:
                result[0]["album"] = ""
            return pd.DataFrame(result)
        result = pd.DataFrame(result)
        result["title"] = result["title"].str.lower()
        result = result[(result["title"].str.replace(",", "") ==
                         title.lower())]
        return result.reset_index(drop=True)
    
    def new_songlist(self, *args, **kwargs):
        self.playablelist = e.find_not_played(self.songlist, self.playlist,
                                              *args, **kwargs)
        self.plstartindex = 0
        self.plendindex = 0
        
    def mergesongdata(self, new_list, songs):
        if new_list.empty or songs.empty: return pd.DataFrame([])
        new_list["artist_l"] = new_list["artist"].str.replace(",", "")
        new_list["artist_l"] = new_list["artist_l"].str.lower()
        new_list["title_l"] = new_list["title"].str.replace(",", "")
        new_list["title_l"] = new_list["title_l"].str.lower()
        songs["artist_l"] = songs["Artist"].str.lower()
        songs["title_l"] = songs["Title"].str.lower()
        new_list = pd.merge(new_list, songs, how="left",
                            on=["artist_l", "title_l"])
        return new_list
    
    def list_songs_fwd(self, num):
        index = self.plendindex
        songs = self.playablelist
        new_list = pd.DataFrame([])
        while len(new_list.index) < num and index < len(songs.index):            
            song = self.search_song(songs.at[index, "Artist"],
                                    songs.at[index, "Album"],
                                    songs.at[index, "Title"])
            if len(song.index) > 0:
                new_list = new_list.append(song[0:1])
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
                new_list = song[0:1].append(new_list)
        if len(new_list.index) > 0:
            self.plendindex = self.plstartindex
            self.plstartindex = index
        new_list = self.mergesongdata(new_list, songs)
        return new_list
    
    def make_suggestion(self):
        song = pd.DataFrame([])
        #print("-----------------------\n make_suggestion")
        while song.empty and self.suggestion:
            suggestionlist = self.suggestion[-1]["suggestions"]
            #print("Checked suggestions for after ",
            #      self.suggestion[-1]["artist"],
            #      " - ", self.suggestion[-1]["title"])
            #print(suggestionlist[0:10][["Artist", "Title", "Point"]])
            #print("\nPlaylist last elements:")
            #print(self.playlist[-2:][["Artist", "Title"]])
            if len(suggestionlist.index) == 0:
        #        print("suggestion list empty, popping...")
                self.suggestion.pop(-1)
                continue
            place, trial = e.choose_song(suggestionlist, self.playlist)      
            song = self.search_song(suggestionlist.at[place, "Artist"],
                                    suggestionlist.at[place, "Album"],
                                    suggestionlist.at[place, "Title"])
            self.suggestion[-1]["suggestions"] = \
                suggestionlist.drop(place).reset_index(drop=True)
            if self.suggestion[-1]["suggestions"].empty:
                self.suggestion.pop(-1)
        newline = (song[0:1][["artist", "album", "title"]]) \
            .rename({"artist": "Artist", "album": "Album",
                     "title": "Title"}, axis="columns")
        newline = newline.rename({0: self.playlist.index[-1]+1},
                                 axis="index")
        newline["Date added"] = datetime.utcnow()
        newline["Place"] = place + 1
        newline["Trial"] = trial
        self.playlist = self.playlist.append(newline)
        #print("\nSelected song: ", song.at[0, "artist"], song.at[0, "title"])
        #print("--------------------------")
        #print(self.playlist[-5:][["Artist", "Title", "Place", "Trial"]])
        #print("========================\n")
        return song
    
    def suggest_song(self):
        lastind = self.playlist.index[-1]
        artist = self.playlist.at[lastind, "Artist"]
        album = self.playlist.at[lastind, "Album"]
        title = self.playlist.at[lastind, "Title"]
        #print(f"suggest_song {artist}, {album}, {title}")
        if not self.suggestion or artist != self.suggestion[-1]["artist"] or \
                album != self.suggestion[-1]["album"] or \
                title != self.suggestion[-1]["title"] or \
                self.suggestion[-1]["suggestions"].empty:
            currsugg = {"index": lastind, "artist": artist,
                        "album": album, "title": title}
            currsugg["suggestions"] = e.find_similar(self.songlist, artist,
                                                     title, album)
            self.suggestion.append(currsugg)
        #    print(f"suggestion added for {artist} - {title}")
            if len(self.suggestion) > 10:
                self.suggestion.pop(0)
        #        print("suggestion list too long: first element popped")
        #print("----------------------------------")
        #print("current suggestion list:")
        #for i, a, t in zip([line["index"] for line in self.suggestion],
        #                [line["artist"] for line in self.suggestion],
        #                [line["title"] for line in self.suggestion]):
        #    print(f"    {i}. {a} - {t}")
        return self.make_suggestion()
    
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
            full = full.append(partial)
            for index2 in partial.index:
                song = self.search_song(partial.at[index2, "Artist"],
                                        partial.at[index2, "Album"],
                                        partial.at[index2, "Title"])
                result = result.append(song[0:1])
        new_list = self.mergesongdata(result, full)
        new_list = new_list.reset_index(drop=True)
        return new_list
    
    def songlist_append(self, artist, album, title):
        return
        # TODO fix this, whatever's wrong with it
        if (self.songlist.at[0, "artist"].lower() == artist.lower()) and \
                (self.songlist.at[0, "title"].lower() == title.lower()):
            return
        new_line = pd.DataFrame([[artist, album, title, datetime.utcnow()]],
                                columns=["Artist", "Album", "Title",
                                         "Scrobble time"])
        self.songlist = new_line.append(self.songlist).reset_index(drop=True)
    
    def playlist_append(self, artist, album, title):        
        last_index = self.playlist.index[-1]
        if ((self.playlist.at[last_index, "Artist"].lower() == artist.lower()) and \
                (self.playlist.at[last_index, "Title"].lower() == title.lower())):
            return
        new_line = pd.DataFrame([[artist, album, title, datetime.utcnow()]],
                                columns=["Artist", "Album", "Title",
                                         "Date added"])
        self.playlist = self.playlist.append(new_line).reset_index(drop=True)

    def delete_song(self, delfrom, delto):
        if self.playlist.empty:
            return
        #print(self.playlist[-10:][["Artist", "Title", "Place", "Trial"]])
        if delto == -1:
            dellist = list(range(delfrom+self.currentplayed,
                                 max(self.playlist.index)+ 1))
        else:
            dellist = list(range(delfrom+self.currentplayed,
                                 delto+self.currentplayed))
        #print(dellist)
        newsugg = [element for element in self.suggestion if
                   not element["index"] in dellist]
        assert len(newsugg) > 0, "Unreachable"
        index = newsugg[0]["index"]
        for element in newsugg[1:]:
            element["index"] = index + 1
            index += 1
        self.suggestion = newsugg
        self.playlist = self.playlist.drop(dellist).reset_index(drop=True)
        #print(self.playlist[-10:][["Artist", "Title", "Place", "Trial"]])
        return pd.DataFrame([{"delfrom": delfrom, "delto": delto}])
    
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
        while line < max(self.playlist.index):
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
