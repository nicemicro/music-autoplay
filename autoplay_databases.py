#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  3 20:35:00 2021

@author: nicemicro
"""

import pandas as pd
import engine as e
from datetime import datetime
#%%

class DataBases:
    def __init__(self, filename):        
        self.songlist = pd.DataFrame([])
        self.artists = pd.DataFrame([])
        self.albums = pd.DataFrame([])
        self.playlist = pd.DataFrame([])
        self.load_file(filename)
        self.suggestion = []
        self.playablelist = pd.DataFrame([])
        self.plstartindex = 0
        self.plendindex = 0
    
    def search_song(self, music, artist, album, title):
        artist = artist.replace(",", "")
        album = album.replace(",", "")
        title = title.replace(",", "")
        s_strings = []
        
        # TODO figure out why Korean strings are not split properly
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
        
        result = music.search(*s_strings)
        if len(result) == 0:
            result = music.search(*s_strings2)
        if len(result) <= 1:
            # TODO what the hell happens if the string searched doesn't
            # actually match what we found???
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
    
    def list_songs_fwd(self, music, num):
        index = self.plendindex
        songs = self.playablelist
        new_list = pd.DataFrame([])
        while len(new_list.index) < num and index < len(songs.index):            
            song = self.search_song(music, songs.at[index, "Artist"],
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
    
    def list_songs_bck(self, music, num):        
        index = self.plstartindex
        songs = self.playablelist
        new_list = pd.DataFrame([])
        while len(new_list.index) < num and index > 0:
            index -= 1
            song = self.search_song(music, songs.at[index, "Artist"],
                                    songs.at[index, "Album"],
                                    songs.at[index, "Title"])
            if len(song.index) > 0:
                new_list = song[0:1].append(new_list)
        if len(new_list.index) > 0:
            self.plendindex = self.plstartindex
            self.plstartindex = index
        new_list = self.mergesongdata(new_list, songs)
        return new_list
    
    def make_suggestion(self, music):
        song = pd.DataFrame([])
        while song.empty and self.suggestion:
            suggestionlist = self.suggestion[-1]["suggestions"]
            #print("Suggesting song for after ", self.suggestion[-1]["artist"],
            #      " - ", self.suggestion[-1]["title"])
            #print(suggestionlist[0:10][["Artist", "Title", "Point"]])
            if len(suggestionlist.index) == 0:
                self.suggestion.pop(-1)
                continue
            place, trial = e.choose_song(suggestionlist, self.playlist)      
            song = self.search_song(music, suggestionlist.at[place, "Artist"],
                                    suggestionlist.at[place, "Album"],
                                    suggestionlist.at[place, "Title"])
            self.suggestion[-1]["suggestions"] = \
                suggestionlist.drop(place).reset_index(drop=True)
            if self.suggestion[-1]["suggestions"].empty:
                self.suggestion.pop(-1)
            #print("Selected song: ", song.at[0, "artist"], song.at[0, "title"])
        return song
    
    def renew_suggestion(self, music, c_artist, c_album, c_title):
        if self.suggestion and c_artist == self.suggestion[-1]["artist"] \
                and c_album == self.suggestion[-1]["album"] and \
                c_title == self.suggestion[-1]["title"]:
            # If the current suggestion is already for the song now playing,
            # we need to go back to the previous suggestion. If not, that means
            # the new song's suggestions haven't been created yet, so we can
            # use the current one.
            self.suggestion.pop(-1)
        return self.make_suggestion(music)
    
    def suggest_song(self, music, artist, album, title):
        if not self.suggestion or artist != self.suggestion[-1]["artist"] or \
                album != self.suggestion[-1]["album"] or \
                title != self.suggestion[-1]["title"] or \
                self.suggestion[-1]["suggestions"].empty:
            currsugg = {"artist": artist, "album": album, "title": title}
            currsugg["suggestions"] = e.find_similar(self.songlist, artist,
                                                     title, album)
            self.suggestion.append(currsugg)
            if len(self.suggestion) > 10:
                self.suggestion.pop(0)
        return self.make_suggestion(music)
    
    def search_artist(self, music, search_string):
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
                song = self.search_song(music, partial.at[index2, "Artist"],
                                        partial.at[index2, "Album"],
                                        partial.at[index2, "Title"])
                result = result.append(song[0:1])
        new_list = self.mergesongdata(result, full)
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
        
    def remove_pl_current(self, c_artist, c_album, c_title):
        if self.playlist.empty:
            return
        last_index = self.playlist.index[-1]
        if ((self.playlist.at[last_index, "Artist"].lower() == c_artist.lower()) and \
                (self.playlist.at[last_index, "Album"].lower() == c_album.lower()) and \
                (self.playlist.at[last_index, "Title"].lower() == c_title.lower())):
            self.playlist = self.playlist.drop(last_index)
    
    def db_maintain(self, music):
        status = music.status()
        if status["state"] != "play":
            return
        currentsong = music.currentsong()
        duration = float(status["duration"])
        elapsed = float(status["elapsed"])
        c_artist = currentsong["artist"].replace(",", "")
        if "album" in currentsong:
            c_album = currentsong["album"].replace(",", "")
        else:
            c_album = ""
        c_title = currentsong["title"].replace(",", "")
        # Registering the currently playing song on the songs list
        if elapsed > 180 or elapsed > duration / 2:
            self.songlist_append(c_artist, c_album, c_title)
        self.playlist_append(c_artist, c_album, c_title)
            
    def load_file(self, fname=""):
        if fname == "":
            fname = "data"
        self.songlist, self.artists, self.albums, self.playlist = \
            e.load_data(fname)
    
    def save_file(self, fname=""):
        if fname == "":
            fname= "data"
        e.save_data(self.songlist, self.artists, self.albums, self.playlist,
                    fname)
