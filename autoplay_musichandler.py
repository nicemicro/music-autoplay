#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 08:18:36 2021

@author: nicemicro
"""

import threading
import time
from queue import Queue
import autoplay_databases as apdb
import pandas as pd
#%%
class DataBaseWrapper(threading.Thread):
    def __init__(self, filename, command, response):
        threading.Thread.__init__(self)
        self.comm = command
        self.resp = response
        self.db = apdb.DataBases("data")
        self.cmds = {
            "search_song": self.db.search_song,
            "search_artist": self.db.search_artist
            }
    
    def run(self):
        exitFlag=False
        while not exitFlag:
            time.sleep(0.2)
            if self.comm.empty():
                continue
            funct, arguments = self.comm.get()
            if funct in self.cmds:
                ret = self.cmds[funct](*arguments)
                if not ret is None:
                    self.resp.put(ret)

#%%
class MusicHandler():
    def __init__(self, music):
        self.music = music
        self.db = apdb.DataBases("data")
        self.clear_playlist()
            
    def clear_playlist(self):
        status = self.music.status()
        if status["state"] == "stop":
            self.music.clear()
            return
        mpdlistlen = int(status["playlistlength"])
        if "song" not in status:
            return
        mpdlistpos = int(status["song"])
        if mpdlistpos <= mpdlistlen - 2:
            self.music.delete((mpdlistpos + 1, mpdlistlen))
        if mpdlistpos > 0:
            self.music.delete((0, mpdlistpos))
            
    def play_pause(self):
        if self.music.status()["state"] != "play":
            self.music.play()
        else:
            self.music.pause()
    
    def add_suggested_song(self):
        self.songsuggestion = pd.DataFrame([])
        currentsong = self.music.currentsong()
        if len(currentsong) == 0:
            return
        c_artist, c_album, c_title = self.current_song_data(currentsong)
        self.db.playlist_append(c_artist, c_album, c_title)
        suggestion = self.db.suggest_song(self.music, c_artist, c_album,
                                          c_title)
        if suggestion.empty: return
        self.music.add(suggestion.at[0, "file"])
    
    def change_current_song(self):
        currentsong = self.music.currentsong()
        if len(currentsong) == 0:
            return
        c_artist, c_album, c_title = self.current_song_data(currentsong)
        self.db.remove_pl_current(c_artist, c_album, c_title)
        status = self.music.status()
        mpdlistlen = int(status["playlistlength"])
        if "song" in status:
            mpdlistpos = int(status["song"])
        else:
            mpdlistpos = -1
        if mpdlistpos <= mpdlistlen - 2:
            self.music.delete((mpdlistpos + 1, mpdlistlen))
        suggestion = self.db.renew_suggestion(self.music, c_artist, c_album,
                                              c_title)
        if suggestion.empty: return
        self.music.add(suggestion.at[0, "file"])
        self.music.random(0)
        if status["state"] != "play":
            self.music.play()
        self.music.next()
        self.music.delete(mpdlistpos)
    
    def play_file(self, playnow, filename):
        # Clearing everything else from MPD's playlist
        status = self.music.status()
        pllength = int(status["playlistlength"])
        if "song" in status:
            place = int(status["song"])
        else:
            place = -1
        if place < pllength  - 1 and place != -1:
            self.music.delete((place + 1, pllength ))
        # Add the next song
        self.music.add(filename)
        self.music.random(0)
        if playnow:            
            self.play_next()
        
    def play_next(self):
        status = self.music.status()
        mpdlistlen = int(status["playlistlength"])
        if "song" in status:
            mpdlistpos = int(status["song"])
        else:
            mpdlistpos = -1
        self.remove_not_played(status)
        if mpdlistpos == mpdlistlen - 1 or mpdlistpos == -1:
            self.add_suggested_song()
        if status["state"] != "play":
            self.music.play()
        self.music.next()
        self.music.play()
    
    def current_song_data(self, currentsong):
        c_artist = currentsong["artist"].replace(",", "")
        if "album" in currentsong:
            c_album = currentsong["album"].replace(",", "")
        else:
            c_album = ""
        c_title = currentsong["title"].replace(",", "")
        return c_artist, c_album, c_title
    
    def volume(self, change):
        volumelevel = int(self.music.status()["volume"])
        self.music.volume(volumelevel + change)
    
    def songlist_page_switch(self, listsize, forward=True):
        if forward:
            self.songlist_page = self.db.list_songs_fwd(self.music, listsize)
        else:
            self.songlist_page = self.db.list_songs_bck(self.music, listsize)
    
    def ret_songl_pg_sw(self):
        return self.songlist_page
    
    def new_songlist(self, sort_by, min_play, max_play):
        self.db.new_songlist(sort_by=sort_by, min_play=min_play,
                             max_play=max_play)
    
    def search_artist(self, artist_string):
        self.found_songs = self.db.search_artist(self.music, artist_string)
        self.found_songs = self.found_songs.reset_index(drop=True)
    
    def ret_search_artists(self):
        return self.found_songs
    
    def song_on_playlist(self, place):
        status = self.music.status()
        if not "song" in status:
            return {}
        current = int(status["song"])
        pllength = int(status["playlistlength"])
        if place + current >= pllength:
            return {}
        return self.music.playlistinfo()[place+current]
    
    def song_played(self):
        current = self.music.currentsong()
        status = self.music.status()
        current["state"] = status["state"]
        if current["state"] == "stop":
            return current
        mpdlistlen = int(status["playlistlength"])
        mpdlistpos = int(status["song"])
        if mpdlistpos == mpdlistlen - 1:
            # need to add a song to the list automatically
            self.add_suggested_song()
        return current
    
    def db_maintain(self):
        self.db.db_maintain(self.music)
    
    def save_db(self):
        self.db.save_file()
    
    def remove_not_played(self, status):
        if status["state"] != "stop":
            duration = float(status["duration"])
            elapsed = float(status["elapsed"])
            if elapsed <= 180 and elapsed <= duration / 2:
                # The song not played at least 3 min or half its range shoud
                # be removed from the independent database playlist
                currentsong = self.music.currentsong() 
                c_artist, c_album, c_title = \
                    self.current_song_data(currentsong)
                self.db.remove_pl_current(c_artist, c_album, c_title)
        
    def destroy(self):
        status = self.music.status()
        self.remove_not_played(status)
        self.db.save_file()
        