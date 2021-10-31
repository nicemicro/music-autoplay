#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 09:26:24 2021

@author: nicemicro
"""

import autoplay_gui_elements as apgui
import autoplay_databases as apdb
import os
#from mpd import MPDmusic
from mpd_wrapper import MPD
import tkinter as tk
from tkinter import ttk

#%%

class AppContainer(tk.Tk):
    def __init__(self, music, db, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)
        self.music = music
        self.db = db
        self.title("Music Autoplay")
        
        container = ttk.Frame(self)
        container.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        
        self.playerframe = apgui.Player(parent=container, controller=self)
        self.playerframe.grid(row=0, column=0, sticky="nsew")
        self.bottomsection = ttk.Notebook(container)
        self.bottomsection.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        self.frames = {}
        for F in (apgui.Not_played, apgui.Search):
            page_name = F.__name__
            frame = F(parent=self.bottomsection, controller=self)
            self.frames[page_name] = frame
            self.bottomsection.add(frame, text=page_name.replace("_", " "))
            # put all of the pages in the same location;
            # the one on the top of the stacking order
            # will be the one that is visible.
            
        
        container.rowconfigure(0, weight=0)
        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)
        
        self.clear_playlist()
        
        # These variables are controling the listing of possible songs to play
        self.nplistsize = 25 # the number of songs listed in the not played box
        self.suggestionlist = False
        self.selectable = None        
        self.switch_page("new add", 3, 15)
        self.searchresult = None

        self.after(500, self.update_current_played)
        self.after(5000, self.db_maintain)
        self.after(30000, self.save_db)
        
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
    
    def current_song_data(self, currentsong):
        c_artist = currentsong["artist"].replace(",", "")
        if "album" in currentsong:
            c_album = currentsong["album"].replace(",", "")
        else:
            c_album = ""
        c_title = currentsong["title"].replace(",", "")
        return c_artist, c_album, c_title
            
    def add_suggested_song(self):
        currentsong = self.music.currentsong()
        if len(currentsong) == 0:
            return
        c_artist, c_album, c_title = self.current_song_data(currentsong)
        suggestion = self.db.suggest_song(self.music, c_artist, c_album,
                                          c_title)
        if suggestion.empty: return
        self.music.add(suggestion.at[0, "file"])
        nextplay_text = "Next up: " + suggestion.at[0, "artist"] + " - " + \
            suggestion.at[0, "title"]
        self.playerframe.set_next_play(text=nextplay_text)
        self.music.random(0)
    
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
        else:
            playlistlast = self.music.playlistinfo()[-1]
            nextplay_text = "Next up: " + playlistlast["artist"] + " - " + \
                playlistlast["title"]
            self.playerframe.set_next_play(text=nextplay_text)
        
    def add_song_from_list(self, playnow, itemnum):
        self.play_file(playnow, self.selectable.at[itemnum, "file"])

    def volume(self, change):
        volumelevel = int(self.music.status()["volume"])
        self.music.volume(volumelevel + change)
    
    def list_choices(self):        
        self.selectable = self.selectable.reset_index(drop=True)
        self.frames["Not_played"].setlist(self.selectable.copy())
        
    def prev_page(self):
        new_selectable = self.db.list_songs_bck(self.music, self.nplistsize)
        if new_selectable.empty: return
        self.selectable = new_selectable
        self.list_choices()
        
    def next_page(self):
        new_selectable = self.db.list_songs_fwd(self.music, self.nplistsize)
        if new_selectable.empty: return
        self.selectable = new_selectable
        self.list_choices()
    
    def switch_page(self, sort_by, min_play, max_play):
        self.db.new_songlist(sort_by=sort_by, min_play=min_play,
                             max_play=max_play)
        self.next_page()
    
    def search_artist(self, artist_string):
        found_songs = self.db.search_artist(self.music, artist_string)
        self.searchresult = found_songs.reset_index(drop=True)
        self.frames["Search"].setlist(self.searchresult)
    
    def add_found_song(self, playnow, itemnum):
        self.play_file(playnow, self.searchresult.at[itemnum, "file"])

    def update_current_played(self):
        current = self.music.currentsong()
        status = self.music.status()
        if status["state"] == "stop":
            self.playerframe.set_now_play(text="Stopped")
            self.after(500, self.update_current_played)
            return
        mpdlistlen = int(status["playlistlength"])
        mpdlistpos = int(status["song"])
        if mpdlistpos == mpdlistlen - 1:
            # need to add a song to the list automatically
            self.add_suggested_song()
        current_text = current["artist"] + " - " + current["title"]
        if status["state"] == "pause":
            current_text = current_text + " (Paused)"
        self.playerframe.set_now_play(text=current_text)
        self.after(500, self.update_current_played)
    
    def db_maintain(self):
        self.db.db_maintain(self.music)
        self.after(5000, self.db_maintain)
    
    def save_db(self):
        self.db.save_file()
        self.after(30000, self.save_db)
    
    def destroy(self):
        status = self.music.status()
        self.remove_not_played(status)
        self.db.save_file()
        tk.Tk.destroy(self)

#%%

def mpd_on(music):
    try:
        music.connect("localhost", 6600) # connect to localhost:6600
    except:
        print("Now starting MPD and YAMS")
        response = os.system("mpd")
        response += os.system("yams")
        if response > 0:
            return 255
        music.connect("localhost", 6600) # connect to localhost:6600
        return 1
    return 0

def mpd_stop():
    print("Shutting down MPD and YAMS")
    os.system("mpd --kill")
    os.system("yams --kill")

def main_loop(music, db):
    print('Select a command')
    cmd = input("search terms: ")
    if cmd != "":
        print(music.find("any", cmd))
    return cmd != ""

def main():
    db = apdb.DataBases()
    db.load_file("data")
    print("Files loaded")
    music = MPD()                    # create music object
    music.timeout = 100              # network timeout in seconds
    music.idletimeout = None         # for fetching the result of idle command
    mpd_shutoff = mpd_on(music)
    app = AppContainer(music, db)
    if mpd_shutoff <= 1:
        print("Mpd version:", music.mpd_version)
        app.mainloop()
        music.close()
        music.disconnect()
    if mpd_shutoff == 1:    
        mpd_stop()
    print('')
    print('Bye')

if __name__ == '__main__':
    main()