#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 09:26:24 2021

@author: nicemicro
"""

import autoplay_gui_elements as apgui
import autoplay_musichandler as apmh
import os
#from mpd import MPDmusic
from mpd_wrapper import MPD
import tkinter as tk
from tkinter import ttk


#%%

class AppContainer(tk.Tk):
    def __init__(self, music, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)
        self.music_handler = apmh.MusicHandler(music)
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
        
        # These variables are controling the listing of possible songs to play
        self.nplistsize = 25 # the number of songs listed in the not played box
        self.suggestionlist = False
        self.selectable = None        
        self.switch_page("new add", 3, 15)
        self.searchresult = None

        self.after(500, self.update_current_played)
        self.after(5000, self.db_maintain)
        self.after(30000, self.save_db)
    
    def play_pause(self):
        self.music_handler.play_pause()
        
    def play_next(self):
        self.music_handler.play_next()
    
    def change_current_song(self):
        self.music_handler.change_current_song()
        self.after(50, self.change_song_now)
    
    def change_song_now(self):
        found = self.music_handler.change_song_now()
        if not found:
            self.after(200, self.change_song_now)
            return
    
    def play_file(self, playnow, filename):
        self.music_handler.play_file(playnow, filename)
        self.update_next_played()
    
    def add_song_from_list(self, playnow, itemnum):
        self.play_file(playnow, self.selectable.at[itemnum, "file"])

    def volume(self, change):
        self.music_handler.volume(change)
    
    def list_choices(self):        
        self.selectable = self.selectable.reset_index(drop=True)
        self.frames["Not_played"].setlist(self.selectable.copy())
        
    def prev_page(self):
        self.music_handler.songlist_page_switch(self.nplistsize, False)
        self.after(50, self.page_change)
        
    def next_page(self):
        self.music_handler.songlist_page_switch(self.nplistsize, True)
        self.after(50, self.page_change)
        
    def page_change(self):
        new_selectable = self.music_handler.ret_songl_pg_sw()
        if new_selectable.empty:
            self.after(200, self.page_change)
            return
        self.selectable = new_selectable
        self.list_choices()
    
    def switch_page(self, sort_by, min_play, max_play):
        self.music_handler.new_songlist(sort_by, min_play, max_play)
        self.next_page()
    
    def search_artist(self, artist_string):
        self.music_handler.search_artist(artist_string)
        self.after(50, self.search_artist_fill)
    
    def search_artist_fill(self):
        self.searchresult = self.music_handler.ret_search_artists()
        if self.searchresult.empty:
            self.after(200, self.search_artist_fill)
            return
        self.frames["Search"].setlist(self.searchresult)
    
    def add_found_song(self, playnow, itemnum):
        self.play_file(playnow, self.searchresult.at[itemnum, "file"])
        
    def update_next_played(self):
        next_song = self.music_handler.song_on_playlist(1)
        if next_song:
            nextplay_text = "Next up: " + next_song["artist"] + " - " + \
                next_song["title"]
            self.playerframe.set_next_play(text=nextplay_text)
        else:
            self.playerframe.set_next_play(text="Next up: ")

    def update_current_played(self):
        self.update_next_played()
        current = self.music_handler.song_played()
        if current["state"] == "stop":
            self.playerframe.set_now_play(text="Stopped")
            self.after(500, self.update_current_played)
            return
        current_text = current["artist"] + " - " + current["title"]
        if current["state"] == "pause":
            current_text = current_text + " (Paused)"
        self.playerframe.set_now_play(text=current_text)
        self.after(500, self.update_current_played)
    
    def db_maintain(self):
        self.music_handler.db_maintain()
        self.after(5000, self.db_maintain)
    
    def save_db(self):
        self.music_handler.save_db()
        self.after(30000, self.save_db)
    
    def destroy(self):
        self.music_handler.destroy()
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
    music = MPD()                    # create music object
    music.timeout = 100              # network timeout in seconds
    music.idletimeout = None         # for fetching the result of idle command
    mpd_shutoff = mpd_on(music)
    app = AppContainer(music)
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