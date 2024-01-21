#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 09:26:24 2021

@author: nicemicro
"""

import argparse
import os
import tkinter as tk
from tkinter import ttk
from typing import Optional, Union

import autoplay_gui_elements as apgui
import autoplay_musichandler as apmh
#from mpd import MPDmusic
from mpd_wrapper import MPD
import autoplay_ir_receiver as apir

#%%

class AppContainer(tk.Tk):
    def __init__(self, music, cmd_args, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)
        self.music_handler = apmh.MusicHandler(music)
        self.title("Music Autoplay")
        
        container = ttk.Frame(self)
        container.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        
        self.playerframe = apgui.Player(container, self, apmh.FWDLIST+1)
        self.playerframe.grid(row=0, column=0, sticky="nsew")
        self.bottomsection = ttk.Notebook(container)
        self.bottomsection.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        self.frames: dict[str, Union[apgui.Not_played, apgui.Search]] = {
            "Not_played": apgui.Not_played(
                parent=self.bottomsection,
                controller=self
            ),
            "Search": apgui.Search(parent=self.bottomsection, controller=self)
        }
        for name, frame in self.frames.items():
            self.bottomsection.add(frame, text=name.replace("_", " "))
            
        container.rowconfigure(0, weight=0)
        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)

        self.irreceiver = apir.IrReceiverHandler(self)
        
        # These variables are controling the listing of possible songs to play
        self.nplistsize: int = 25 # the number of songs listed in the not played box
        self.suggestionlist: bool = False
        self.selectable: Optional[apmh.pd.DataFrame] = None
        self.switch_page("new add", 0, 15)
        self.searchresult: Optional[apmh.pd.DataFrame] = None

        if cmd_args.start_playing or cmd_args.alarmclock:
            self.play_next(0)
        if cmd_args.alarmclock:
            self.music_handler.set_volume(50)
            self.after(5000, self.alarmclock_volumeup)

        self.after(500, self.update_current_played)
        self.after(500, self.check_ir_queue)
        self.after(5000, self.db_maintain)
    
    def play_pause(self):
        self.music_handler.play_pause()
        
    def play_next(self, place: int):
        self.music_handler.play_next(place)
    
    def change_song(self, place: int, group: int = -1):
        print(f"changing song, group={group}")
        self.music_handler.change_song(place, group)
  
    def play_file(self, position: int, filedata: tuple[str, str, str, str]) -> None:
        self.music_handler.play_file(position, filedata)
    
    def add_song_from_list(self, position: int, itemnum: int) -> None:
        assert self.selectable is not None
        filedata = (str(self.selectable.at[itemnum, "file"]),
                    str(self.selectable.at[itemnum, "Artist"]),
                    str(self.selectable.at[itemnum, "Album"]),
                    str(self.selectable.at[itemnum, "Title"]))
        self.play_file(position, filedata)

    def volume(self, change):
        self.music_handler.change_volume(change)

    def alarmclock_volumeup(self):
        self.volume(1)
        if self.music_handler.get_volume() < 100:
            self.after(5000, self.alarmclock_volumeup)

    def scrub_to_percent(self, percent: float) -> None:
        self.music_handler.scrub_to_percent(percent)
    
    def list_choices(self):
        assert self.selectable is not None
        self.selectable = self.selectable.reset_index(drop=True)
        self.frames["Not_played"].setlist(self.selectable.copy())
        
    def prev_page(self):
        self.music_handler.songlist_page_switch(self.nplistsize, False)
        self.after(50, self.page_change)
        
    def next_page(self):
        self.music_handler.songlist_page_switch(self.nplistsize, True)
        self.after(50, self.page_change)
        
    def page_change(self) -> None:
        new_selectable = self.music_handler.ret_songl_pg_sw()
        if new_selectable is None:
            self.after(200, self.page_change)
            return
        self.selectable = new_selectable
        self.list_choices()
    
    def switch_page(self, sort_by, min_play, max_play):
        self.music_handler.new_songlist(sort_by, min_play, max_play)
        self.next_page()
    
    def search_string(self, key: str, hide_played: bool) -> None:
        self.music_handler.search_string(key, hide_played)
        self.after(50, self.search_string_fill)

    def search_string_fill(self) -> None:
        self.searchresult = self.music_handler.ret_search_strings()
        if self.searchresult is None:
            self.after(200, self.search_string_fill)
            return
        self.frames["Search"].setlist(self.searchresult)

    def search_artist(self, artist_string: str) -> None:
        self.music_handler.search_artist(artist_string)
        self.after(50, self.search_artist_fill)
    
    def search_artist_fill(self) -> None:
        self.searchresult = self.music_handler.ret_search_artists()
        if self.searchresult is None:
            self.after(200, self.search_artist_fill)
            return
        self.frames["Search"].setlist(self.searchresult)
    
    def add_found_song(self, playnow: bool, itemnum: int):
        assert self.searchresult is not None
        filedata = (str(self.searchresult.at[itemnum, "file"]),
                    str(self.searchresult.at[itemnum, "Artist"]),
                    str(self.searchresult.at[itemnum, "Album"]),
                    str(self.searchresult.at[itemnum, "Title"]))
        self.play_file(playnow, filedata)
        
    #def update_next_played(self):
    #    next_song = self.music_handler.song_on_playlist()
    #    if next_song:
    #        nextplay_text = "Next up: " + next_song["artist"] + " - " + \
    #            next_song["title"]
    #        self.playerframe.set_next_play(text=nextplay_text)
    #    else:
    #        self.playerframe.set_next_play(text="Next up: ")

    def update_current_played(self):
        current = self.music_handler.song_played()
        self.playerframe.set_now_play(current)
        self.after(500, self.update_current_played)
    
    def db_maintain(self):
        self.music_handler.db_maintain()
        self.after(5000, self.db_maintain)
    
    def check_ir_queue(self):
        self.irreceiver.check_queue()
        self.after(100, self.check_ir_queue)
    
    def destroy(self):
        self.music_handler.destroy()
        self.irreceiver.destroy()
        tk.Tk.destroy(self)

def arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-playing",
        help="finds a song to play and starts automatically",
        action="store_true"
    )
    parser.add_argument(
        "--alarmclock",
        help=("starts the app at a low volume and increases it gradually" +
            " (implies --start-playing)"),
        action="store_true"
    )
    return parser

def mpd_on(music):
    try:
        music.connect("localhost", 6600) # connect to localhost:6600
    except:
        print("Now starting MPD")
        response = os.system("mpd")
        if response > 0:
            return 255
        print("Trying to start YAMS")
        response = os.system("yams 2>/dev/null")
        if response > 0:
            print("YAMS failed, starting MPDScribble")
            response = os.system("mpdscribble 2>/dev/null")
        if response > 0:
            print("MPDScribble failed, starting MPDAS")
            response = os.system("mpdas -d 2>/dev/null")
        music.connect("localhost", 6600) # connect to localhost:6600
        return 1
    return 0

def mpd_stop():
    print("Shutting down MPD and YAMS")
    os.system("mpd --kill")
    os.system("yams --kill 2>/dev/null")

def main(args):
    music = MPD()                    # create music object
    music.timeout = 100              # network timeout in seconds
    music.idletimeout = None         # for fetching the result of idle command
    mpd_shutoff = mpd_on(music)
    app = AppContainer(music, args)
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
    parser = arguments()
    args = parser.parse_args()
    main(args)
