#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 23:33:44 2021

@author: nicemicro
"""

import tkinter as tk
from tkinter import ttk
import pandas as pd

class Player(ttk.Frame):
    def __init__(self, parent, controller, playlist):
        ttk.Frame.__init__(self, parent)
        self.controller = controller
        
        # TOP CONTROL
        self.playlistbox = ttk.Treeview(self, \
            columns=("Album", "Status"), selectmode='browse', height=playlist)
        self.playlistbox.heading('#0', text='Song')
        self.playlistbox.heading('#1', text='Album')
        self.playlistbox.heading('#2', text='Status')
        self.playlistbox.column('#0', stretch=tk.YES)
        self.playlistbox.column('#1', width=100)
        self.playlistbox.column('#2', width=20)
        self.playlistbox.grid(row=0, column=0, columnspan=5, sticky="nsew")
        self.playlistbox.bind('<Return>', lambda x: self.new_search())
        self.playlistbox.bind('<KP_Enter>', lambda x: self.new_search())
        for number in range(1, 10):
            self.playlistbox.bind(
                str(number),
                lambda x, number=number: self.new_search(number * 100)
            )
            self.playlistbox.bind(
                "<KP_" + str(number) + ">",
                lambda x, number=number: self.new_search(number * 100)
            )
        self.song_percentage = tk.DoubleVar(self, 0)
        self.percentage_scale = ttk.Scale(
            self,
            orient="horizontal",
            from_=0, to=100,
            variable=self.song_percentage,
            state=tk.DISABLED,
            command=self.scrub_position
        )
        self.percentage_scale.grid(row=2, column=0, columnspan=5, sticky="ew")
        ttk.Button(self, text="Play / Pause", command=self.playpause) \
            .grid(row=3, column=0, columnspan=1)
        ttk.Button(self, text="Change song", command=self.new_search) \
            .grid(row=3, column=1, columnspan=1)
        ttk.Button(self, text="Next song", command=self.step_next) \
            .grid(row=3, column=2, columnspan=1)
        ttk.Button(self, text="+",
                   command= lambda: self.controller.volume(+5)) \
            .grid(row=3, column=3, columnspan=1)
        ttk.Button(self, text="-",
                   command= lambda: self.controller.volume(-5)) \
            .grid(row=3, column=4, columnspan=1)
        
        self.columnconfigure(0, weight = 1)
        self.columnconfigure(1, weight = 1)
        self.columnconfigure(2, weight = 1)
        self.columnconfigure(3, weight = 1)
        self.columnconfigure(4, weight = 1)
        
    def playpause(self):
        self.controller.play_pause()
    
    def selection(self):
        selected = self.playlistbox.focus()
        if len(selected) < 1:
            return(-2)
        if str(selected) == "-1":
            return(-1)
        if not str(selected).isdigit():
            assert False, \
                "somehow we got a non-digit ID for an element in playlist"
        else:
            return int(selected)

    def new_search(self, group: int = -1):
        self.controller.change_song(self.selection(), group)

    def step_next(self):
        self.controller.play_next(self.selection())

    def scrub_position(self, scrub_to):
        self.controller.scrub_to_percent(float(scrub_to))

    def set_now_play(self, playlist):
        positions = [song["pos"] for song in playlist]
        elements = self.playlistbox.get_children()
        el_to_del = [pos for pos in elements if not (pos in positions)]
        for element in el_to_del:
            self.playlistbox.delete(element)
        for songinfo in playlist:
            if songinfo["pos"] in elements:
                self.playlistbox.item(
                    songinfo["pos"],
                    text=songinfo["display"],
                    values=(songinfo["album"],
                    songinfo["status"])
                )
            else:
                self.playlistbox.insert(
                    "", "end", iid=songinfo["pos"],
                    text=songinfo["display"],
                    values=(songinfo["album"],
                    songinfo["status"])
                )
        if playlist[0]["mpdstate"] == "stop":
            self.song_percentage.set(0)
        else:
            elapsed = int(playlist[0]["mpdtime"].split(":")[0])
            total = int(playlist[0]["mpdtime"].split(":")[1])
            self.song_percentage.set(elapsed / (total + 0.01) * 100)
        if playlist[0]["mpdstate"] == "stop" or playlist[0]["mpdstate"] == "pause":
            self.percentage_scale.configure(state=tk.DISABLED)
        if playlist[0]["mpdstate"] == "play":
            self.percentage_scale.configure(state=tk.NORMAL)

class Not_played(ttk.Frame):
    def __init__(self, parent, controller):
        ttk.Frame.__init__(self, parent)
        self.controller = controller
        
        # MIDSECTION
        self.songlistframe = ttk.Frame(self)
        self.songlistframe.grid(row=0, column=0, columnspan=5, sticky="nsew")
        self.songlistbox = ttk.Treeview(
            self.songlistframe,
            columns=("Album", "Plays", "Last", "First"),
            selectmode='browse', height=25
        )
        self.songlistbox.heading('#0', text='Song')
        self.songlistbox.heading('#1', text='Album')
        self.songlistbox.heading('#2', text='Plays')
        self.songlistbox.heading('#3', text='Last played')
        self.songlistbox.heading('#4', text='First played')
        self.songlistbox.column('#0', width=340, stretch=tk.YES)
        self.songlistbox.column('#1', width=240)
        self.songlistbox.column('#2', width=40)
        self.songlistbox.column('#3', width=160)
        self.songlistbox.column('#4', width=160)
        self.songlistbox.grid(row=0, column=0, sticky="nsew")
        self.songbar = ttk.Scrollbar(
            self.songlistframe,
            orient=tk.VERTICAL,
            command=self.songlistbox.yview
        )
        self.songbar.grid(row=0, column=1, sticky="nsew")
        self.loading_bar = ttk.Progressbar(
            self.songlistframe, mode="indeterminate"
        )
        self.loading_bar.start(10)
        self.loading_bar.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self.songlistbox.configure(yscrollcommand=self.songbar.set)
        self.songlistframe.columnconfigure(0, weight=1)
        self.songlistframe.columnconfigure(1, weight=0)
        self.songlistframe.rowconfigure(0, weight=1)
        
        ttk.Button(self, text="Add selected",
                   command= lambda: self.add_song()) \
            .grid(row=1, column=0, columnspan=1)

        minplay_frame = ttk.Frame(self)
        minplay_frame.columnconfigure(0, weight=1)
        minplay_frame.grid(row=1, column=1, columnspan=2, sticky="nswe")
        self.minplay_value = tk.IntVar(value=1)
        self.minplay_scale = ttk.Scale(
            minplay_frame,
            orient="horizontal",
            length=30,
            from_=1,
            to=30,
            value=1,
            command=lambda x: self.change_minmax(self.minplay_value, float(x))
        )
        self.minplay_scale.grid(row=0, column=0, sticky="nswe")
        self.minplay_text = ttk.Entry(
            minplay_frame,
            textvariable=self.minplay_value,
            width=4,
        )
        self.minplay_text.grid(row=0, column=1, sticky="ns")

        maxplay_frame = ttk.Frame(self)
        maxplay_frame.columnconfigure(0, weight=1)
        maxplay_frame.grid(row=1, column=3, columnspan=2, sticky="nswe")
        self.maxplay_value = tk.IntVar(value=15)
        self.maxplay_scale = ttk.Scale(
            maxplay_frame,
            orient="horizontal",
            length=30,
            from_=1,
            to=30,
            value=15,
            command=lambda x: self.change_minmax(self.maxplay_value, float(x))
        )
        self.maxplay_scale.grid(row=0, column=0, sticky="nswe")
        self.maxplay_text = ttk.Entry(
            maxplay_frame,
            textvariable=self.maxplay_value,
            width=4,
        )
        self.maxplay_text.grid(row=0, column=1, sticky="ns")
            
        self.back_button = ttk.Button(
            self,
            text="<- Page",
            command=lambda: self.prev_page()
        )
        self.back_button.state(["disabled"])
        self.back_button.grid(row=2, column=0, columnspan=1)
        self.button_types: dict[str, str] = {
            "Recently added": "new add",
            "Not played recently": "old p",
            "Rarely played": "rarely"
        }
        self.generate_buttons: dict[str, ttk.Button] = {}
        counter=1
        for name in self.button_types:
            self.generate_buttons[name] = ttk.Button(
                self,
                text=name,
                command=lambda x=name: self.switch_page(x)
            )
            self.generate_buttons[name].grid(row=2, column=counter, columnspan=1)
            self.generate_buttons[name].state(["disabled"])
            counter += 1
        self.fwd_button = ttk.Button(
            self,
            text="Page ->",
            command= lambda: self.next_page()
        )
        self.fwd_button.state(["disabled"])
        self.fwd_button.grid(row=2, column=4, columnspan=1)
            
        self.columnconfigure(0, weight = 1)
        self.columnconfigure(1, weight = 1)
        self.columnconfigure(2, weight = 1)
        self.columnconfigure(3, weight = 1)
        self.columnconfigure(4, weight = 1)
        self.rowconfigure(0, weight = 1)
        self.rowconfigure(1, weight = 0)
        self.rowconfigure(2, weight = 0)
    
    def start_loading(self):
        self.loading_bar.start(10)
        self.loading_bar.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self.back_button.state(["disabled"])
        for button in self.generate_buttons.values():
            button.state(["disabled"])
        self.fwd_button.state(["disabled"])
    

    def prev_page(self):
        self.start_loading()
        self.controller.prev_page()

    def next_page(self):
        self.start_loading()
        self.controller.next_page()

    def switch_page(self, button_name: str):
        self.start_loading()
        self.controller.switch_page(
            self.button_types[button_name],
            self.minplay_value.get(),
            self.maxplay_value.get()
        )

    def change_minmax(self, textvariable: tk.IntVar, value: float) -> None:
        if textvariable == self.minplay_value:
            value = min(value, self.maxplay_value.get())
        elif textvariable == self.maxplay_value:
            value = max(value, self.minplay_value.get())
        else:
            assert False
        textvariable.set(round(value))
    
    def setlist(self, newlist: pd.DataFrame):
        elements = self.songlistbox.get_children()
        if elements:
            for element in elements:
                self.songlistbox.delete(element)
        newlist["display"] = newlist["artist"] + " - " + newlist["title"]
        for colname in ["album", "Played last", "Added first"]:
            mask = (newlist[colname].isnull())
            newlist.loc[mask, colname] = "-"
        mask = (newlist["Played"].isnull())
        newlist.loc[mask, "Played"] = 0
        toadd = newlist.to_dict(orient="records")
        index = 0
        for songinfo in toadd:
            self.songlistbox.insert(
                "",
                "end",
                iid=str(index),
                text=songinfo["display"],
                values=(
                    songinfo["album"],
                    int(songinfo["Played"]),
                    str(songinfo["Played last"]).split(".")[0],
                    str(songinfo["Added first"]).split(".")[0]
                )
            )
            index += 1
        self.loading_bar.stop()
        self.loading_bar.grid_forget()
        self.back_button.state(["!disabled"])
        for button in self.generate_buttons.values():
            button.state(["!disabled"])
        self.fwd_button.state(["!disabled"])
    
    def add_song(self):
        selected = self.songlistbox.focus()
        if len(selected) < 1:
            return
        if not str(selected).isdigit():
            assert False, \
                "somehow we got a non-digit ID for an element in playlist"
        position = self.controller.playerframe.selection()
        self.controller.add_song_from_list(position, int(selected))

class Search(ttk.Frame):
    def __init__(self, parent, controller):
        ttk.Frame.__init__(self, parent)
        self.controller = controller
        self._show_prev_search = tk.IntVar()
        
        self.header = ttk.Frame(self)
        self.header.grid(row=0, column=0, columnspan=4, sticky="nsew")
        ttk.Label(
            self.header,
            text="Search: "
        ).grid(row=0, column=0, sticky="nse")
        self.searchstring = tk.StringVar()
        self.searchbox = ttk.Entry(self.header, textvariable=self.searchstring)
        self.searchbox.bind('<Return>', self.search_enter)
        self.searchbox.grid(row=0, column=1, sticky="nsew")
        ttk.Button(self.header, text="Search", command=self.searchnow).\
            grid(row=0, column=2, sticky="nsew")
        ttk.Label(
            self.header,
            text="Show already played:"
        ).grid(row=1, column=0, sticky="nse")
        ttk.Checkbutton(
            self.header,
            variable=self._show_prev_search,
            offvalue=0,
            onvalue=1
        ).grid(row=1, column=1, sticky="nsew")
        self.header.columnconfigure(0, weight = 0)
        self.header.columnconfigure(1, weight = 10)
        self.header.columnconfigure(2, weight = 1)
        
        self.songlistbox = ttk.Treeview(
            self,
            columns=("Plays", "Info", "Last", "First"),
            selectmode='browse',
            height=10
        )
        self.songlistbox.heading('#0', text='Song')        
        self.songlistbox.heading('#1', text='Info')
        self.songlistbox.heading('#2', text='Plays')
        self.songlistbox.heading('#3', text='Last played')
        self.songlistbox.heading('#4', text='First played')
        self.songlistbox.column('#0', width=480, stretch=tk.YES)
        self.songlistbox.column('#1', width=100)
        self.songlistbox.column('#2', width=40)
        self.songlistbox.column('#3', width=160)
        self.songlistbox.column('#4', width=160)
        self.songlistbox.grid(row=1, column=0, columnspan=3, sticky="nsew")
        self.songbar = ttk.Scrollbar(self, orient=tk.VERTICAL,
                                     command=self.songlistbox.yview)
        self.songbar.grid(row=1, column=3, sticky="nsew")
        self.songlistbox.configure(yscrollcommand=self.songbar.set)
        
        ttk.Button(
            self,
            text="Add selected",
            command= lambda: self.add_song()
        ).grid(row=2, column=0, columnspan=1)
        
        self.columnconfigure(0, weight = 1)
        self.columnconfigure(1, weight = 1)
        self.columnconfigure(2, weight = 3)
        self.columnconfigure(3, weight = 0)
        self.rowconfigure(0, weight = 0)
        self.rowconfigure(1, weight = 1)
        self.rowconfigure(2, weight = 0)
        
    def search_enter(self, event):
        self.searchnow()
        
    def searchnow(self):
        search_string = self.searchstring.get()
        if len(search_string) < 2:
            return
        self.controller.search_string(search_string, self._show_prev_search.get()!=1)

    def setlist(self, newlist: pd.DataFrame) -> None:
        elements = self.songlistbox.get_children()
        if elements:
            for element in elements:
                self.songlistbox.delete(element)
        if newlist.empty: return
        for colname in ["track", "album", "date", "genre"]:
            if colname not in newlist.columns:
                newlist[colname] = ""
        newlist["trackstr"] = ""
        newlist.loc[
            ((newlist["track"].notna()) & (newlist["track"] != "")),
            "trackstr"
        ] = (
            newlist.loc[
                ((newlist["track"].notna()) & (newlist["track"] != "")),
                "track"
            ]
            .astype("string") + ". "
        )
        for colname in ["album", "Played last", "Added first"]:
            mask = ((newlist[colname].isnull()) | (newlist[colname] == ""))
            newlist.loc[mask, colname] = "-"
        for colname in ["genre", "date", "track"]:
            mask = ((newlist[colname].isnull()) | (newlist[colname] == "-1"))
            newlist.loc[mask, colname] = ""
        mask = (newlist["Played"].isnull())
        newlist.loc[mask, "Played"] = 0
        artists = (
            newlist.groupby(["artist"])
            .agg({"title": "count"})
            .reset_index(drop=False)[["artist"]]
        )
        albums = (
            newlist.groupby(["artist", "album", "date"])
            .agg({"title": "count"})
            .reset_index(drop=False)[["artist", "album", "date"]]
        )
        for art_ind in artists.index:
            self.songlistbox.insert(
                "",
                "end",
                iid=artists.at[art_ind, "artist"],
                text=artists.at[art_ind, "artist"]
            )
        for alb_ind in albums.index:
            self.songlistbox.insert(
                albums.at[alb_ind, "artist"],
                "end",
                iid=(
                    albums.at[alb_ind, "artist"] +
                    albums.at[alb_ind, "album"] +
                    albums.at[alb_ind, "date"]
                ),
                text=albums.at[alb_ind, "album"],
                values=(albums.at[alb_ind, "date"], "", "", "")
            )
        for s_ind in newlist.index:
            self.songlistbox.insert(
                (
                    newlist.at[s_ind, "artist"] +
                    newlist.at[s_ind, "album"] +
                    newlist.at[s_ind, "date"]
                ),
                "end",
                iid=s_ind,
                text=(
                    newlist.at[s_ind, "trackstr"] + newlist.at[s_ind, "title"]
                ),
                values=(
                    newlist.at[s_ind, "genre"],
                    int(newlist.at[s_ind, "Played"]),
                    str(newlist.at[s_ind, "Played last"]).split(".")[0],
                    str(newlist.at[s_ind, "Added first"]).split(".")[0]
                )
            )
    
    def add_song(self):
        selected = self.songlistbox.focus()
        if len(selected) < 1:
            return
        if not str(selected).isdigit():
            assert False, \
                "somehow we got a non-digit ID for an element in playlist"
        position = self.controller.playerframe.selection()
        self.controller.add_found_song(position, int(selected))
