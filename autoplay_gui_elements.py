#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 23:33:44 2021

@author: nicemicro
"""

import tkinter as tk
from tkinter import ttk

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
        #self.nowplaytext = tk.StringVar()
        #self.nowplaylabel = ttk.Label(self, textvariable=self.nowplaytext, 
        #                              justify=tk.CENTER)
        #self.nowplaylabel.grid(row=0, column=0, columnspan=5)
        #self.nextplaytext = tk.StringVar()
        #self.nextplaytext.set("Up next: ")
        #self.nextplaylabel = ttk.Label(self, textvariable=self.nextplaytext, 
        #                              justify=tk.CENTER)
        #self.nextplaylabel.grid(row=1, column=0, columnspan=5)
        ttk.Button(self, text="Play / Pause", command=self.playpause) \
            .grid(row=2, column=0, columnspan=1)
        ttk.Button(self, text="Change song", command=self.new_search) \
            .grid(row=2, column=1, columnspan=1)
        ttk.Button(self, text="Next song", command=self.step_next) \
            .grid(row=2, column=2, columnspan=1)
        ttk.Button(self, text="+",
                   command= lambda: self.controller.volume(+5)) \
            .grid(row=2, column=3, columnspan=1)
        ttk.Button(self, text="-",
                   command= lambda: self.controller.volume(-5)) \
            .grid(row=2, column=4, columnspan=1)
        
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
            self.controller.play_next(0)
        elif str(selected) == "-1":
            self.controller.play_next(-1)
        elif not str(selected).isdigit():
            assert False, \
                "somehow we got a non-digit ID for an element in playlist"
        else:
            return int(selected)

    def new_search(self):
        self.controller.change_current_song(self.selection())

    def step_next(self):
        self.controller.play_next(self.selection())

    def set_now_play(self, playlist):
        #print("text: ", text)
        positions = [song["pos"] for song in playlist]
        elements = self.playlistbox.get_children()
        el_to_del = [pos for pos in elements if not (pos in positions)]
        for element in el_to_del:
            #print("Delete element ", element)
            self.playlistbox.delete(element)
        #new_el_pos = [pos for pos in positions if not (pos in elements)]
        #new_el = [song for song in playlist if song["pos"] in new_el_pos]
        for songinfo in playlist:
            if songinfo["pos"] in elements:
                self.playlistbox.item(songinfo["pos"],
                                      text=songinfo["display"],
                                      values=(songinfo["album"],
                                              songinfo["status"]))
            else:
                self.playlistbox.insert("", "end", iid=songinfo["pos"], \
                    text=songinfo["display"], values=(songinfo["album"], \
                    songinfo["status"]))

class Not_played(ttk.Frame):
    def __init__(self, parent, controller):
        ttk.Frame.__init__(self, parent)
        self.controller = controller
        
        # MIDSECTION
        self.songlistframe = ttk.Frame(self)
        self.songlistframe.grid(row=0, column=0, columnspan=5, sticky="nsew")
        self.songlistbox = ttk.Treeview(self.songlistframe, \
            columns=("Album", "Plays", "Last", "First"), \
            selectmode='browse', height=25)
        self.songlistbox.heading('#0', text='Song')
        self.songlistbox.heading('#1', text='Album')
        self.songlistbox.heading('#2', text='Plays')
        self.songlistbox.heading('#3', text='Last played')
        self.songlistbox.heading('#4', text='First played')
        self.songlistbox.column('#0', stretch=tk.YES)
        self.songlistbox.column('#1', width=150)
        self.songlistbox.column('#2', width=20)
        self.songlistbox.column('#3', width=100)
        self.songlistbox.column('#4', width=100)
        self.songlistbox.grid(row=0, column=0, sticky="nsew")
        self.songbar = ttk.Scrollbar(self.songlistframe, orient=tk.VERTICAL,
                                     command=self.songlistbox.yview)
        self.songbar.grid(row=0, column=1, sticky="nsew")
        self.songlistbox.configure(yscrollcommand=self.songbar.set)
        self.songlistframe.columnconfigure(0, weight=1)
        self.songlistframe.columnconfigure(1, weight=0)
        self.songlistframe.rowconfigure(0, weight=1)
        
        ttk.Button(self, text="Play selected", 
                   command= lambda: self.add_song(True)) \
            .grid(row=1, column=0, columnspan=1)
        ttk.Button(self, text="Queue selected",
                   command= lambda: self.add_song(False)) \
            .grid(row=1, column=1, columnspan=1)
            
        ttk.Button(self, text="<- Page", 
                   command= lambda: self.controller.prev_page()) \
            .grid(row=2, column=0, columnspan=1)
        ttk.Button(self, text="Recently added",
                   command= lambda: self.controller.switch_page("new add", 3,
                                                                15)) \
            .grid(row=2, column=1, columnspan=1)
        ttk.Button(self, text="Not played recently",
                   command= lambda: self.controller.switch_page("old p", 10,
                                                                0)) \
            .grid(row=2, column=2, columnspan=1)
        ttk.Button(self, text="Rarely played",
                   command= lambda: self.controller.switch_page("rarely", 3,
                                                                0)) \
            .grid(row=2, column=3, columnspan=1)
        ttk.Button(self, text="Page ->", 
                   command= lambda: self.controller.next_page()) \
            .grid(row=2, column=4, columnspan=1)
            
        self.columnconfigure(0, weight = 1)
        self.columnconfigure(1, weight = 1)
        self.columnconfigure(2, weight = 1)
        self.columnconfigure(3, weight = 1)
        self.columnconfigure(4, weight = 1)
        self.rowconfigure(0, weight = 1)
        self.rowconfigure(1, weight = 0)
        self.rowconfigure(2, weight = 0)
    
    def setlist(self, newlist):
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
            self.songlistbox.insert("", "end", iid=index, \
                text=songinfo["display"], values=(songinfo["album"], \
                int(songinfo["Played"]), songinfo["Played last"], \
                songinfo["Added first"]))
            index += 1
    
    def add_song(self, playnow):
        selected = self.songlistbox.focus()
        if len(selected) < 1:
            return
        if not str(selected).isdigit():
            assert False, \
                "somehow we got a non-digit ID for an element in playlist"
        self.controller.add_song_from_list(playnow, int(selected))

class Search(ttk.Frame):
    def __init__(self, parent, controller):
        ttk.Frame.__init__(self, parent)
        self.controller = controller
        
        self.header = ttk.Frame(self)
        self.header.grid(row=0, column=0, columnspan=4, sticky="nsew")
        ttk.Label(self.header, text="Artist search: ").grid(row=0, column=0,
                                                            sticky="nse")
        self.searchstring = tk.StringVar()
        self.searchbox = ttk.Entry(self.header, textvariable=self.searchstring)
        self.searchbox.bind('<Return>', self.search_enter)
        self.searchbox.grid(row=0, column=1, sticky="nsew")
        ttk.Button(self.header, text="Search", command=self.searchnow).\
            grid(row=0, column=2, sticky="nsew")
        self.header.columnconfigure(0, weight = 0)
        self.header.columnconfigure(1, weight = 10)
        self.header.columnconfigure(2, weight = 1)
        
        self.songlistbox = ttk.Treeview(self, columns=("Plays", \
            "Last", "First"), selectmode='browse', height=10)
        self.songlistbox.heading('#0', text='Song')        
        self.songlistbox.heading('#1', text='Plays')
        self.songlistbox.heading('#2', text='Last played')
        self.songlistbox.heading('#3', text='First played')
        self.songlistbox.column('#0', stretch=tk.YES)
        self.songlistbox.column('#1', width=20)
        self.songlistbox.column('#2', width=100)
        self.songlistbox.column('#3', width=100)
        self.songlistbox.grid(row=1, column=0, columnspan=3, sticky="nsew")
        self.songbar = ttk.Scrollbar(self, orient=tk.VERTICAL,
                                     command=self.songlistbox.yview)
        self.songbar.grid(row=1, column=3, sticky="nsew")
        self.songlistbox.configure(yscrollcommand=self.songbar.set)
        
        ttk.Button(self, text="Play selected", 
                   command= lambda: self.add_song(True)) \
            .grid(row=2, column=0, columnspan=1)
        ttk.Button(self, text="Queue selected",
                   command= lambda: self.add_song(False)) \
            .grid(row=2, column=1, columnspan=1)
        
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
        artist_string = self.searchstring.get()
        if len(artist_string) < 3: return
        self.controller.search_artist(artist_string)
    
    def setlist(self, newlist):
        elements = self.songlistbox.get_children()
        if elements:
            for element in elements:
                self.songlistbox.delete(element)
        if newlist.empty: return
        for colname in ["album", "Played last", "Added first"]:
            mask = (newlist[colname].isnull())
            newlist.loc[mask, colname] = "-"
        mask = (newlist["Played"].isnull())
        newlist.loc[mask, "Played"] = 0
        artists = newlist.groupby(["artist"]).agg({"title": "count"}).\
            reset_index(drop=False)[["artist"]]
        albums = newlist.groupby(["artist", "album"]).agg({"title": "count"}).\
            reset_index(drop=False)[["artist", "album"]]
        for art_ind in artists.index:
            self.songlistbox.insert("", "end",
                                    iid=artists.at[art_ind, "artist"],
                                    text=artists.at[art_ind, "artist"])
        for alb_ind in albums.index:
            self.songlistbox.insert(albums.at[alb_ind, "artist"], "end",
                                    iid=(albums.at[alb_ind, "artist"]+
                                         albums.at[alb_ind, "album"]),
                                    text=albums.at[alb_ind, "album"])
        toadd = newlist.to_dict(orient="records")
        s_ind = 0
        for songinfo in toadd:
            self.songlistbox.insert(\
                (newlist.at[s_ind, "artist"]+newlist.at[s_ind, "album"]),
                "end", iid=s_ind, text=songinfo["title"],
                values=(int(songinfo["Played"]), songinfo["Played last"],
                        songinfo["Added first"]))
            s_ind += 1
    
    def add_song(self, playnow):
        selected = self.songlistbox.focus()
        if len(selected) < 1:
            return
        if not str(selected).isdigit():
            assert False, \
                "somehow we got a non-digit ID for an element in playlist"
        self.controller.add_found_song(playnow, int(selected))